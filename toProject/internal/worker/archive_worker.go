package worker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"test.com/toProject/internal/lock"
)

var (
	archiveMoved = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "archive_rows_moved_total",
		Help: "rows moved from hot tables to archive tables",
	}, []string{"table"})

	archiveCandidates = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "archive_rows_candidate",
		Help: "rows eligible for archiving observed in the last run",
	}, []string{"table"})

	archiveFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "archive_failed_total",
		Help: "archive batch failures",
	}, []string{"table"})

	archiveRuns = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "archive_runs_total",
		Help: "archive job runs",
	}, []string{"result"})
)

type ArchiveTarget struct {
	Table        string
	ArchiveTable string
	TimeColumn   string
	Retention    time.Duration
	Batch        int
	UnixMilli    bool
}

func DefaultArchiveTargets() []ArchiveTarget {
	return []ArchiveTarget{
		{Table: "iteration_log", ArchiveTable: "iteration_log_archive", TimeColumn: "create_time", Retention: 180 * 24 * time.Hour, Batch: 2000, UnixMilli: true},
		{Table: "notification", ArchiveTable: "notification_archive", TimeColumn: "created_at", Retention: 90 * 24 * time.Hour, Batch: 2000},
		{Table: "dead_letter", ArchiveTable: "dead_letter_archive", TimeColumn: "created_at", Retention: 90 * 24 * time.Hour, Batch: 2000},
		{Table: "delay_task", ArchiveTable: "delay_task_archive", TimeColumn: "created_at", Retention: 30 * 24 * time.Hour, Batch: 2000},
		{Table: "idempotent_record", ArchiveTable: "idempotent_record_archive", TimeColumn: "expire_at", Retention: 7 * 24 * time.Hour, Batch: 2000},
		{Table: "audit_event", ArchiveTable: "audit_event_archive", TimeColumn: "created_at", Retention: 365 * 24 * time.Hour, Batch: 2000},
	}
}

type ArchiveConfig struct {
	Interval time.Duration
	Targets  []ArchiveTarget
	DryRun   bool
}

func (c *ArchiveConfig) withDefaults() {
	if c.Interval <= 0 {
		c.Interval = time.Hour
	}
	if len(c.Targets) == 0 {
		c.Targets = DefaultArchiveTargets()
	}
	for i := range c.Targets {
		if c.Targets[i].Batch <= 0 {
			c.Targets[i].Batch = 1000
		}
		if c.Targets[i].Retention <= 0 {
			c.Targets[i].Retention = 30 * 24 * time.Hour
		}
		if c.Targets[i].TimeColumn == "" {
			c.Targets[i].TimeColumn = "created_at"
		}
	}
}

type ArchiveTableReport struct {
	Table      string
	Candidates int
	Moved      int
	Skipped    bool
	SkipReason string
}

type ArchiveReport struct {
	Tables   []ArchiveTableReport
	DryRun   bool
	Duration time.Duration
}

type ArchiveWorker struct {
	db   *gorm.DB
	cfg  ArchiveConfig
	Lock *lock.RedisLock
}

func NewArchiveWorker(db *gorm.DB, cfg ArchiveConfig) *ArchiveWorker {
	cfg.withDefaults()
	return &ArchiveWorker{db: db, cfg: cfg}
}

func (w *ArchiveWorker) runOnce(ctx context.Context) {
	if w.Lock != nil {
		ok, err := w.Lock.TryAcquire(ctx)
		if err != nil {
			zap.L().Warn("archive lock acquire failed, run anyway", zap.Error(err))
		} else if !ok {
			archiveRuns.WithLabelValues("skipped_locked").Inc()
			return
		} else {
			defer w.Lock.Release(ctx)
		}
	}

	report, err := w.ArchiveOnce(ctx)
	if err != nil {
		archiveRuns.WithLabelValues("error").Inc()
		zap.L().Error("archive run failed", zap.Error(err))
		return
	}
	archiveRuns.WithLabelValues("ok").Inc()
	for _, t := range report.Tables {
		if t.Moved > 0 || t.Skipped {
			zap.L().Info("archive table processed",
				zap.String("table", t.Table),
				zap.Int("candidates", t.Candidates),
				zap.Int("moved", t.Moved),
				zap.Bool("skipped", t.Skipped),
				zap.String("reason", t.SkipReason))
		}
	}
}

func (w *ArchiveWorker) Run(ctx context.Context) {
	if w.db == nil {
		zap.L().Warn("archive worker disabled: db is nil")
		return
	}

	zap.L().Info("archive worker started",
		zap.Duration("interval", w.cfg.Interval),
		zap.Bool("dry_run", w.cfg.DryRun),
		zap.Int("targets", len(w.cfg.Targets)))

	ticker := time.NewTicker(w.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			zap.L().Info("archive worker stopped")
			return
		case <-ticker.C:
			w.runOnce(ctx)
		}
	}
}

func (w *ArchiveWorker) ArchiveOnce(ctx context.Context) (ArchiveReport, error) {
	start := time.Now()
	report := ArchiveReport{DryRun: w.cfg.DryRun}

	if w.db == nil {
		return report, nil
	}

	for _, t := range w.cfg.Targets {
		tr, err := w.archiveTarget(ctx, t)
		if err != nil {
			tr.Skipped = true
			tr.SkipReason = err.Error()
			archiveFailed.WithLabelValues(t.Table).Inc()
			zap.L().Warn("archive target failed, continue with next",
				zap.String("table", t.Table), zap.Error(err))
		}
		report.Tables = append(report.Tables, tr)
	}

	report.Duration = time.Since(start)
	return report, nil
}

type archiveColumn struct {
	Name       string  `gorm:"column:name"`
	ColumnType string  `gorm:"column:column_type"`
	IsNullable string  `gorm:"column:is_nullable"`
	DefaultVal *string `gorm:"column:default_value"`
	Extra      string  `gorm:"column:extra"`
}

func (w *ArchiveWorker) tableColumns(ctx context.Context, table string) ([]archiveColumn, error) {
	var cols []archiveColumn
	err := w.db.WithContext(ctx).Raw(`SELECT COLUMN_NAME AS name, COLUMN_TYPE AS column_type,
		IS_NULLABLE AS is_nullable, COLUMN_DEFAULT AS default_value, EXTRA AS extra
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION`, table).Scan(&cols).Error
	return cols, err
}

func (w *ArchiveWorker) ensureArchiveTable(ctx context.Context, source, archive string) ([]archiveColumn, error) {
	if err := w.db.WithContext(ctx).
		Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` LIKE `%s`", archive, source)).Error; err != nil {
		return nil, err
	}

	srcCols, err := w.tableColumns(ctx, source)
	if err != nil {
		return nil, err
	}
	arcCols, err := w.tableColumns(ctx, archive)
	if err != nil {
		return nil, err
	}

	have := make(map[string]struct{}, len(arcCols))
	for _, c := range arcCols {
		have[c.Name] = struct{}{}
	}

	for _, c := range srcCols {
		if _, ok := have[c.Name]; ok {
			continue
		}
		ddl := fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN `%s` %s", archive, c.Name, c.ColumnType)
		if c.IsNullable == "NO" {
			ddl += " NOT NULL"
		} else {
			ddl += " NULL"
		}
		if c.DefaultVal != nil {
			ddl += " DEFAULT " + *c.DefaultVal
		}
		if err := w.db.WithContext(ctx).Exec(ddl).Error; err != nil {
			return nil, fmt.Errorf("align archive column %s: %w", c.Name, err)
		}
		zap.L().Warn("archive column added to match source",
			zap.String("archive", archive), zap.String("column", c.Name))
	}

	return w.tableColumns(ctx, archive)
}

func (w *ArchiveWorker) archiveTarget(ctx context.Context, t ArchiveTarget) (ArchiveTableReport, error) {
	out := ArchiveTableReport{Table: t.Table}

	cutoff := w.cutoffValue(t)

	var ids []int64
	sql := fmt.Sprintf("SELECT id FROM %s WHERE %s < ? ORDER BY id ASC LIMIT ?", t.Table, t.TimeColumn)
	if err := w.db.WithContext(ctx).Raw(sql, cutoff, t.Batch).Scan(&ids).Error; err != nil {
		return out, err
	}

	out.Candidates = len(ids)
	archiveCandidates.WithLabelValues(t.Table).Set(float64(len(ids)))

	if len(ids) == 0 {
		return out, nil
	}

	if w.cfg.DryRun {
		out.Skipped = true
		out.SkipReason = "dry_run"
		return out, nil
	}

	if _, err := w.ensureArchiveTable(ctx, t.Table, t.ArchiveTable); err != nil {
		archiveFailed.WithLabelValues(t.Table).Inc()
		out.Skipped = true
		out.SkipReason = "ensure archive table: " + err.Error()
		return out, nil
	}

	srcCols, err := w.tableColumns(ctx, t.Table)
	if err != nil {
		archiveFailed.WithLabelValues(t.Table).Inc()
		out.Skipped = true
		out.SkipReason = "load source columns: " + err.Error()
		return out, nil
	}

	names := make([]string, 0, len(srcCols))
	for _, c := range srcCols {
		names = append(names, "`"+c.Name+"`")
	}
	colList := strings.Join(names, ", ")

	placeholders := buildPlaceholders(len(ids))
	args := toArgs(ids)

	insertSQL := fmt.Sprintf("INSERT INTO `%s` (%s) SELECT %s FROM `%s` WHERE id IN (%s)",
		t.ArchiveTable, colList, colList, t.Table, placeholders)
	deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE id IN (%s)", t.Table, placeholders)

	var deleted int64
	err = w.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		insertRes := tx.Exec(insertSQL, args...)
		if insertRes.Error != nil {
			return fmt.Errorf("insert failed: %w", insertRes.Error)
		}
		if int(insertRes.RowsAffected) != len(ids) {
			return fmt.Errorf("insert rows mismatch: inserted=%d expected=%d", insertRes.RowsAffected, len(ids))
		}

		deleteRes := tx.Exec(deleteSQL, args...)
		if deleteRes.Error != nil {
			return fmt.Errorf("delete failed: %w", deleteRes.Error)
		}
		deleted = deleteRes.RowsAffected
		return nil
	})
	if err != nil {
		archiveFailed.WithLabelValues(t.Table).Inc()
		out.Skipped = true
		out.SkipReason = err.Error()
		return out, nil
	}

	archiveMoved.WithLabelValues(t.Table).Add(float64(deleted))
	out.Moved = int(deleted)
	return out, nil
}

func (w *ArchiveWorker) cutoffValue(t ArchiveTarget) interface{} {
	cutoff := time.Now().Add(-t.Retention)
	if t.UnixMilli {
		return cutoff.UnixMilli()
	}
	return cutoff
}

func buildPlaceholders(n int) string {
	if n <= 0 {
		return ""
	}
	var sb strings.Builder
	sb.Grow(n * 2)
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('?')
	}
	return sb.String()
}

func toArgs(ids []int64) []interface{} {
	args := make([]interface{}, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	return args
}
