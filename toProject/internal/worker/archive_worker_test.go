package worker

import (
	"strings"
	"testing"
	"time"
)

func TestBuildPlaceholders(t *testing.T) {
	cases := map[int]string{
		0: "",
		1: "?",
		3: "?,?,?",
	}
	for n, want := range cases {
		if got := buildPlaceholders(n); got != want {
			t.Fatalf("buildPlaceholders(%d) = %q, want %q", n, got, want)
		}
	}
}

func TestToArgsPreservesOrder(t *testing.T) {
	ids := []int64{7, 3, 9}
	args := toArgs(ids)
	if len(args) != 3 {
		t.Fatalf("args 长度 = %d", len(args))
	}
	for i, id := range ids {
		if args[i].(int64) != id {
			t.Fatalf("args[%d] = %v, want %d", i, args[i], id)
		}
	}
}

func TestCutoffValueHonoursUnixMilli(t *testing.T) {
	w := &ArchiveWorker{}

	milli := w.cutoffValue(ArchiveTarget{Retention: time.Hour, UnixMilli: true})
	if _, ok := milli.(int64); !ok {
		t.Fatalf("UnixMilli 目标应产出毫秒时间戳, got %T", milli)
	}
	ms := milli.(int64)
	if ms > time.Now().UnixMilli() {
		t.Fatalf("cutoff 不应晚于当前时间")
	}
	if time.Now().UnixMilli()-ms < int64(50*time.Minute/time.Millisecond) {
		t.Fatalf("cutoff 应为 1 小时前, got %d", ms)
	}

	dt := w.cutoffValue(ArchiveTarget{Retention: time.Hour})
	if _, ok := dt.(time.Time); !ok {
		t.Fatalf("非 UnixMilli 目标应产出 time.Time, got %T", dt)
	}
}

func TestDefaultArchiveTargetsCoverHotTables(t *testing.T) {
	targets := DefaultArchiveTargets()
	if len(targets) == 0 {
		t.Fatalf("默认归档目标不应为空")
	}

	seen := make(map[string]bool)
	for _, tg := range targets {
		if tg.Table == "" || tg.ArchiveTable == "" {
			t.Fatalf("归档目标缺少表名: %+v", tg)
		}
		if tg.Table == tg.ArchiveTable {
			t.Fatalf("归档表不能等于源表: %s", tg.Table)
		}
		if !strings.HasSuffix(tg.ArchiveTable, "_archive") {
			t.Fatalf("归档表命名应带 _archive 后缀: %s", tg.ArchiveTable)
		}
		seen[tg.Table] = true
	}

	for _, required := range []string{"iteration_log", "notification", "dead_letter", "delay_task", "idempotent_record", "audit_event"} {
		if !seen[required] {
			t.Fatalf("缺少关键表的归档目标: %s", required)
		}
	}
}

func TestArchiveConfigAppliesDefaults(t *testing.T) {
	cfg := ArchiveConfig{}
	cfg.withDefaults()

	if cfg.Interval != time.Hour {
		t.Fatalf("Interval 默认 = %v", cfg.Interval)
	}
	if len(cfg.Targets) != len(DefaultArchiveTargets()) {
		t.Fatalf("未配置时应使用默认目标")
	}
	for _, tg := range cfg.Targets {
		if tg.Batch <= 0 {
			t.Fatalf("%s Batch 未设置", tg.Table)
		}
		if tg.Retention <= 0 {
			t.Fatalf("%s Retention 未设置", tg.Table)
		}
		if tg.TimeColumn == "" {
			t.Fatalf("%s TimeColumn 未设置", tg.Table)
		}
	}
}

func TestArchiveConfigFillsBatchWhenUnset(t *testing.T) {
	cfg := ArchiveConfig{
		Targets: []ArchiveTarget{{Table: "t1", ArchiveTable: "t1_archive", TimeColumn: "created_at"}},
	}
	cfg.withDefaults()

	if cfg.Targets[0].Batch != 1000 {
		t.Fatalf("未显式配置 Batch 时应填 1000, got %d", cfg.Targets[0].Batch)
	}
	if cfg.Targets[0].Retention != 30*24*time.Hour {
		t.Fatalf("未显式配置 Retention 时应填 30 天, got %v", cfg.Targets[0].Retention)
	}
}

func TestReconcileConfigAppliesDefaults(t *testing.T) {
	cfg := ReconcileConfig{}
	cfg.withDefaults()

	if cfg.Interval != 5*time.Minute {
		t.Fatalf("Interval 默认 = %v", cfg.Interval)
	}
	if cfg.Batch != 500 {
		t.Fatalf("Batch 默认 = %d", cfg.Batch)
	}
	if cfg.Repair {
		t.Fatalf("默认不开启自动修复")
	}
}
