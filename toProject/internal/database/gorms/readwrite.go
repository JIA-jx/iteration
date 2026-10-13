package gorms

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"gorm.io/gorm"
)

type readMode int

const (
	readAuto readMode = iota
	readMaster
	readSlave
)

type readModeKey struct{}

type ScopeFunc func(ctx context.Context) string

type ReplicationStatsSnapshot struct {
	Reads        int64
	MasterReads  int64
	SlaveReads   int64
	AutoReads    int64
	FallbackRead int64
}

var (
	defaultReplicationLag = 3 * time.Second
	replicationLagNanos   atomic.Int64

	scopeFn atomic.Value

	trackerMu   sync.Mutex
	writeMarker = make(map[string]time.Time)

	statReads        atomic.Int64
	statMasterReads  atomic.Int64
	statSlaveReads   atomic.Int64
	statAutoReads    atomic.Int64
	statFallbackRead atomic.Int64
)

const maxTrackedScopes = 65536

func init() {
	replicationLagNanos.Store(int64(defaultReplicationLag))
	scopeFn.Store(ScopeFunc(func(ctx context.Context) string { return "default" }))
}

func SetScopeExtractor(f ScopeFunc) {
	if f == nil {
		return
	}
	scopeFn.Store(f)
}

func SetReplicationLag(d time.Duration) {
	if d < 0 {
		d = 0
	}
	replicationLagNanos.Store(int64(d))
}

func ReplicationLag() time.Duration {
	return time.Duration(replicationLagNanos.Load())
}

func MarkWrite(ctx context.Context) {
	scope := currentScope(ctx)
	now := time.Now()

	trackerMu.Lock()
	defer trackerMu.Unlock()

	if len(writeMarker) >= maxTrackedScopes {
		sweepLocked(now)
	}
	writeMarker[scope] = now
}

func ShouldReadMaster(ctx context.Context) bool {
	lag := ReplicationLag()
	if lag <= 0 {
		return false
	}

	trackerMu.Lock()
	defer trackerMu.Unlock()

	at, ok := writeMarker[currentScope(ctx)]
	if !ok {
		return false
	}
	if time.Since(at) >= lag {
		delete(writeMarker, currentScope(ctx))
		return false
	}
	return true
}

func WithForceMaster(ctx context.Context) context.Context {
	return context.WithValue(ctx, readModeKey{}, readMaster)
}

func WithForceSlave(ctx context.Context) context.Context {
	return context.WithValue(ctx, readModeKey{}, readSlave)
}

func Reader(ctx context.Context) *gorm.DB {
	statReads.Add(1)

	switch modeOf(ctx) {
	case readMaster:
		statMasterReads.Add(1)
		return masterDB
	case readSlave:
		statSlaveReads.Add(1)
		return slave()
	}

	statAutoReads.Add(1)
	if ShouldReadMaster(ctx) {
		statMasterReads.Add(1)
		return masterDB
	}

	db := slave()
	if db == masterDB {
		statFallbackRead.Add(1)
	}
	return db
}

func ReplicationStats() ReplicationStatsSnapshot {
	return ReplicationStatsSnapshot{
		Reads:        statReads.Load(),
		MasterReads:  statMasterReads.Load(),
		SlaveReads:   statSlaveReads.Load(),
		AutoReads:    statAutoReads.Load(),
		FallbackRead: statFallbackRead.Load(),
	}
}

func modeOf(ctx context.Context) readMode {
	if ctx == nil {
		return readAuto
	}
	if v, ok := ctx.Value(readModeKey{}).(readMode); ok {
		return v
	}
	return readAuto
}

func currentScope(ctx context.Context) string {
	f, ok := scopeFn.Load().(ScopeFunc)
	if !ok || f == nil {
		return "default"
	}
	return f(ctx)
}

func sweepLocked(now time.Time) {
	lag := ReplicationLag()
	for k, at := range writeMarker {
		if now.Sub(at) >= lag {
			delete(writeMarker, k)
		}
	}
	if len(writeMarker) < maxTrackedScopes {
		return
	}

	type marker struct {
		scope string
		at    time.Time
	}
	markers := make([]marker, 0, len(writeMarker))
	for k, at := range writeMarker {
		markers = append(markers, marker{scope: k, at: at})
	}
	sort.Slice(markers, func(i, j int) bool { return markers[i].at.Before(markers[j].at) })

	target := maxTrackedScopes / 2
	for i := 0; i < len(markers) && len(writeMarker) > target; i++ {
		delete(writeMarker, markers[i].scope)
	}
}
