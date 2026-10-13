package idem

import (
	"context"
	"sync"
	"time"
)

type memEntry struct {
	rec    *Record
	locked bool
}

type MemoryStore struct {
	mu      sync.Mutex
	entries map[string]*memEntry
	now     func() time.Time
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{entries: make(map[string]*memEntry), now: time.Now}
}

func (m *MemoryStore) TryAcquire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.entries[key]
	if ok && e.rec != nil && !e.rec.ExpireAt.IsZero() && !m.now().Before(e.rec.ExpireAt) {
		delete(m.entries, key)
		ok = false
	}

	if ok && e.locked {
		return false, nil
	}
	if ok && e.rec != nil && e.rec.Status == StatusSucceeded {
		return false, nil
	}

	if !ok {
		e = &memEntry{}
		m.entries[key] = e
	}
	e.locked = true
	e.rec = &Record{
		Key:       key,
		Status:    StatusProcessing,
		CreatedAt: m.now(),
		UpdatedAt: m.now(),
		ExpireAt:  m.now().Add(ttl),
	}
	return true, nil
}

func (m *MemoryStore) Get(ctx context.Context, key string) (*Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.entries[key]
	if !ok || e.rec == nil {
		return nil, nil
	}
	if !e.rec.ExpireAt.IsZero() && !m.now().Before(e.rec.ExpireAt) {
		delete(m.entries, key)
		return nil, nil
	}
	cp := *e.rec
	return &cp, nil
}

func (m *MemoryStore) Complete(ctx context.Context, key, result string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.entries[key]
	if !ok {
		e = &memEntry{}
		m.entries[key] = e
	}
	e.locked = false
	e.rec = &Record{
		Key:       key,
		Status:    StatusSucceeded,
		Result:    result,
		CreatedAt: m.now(),
		UpdatedAt: m.now(),
		ExpireAt:  m.now().Add(ttl),
	}
	return nil
}

func (m *MemoryStore) Fail(ctx context.Context, key, reason string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.entries[key]
	if !ok {
		e = &memEntry{}
		m.entries[key] = e
	}
	e.locked = false
	e.rec = &Record{
		Key:       key,
		Status:    StatusFailed,
		Reason:    reason,
		CreatedAt: m.now(),
		UpdatedAt: m.now(),
		ExpireAt:  m.now().Add(ttl),
	}
	return nil
}

func (m *MemoryStore) Release(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.entries[key]
	if !ok {
		return nil
	}
	e.locked = false
	if e.rec != nil && e.rec.Status == StatusProcessing {
		delete(m.entries, key)
	}
	return nil
}
