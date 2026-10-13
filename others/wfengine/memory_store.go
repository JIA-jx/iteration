package wfengine

import (
	"context"
	"sort"
	"strings"
	"sync"
)

type MemoryStore struct {
	mu        sync.RWMutex
	instances map[string]*Instance
	order     []string
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		instances: make(map[string]*Instance),
	}
}

func (s *MemoryStore) Create(ctx context.Context, inst *Instance) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if inst == nil {
		return invalidRequest("instance is nil")
	}
	if inst.Id == "" {
		return invalidRequest("instance id is empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.instances[inst.Id]; ok {
		return invalidRequest("instance %s already exists", inst.Id)
	}
	saved := cloneInstance(inst)
	if saved.Version == 0 {
		saved.Version = 1
	}
	s.instances[saved.Id] = saved
	s.order = append(s.order, saved.Id)
	inst.Version = saved.Version
	return nil
}

func (s *MemoryStore) Get(ctx context.Context, id string) (*Instance, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	inst, ok := s.instances[id]
	if !ok {
		return nil, instanceNotFound(id)
	}
	return cloneInstance(inst), nil
}

func (s *MemoryStore) Update(ctx context.Context, id string, expectedVersion int64, mutate func(*Instance) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if mutate == nil {
		return invalidRequest("mutate function is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	inst, ok := s.instances[id]
	if !ok {
		return instanceNotFound(id)
	}
	if inst.Version != expectedVersion {
		return versionConflict(id, expectedVersion, inst.Version)
	}
	working := cloneInstance(inst)
	if err := mutate(working); err != nil {
		return err
	}
	working.Version = inst.Version + 1
	s.instances[id] = working
	return nil
}

func (s *MemoryStore) ListByBiz(ctx context.Context, tenantId int64, bizId string) ([]*Instance, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.list(false, tenantId, bizId), nil
}

func (s *MemoryStore) ListAll(ctx context.Context) ([]*Instance, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.list(true, 0, ""), nil
}

func (s *MemoryStore) list(everyTenant bool, tenantId int64, bizId string) []*Instance {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Instance, 0, len(s.order))
	for _, id := range s.order {
		inst, ok := s.instances[id]
		if !ok {
			continue
		}
		if !everyTenant && inst.TenantId != tenantId {
			continue
		}
		if bizId != "" && inst.BizId != bizId {
			continue
		}
		out = append(out, cloneInstance(inst))
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out
}

func (s *MemoryStore) ListPendingTasks(ctx context.Context, approverId int64, tenantId int64) ([]*Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Task, 0)
	for _, id := range s.order {
		inst, ok := s.instances[id]
		if !ok {
			continue
		}
		if inst.TenantId != tenantId {
			continue
		}
		for _, t := range inst.Tasks {
			if t.Status != TaskPending {
				continue
			}
			if approverId > 0 && t.ApproverId != approverId {
				continue
			}
			out = append(out, cloneTask(t))
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].Id < out[j].Id
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out, nil
}

func (s *MemoryStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.instances)
}

func (s *MemoryStore) Snapshot(id string) (*Instance, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	inst, ok := s.instances[id]
	if !ok {
		return nil, false
	}
	return cloneInstance(inst), true
}

func (s *MemoryStore) AllPending(tenantId int64) ([]*Task, error) {
	return s.ListPendingTasks(context.Background(), 0, tenantId)
}

func (s *MemoryStore) FindTask(taskId string) (*Task, string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, id := range s.order {
		inst, ok := s.instances[id]
		if !ok {
			continue
		}
		for _, t := range inst.Tasks {
			if t.Id == taskId {
				return cloneTask(t), id, true
			}
		}
	}
	return nil, "", false
}

func (s *MemoryStore) IDsWithPrefix(prefix string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0)
	for _, id := range s.order {
		inst, ok := s.instances[id]
		if !ok {
			continue
		}
		if strings.HasPrefix(inst.Id, prefix) {
			out = append(out, inst.Id)
		}
	}
	return out
}
