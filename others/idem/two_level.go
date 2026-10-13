package idem

import (
	"context"
	"time"
)

type twoLevelStore struct {
	fast      Store
	authority Store
}

func NewTwoLevelStore(fast, authority Store) Store {
	if fast == nil {
		return authority
	}
	if authority == nil {
		return fast
	}
	return &twoLevelStore{fast: fast, authority: authority}
}

func (s *twoLevelStore) TryAcquire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	fastOK, fastErr := s.fast.TryAcquire(ctx, key, ttl)
	if fastErr == nil && !fastOK {
		return false, nil
	}

	authOK, authErr := s.authority.TryAcquire(ctx, key, ttl)
	if authErr != nil {
		idemAuthorityFailed.Inc()
		if fastErr == nil && fastOK {
			s.releaseFast(key, ttl)
		}
		return false, authErr
	}
	if !authOK {
		if fastErr == nil && fastOK {
			s.markFastDuplicated(key, ttl)
		}
		return false, nil
	}
	return true, nil
}

func (s *twoLevelStore) Get(ctx context.Context, key string) (*Record, error) {
	rec, err := s.authority.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if rec != nil {
		return rec, nil
	}
	return s.fast.Get(ctx, key)
}

func (s *twoLevelStore) Complete(ctx context.Context, key, result string, ttl time.Duration) error {
	if err := s.authority.Complete(ctx, key, result, ttl); err != nil {
		return err
	}
	bg, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.fast.Complete(bg, key, result, ttl)
	return nil
}

func (s *twoLevelStore) Fail(ctx context.Context, key, reason string, ttl time.Duration) error {
	if err := s.authority.Fail(ctx, key, reason, ttl); err != nil {
		return err
	}
	bg, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.fast.Fail(bg, key, reason, ttl)
	return nil
}

func (s *twoLevelStore) Release(ctx context.Context, key string) error {
	authErr := s.authority.Release(ctx, key)
	bg, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.fast.Release(bg, key)
	return authErr
}

func (s *twoLevelStore) releaseFast(key string, ttl time.Duration) {
	bg, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.fast.Release(bg, key)
}

func (s *twoLevelStore) markFastDuplicated(key string, ttl time.Duration) {
	bg, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.fast.Fail(bg, key, "duplicated in authority store", ttl)
}
