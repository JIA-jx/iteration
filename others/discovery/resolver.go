package discovery

import (
	"context"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc/resolver"
)

const (
	schema = "etcd"
)

type Resolver struct {
	schema      string
	EtcdAddrs   []string
	DialTimeout int
	logger      *zap.Logger

	keyPrefix    string
	srvAddrsList []resolver.Address
	cc           resolver.ClientConn
	watchCh      clientv3.WatchChan
	cli          *clientv3.Client
	closeCh      chan struct{}

	mu       sync.RWMutex
	closed   bool
	stopOnce sync.Once
}

func NewResolver(etcdAddrs []string, logger *zap.Logger) *Resolver {
	return &Resolver{
		schema:      schema,
		EtcdAddrs:   etcdAddrs,
		DialTimeout: 3,
		logger:      logger,
	}
}

func (r *Resolver) Scheme() string {
	return r.schema
}

func (r *Resolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r.cc = cc

	r.keyPrefix = BuildPrefix(Server{Name: target.Endpoint(), Version: target.URL.Host})
	if _, err := r.start(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Resolver) ResolveNow(o resolver.ResolveNowOptions) {}

func (r *Resolver) Close() {
	r.stopOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.mu.Unlock()
		close(r.closeCh)
	})
}

func (r *Resolver) start() (chan<- struct{}, error) {
	var err error
	r.cli, err = clientv3.New(clientv3.Config{
		Endpoints:   r.EtcdAddrs,
		DialTimeout: time.Duration(r.DialTimeout) * time.Second,
	})
	if err != nil {
		return nil, err
	}

	r.closeCh = make(chan struct{})

	if err = r.sync(); err != nil {
		return nil, err
	}

	go r.watch()

	return r.closeCh, nil
}

func (r *Resolver) watch() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		if err := r.watchOnce(ticker); err != nil {
			r.logger.Warn("etcd watch interrupted, rebuilding", zap.String("prefix", r.keyPrefix), zap.Error(err))

			if !r.sleep(500 * time.Millisecond) {
				return
			}
			if syncErr := r.sync(); syncErr != nil {
				r.logger.Error("resync after watch interrupt failed", zap.Error(syncErr))
				if !r.sleep(time.Second) {
					return
				}
			}
			continue
		}
		return
	}
}

func (r *Resolver) watchOnce(ticker *time.Ticker) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r.watchCh = r.cli.Watch(ctx, r.keyPrefix, clientv3.WithPrefix())

	for {
		select {
		case <-r.closeCh:
			return nil
		case res, ok := <-r.watchCh:
			if !ok {
				return context.Canceled
			}
			if err := res.Err(); err != nil {
				return err
			}
			r.update(res.Events)
		case <-ticker.C:
			if err := r.sync(); err != nil {
				r.logger.Error("sync failed", zap.Error(err))
			}
		}
	}
}

func (r *Resolver) sleep(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-r.closeCh:
		return false
	case <-timer.C:
		return true
	}
}

func (r *Resolver) update(events []*clientv3.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, evt := range events {
		var info Server
		var err error

		switch evt.Type {
		case mvccpb.PUT:
			info, err = ParseValue(evt.Kv.Value)
			if err != nil {
				continue
			}
			addr := resolver.Address{Addr: info.Addr, Metadata: info.Weight}
			if !Exist(r.srvAddrsList, addr) {
				r.srvAddrsList = append(r.srvAddrsList, addr)
				r.updateStateLocked()
			}
		case mvccpb.DELETE:
			info, err = SplitPath(string(evt.Kv.Key))
			if err != nil {
				continue
			}
			addr := resolver.Address{Addr: info.Addr}
			if s, ok := Remove(r.srvAddrsList, addr); ok {
				r.srvAddrsList = s
				r.updateStateLocked()
			}
		}
	}
}

func (r *Resolver) updateStateLocked() {
	if r.closed {
		return
	}
	addrs := make([]resolver.Address, len(r.srvAddrsList))
	copy(addrs, r.srvAddrsList)
	if err := r.cc.UpdateState(resolver.State{Addresses: addrs}); err != nil {
		r.logger.Error("update resolver state failed", zap.Error(err))
	}
}

func (r *Resolver) sync() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	res, err := r.cli.Get(ctx, r.keyPrefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	list := make([]resolver.Address, 0, len(res.Kvs))
	for _, v := range res.Kvs {
		info, err := ParseValue(v.Value)
		if err != nil {
			continue
		}
		list = append(list, resolver.Address{Addr: info.Addr, Metadata: info.Weight})
	}

	r.mu.Lock()
	r.srvAddrsList = list
	r.updateStateLocked()
	r.mu.Unlock()

	return nil
}
