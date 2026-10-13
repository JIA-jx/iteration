package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type Register struct {
	EtcdAddrs   []string
	DialTimeout int
	closeCh     chan struct{}
	leasesID    clientv3.LeaseID
	keepAliveCh <-chan *clientv3.LeaseKeepAliveResponse
	srvInfo     Server
	srvTTL      int64

	cli      *clientv3.Client
	logger   *zap.Logger
	mu       sync.Mutex
	stopOnce sync.Once
}

func NewRegister(etcdAddrs []string, logger *zap.Logger) *Register {
	return &Register{
		EtcdAddrs:   etcdAddrs,
		DialTimeout: 3,
		logger:      logger,
	}
}

func (r *Register) Register(srvInfo Server, ttl int64) (chan<- struct{}, error) {
	var err error

	if strings.Split(srvInfo.Addr, ":")[0] == "" {
		return nil, errors.New("invalid ip")
	}

	if r.cli, err = clientv3.New(clientv3.Config{
		Endpoints:   r.EtcdAddrs,
		DialTimeout: time.Duration(r.DialTimeout) * time.Second,
	}); err != nil {
		return nil, err
	}

	r.srvInfo = srvInfo
	r.srvTTL = ttl

	if err = r.register(); err != nil {
		if r.cli != nil {
			_ = r.cli.Close()
		}
		return nil, err
	}

	r.closeCh = make(chan struct{}, 1)

	go r.keepAlive()

	return r.closeCh, nil
}

func (r *Register) Stop() {
	r.notifyStop()

	r.stopOnce.Do(func() {
		r.mu.Lock()
		cli := r.cli
		leaseID := r.leasesID
		r.mu.Unlock()

		if cli == nil {
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		if err := r.unregister(ctx); err != nil {
			r.logger.Error("unregister failed", zap.Error(err))
		} else {
			r.logger.Info("unregistered from etcd", zap.String("path", BuildRegPath(r.srvInfo)))
		}
		if leaseID != 0 {
			if _, err := cli.Revoke(ctx, leaseID); err != nil {
				r.logger.Error("revoke lease failed", zap.Error(err))
			}
		}
		if err := cli.Close(); err != nil {
			r.logger.Error("close etcd client failed", zap.Error(err))
		}
	})
}

func (r *Register) notifyStop() {
	if r.closeCh == nil {
		return
	}
	select {
	case r.closeCh <- struct{}{}:
	default:
	}
}

func (r *Register) RegisterWithStop(srvInfo Server, ttl int64) (func(), error) {
	ch, err := r.Register(srvInfo, ttl)
	if err != nil {
		return nil, err
	}

	var stopOnce sync.Once
	return func() {
		stopOnce.Do(func() {
			unregisterCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			if _, err := r.cli.Delete(unregisterCtx, BuildRegPath(r.srvInfo)); err != nil {
				r.logger.Error("unregister failed", zap.Error(err))
			} else {
				r.logger.Info("unregistered from etcd", zap.String("path", BuildRegPath(r.srvInfo)))
			}
			if _, err := r.cli.Revoke(unregisterCtx, r.leasesID); err != nil {
				r.logger.Error("revoke lease failed", zap.Error(err))
			}

			select {
			case ch <- struct{}{}:
			default:
			}

			if err := r.cli.Close(); err != nil {
				r.logger.Error("close etcd client failed", zap.Error(err))
			}
		})
	}, nil
}

func (r *Register) register() error {
	leaseCtx, cancel := context.WithTimeout(context.Background(), time.Duration(r.DialTimeout)*time.Second)
	defer cancel()

	leaseResp, err := r.cli.Grant(leaseCtx, r.srvTTL)
	if err != nil {
		return err
	}

	keepAliveCh, err := r.cli.KeepAlive(context.Background(), leaseResp.ID)
	if err != nil {
		return err
	}

	data, err := json.Marshal(r.srvInfo)
	if err != nil {
		return err
	}
	if _, err = r.cli.Put(context.Background(), BuildRegPath(r.srvInfo), string(data), clientv3.WithLease(leaseResp.ID)); err != nil {
		return err
	}

	r.mu.Lock()
	r.leasesID = leaseResp.ID
	r.keepAliveCh = keepAliveCh
	r.mu.Unlock()
	return nil
}

func (r *Register) unregister(ctx context.Context) error {
	_, err := r.cli.Delete(ctx, BuildRegPath(r.srvInfo))
	return err
}

func (r *Register) keepAlive() {
	ticker := time.NewTicker(time.Duration(r.srvTTL) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.closeCh:
			return
		case res, ok := <-r.keepAliveCh:
			if !ok || res == nil {
				r.mu.Lock()
				r.keepAliveCh = nil
				r.mu.Unlock()
				if err := r.register(); err != nil {
					r.logger.Error("register failed", zap.Error(err))
				}
			}
		case <-ticker.C:
			r.mu.Lock()
			keepAliveCh := r.keepAliveCh
			r.mu.Unlock()
			if keepAliveCh == nil {
				if err := r.keepAliveOnce(); err != nil {
					r.logger.Error("keep alive failed", zap.Error(err))
				}
			}
		}
	}
}

func (r *Register) keepAliveOnce() error {
	r.mu.Lock()
	cli := r.cli
	leaseID := r.leasesID
	r.mu.Unlock()

	if cli == nil || leaseID == 0 {
		return r.register()
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(r.DialTimeout)*time.Second)
	defer cancel()

	if _, err := cli.KeepAliveOnce(ctx, leaseID); err != nil {
		r.logger.Error("keepAliveOnce failed", zap.Error(err))
		return r.register()
	}
	return nil
}
