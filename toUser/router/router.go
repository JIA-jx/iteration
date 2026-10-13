package router

import (
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
	"log"
	"net"
	common "test.com/others"
	"test.com/others/discovery"
	"test.com/others/logs"
	"test.com/rpcOrgrpc/user/login"
	"test.com/toUser/config"
	"test.com/toUser/internal/interceptor"

	loginServiceV1 "test.com/toUser/pkg/service/login.service.v1"
)

type Router interface {
	Route(r *gin.Engine)
}

type RegisterRouter struct {
}

func New() *RegisterRouter {
	return &RegisterRouter{}
}

func (*RegisterRouter) Route(ro Router, r *gin.Engine) {
	ro.Route(r)
}

var routers []Router

func InitRouter(r *gin.Engine) {
	for _, ro := range routers {
		ro.Route(r)
	}
}

type gRPCConfig struct {
	Addr         string
	RegisterFunc func(*grpc.Server)
}

func RegisterGrpc() *grpc.Server {
	c := gRPCConfig{
		Addr: config.C.GC.Addr,
		RegisterFunc: func(g *grpc.Server) {
			login.RegisterLoginServiceServer(g, loginServiceV1.New())
		}}
	s := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(
			common.InternalAuthServerInterceptor(),
			interceptor.RecoveryUnaryInterceptor(),
			interceptor.TenantUnaryInterceptor(),
		),
	)

	c.RegisterFunc(s)
	lis, err := net.Listen("tcp", c.Addr)
	if err != nil {
		log.Printf("cannot listen: %v", err)
		return s
	}
	go func() {
		log.Printf("grpc server started as: %s \n", c.Addr)
		err = s.Serve(lis)
		if err != nil {
			log.Println("server started error", err)
			return
		}
	}()
	return s
}

func RegisterEtcdServer() func() {
	etcdRegister := discovery.NewResolver(config.C.EtcdConfig.Addrs, logs.LG)
	resolver.Register(etcdRegister)

	info := discovery.Server{
		Name:    config.C.GC.Name,
		Addr:    config.C.GC.EtcdAddr,
		Version: config.C.GC.Version,
		Weight:  config.C.GC.Weight,
	}
	zap.L().Info("register grpc addr: ", zap.String("addr", info.Addr))
	r := discovery.NewRegister(config.C.EtcdConfig.Addrs, logs.LG)
	stop, err := r.RegisterWithStop(info, 2)
	if err != nil {
		log.Fatalln(err)
	}
	return stop
}
