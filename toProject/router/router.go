package router

import (
	"github.com/gin-gonic/gin"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
	"log"
	"net"
	common "test.com/others"
	"test.com/others/discovery"
	"test.com/others/logs"
	"test.com/rpcOrgrpc/account"
	"test.com/rpcOrgrpc/analysis"
	"test.com/rpcOrgrpc/auth"
	"test.com/rpcOrgrpc/department"
	"test.com/rpcOrgrpc/file"
	"test.com/rpcOrgrpc/fileupload"
	"test.com/rpcOrgrpc/menu"
	"test.com/rpcOrgrpc/project"
	"test.com/rpcOrgrpc/system"
	"test.com/rpcOrgrpc/task"
	"test.com/toProject/config"
	"test.com/toProject/internal/interceptor"
	"test.com/toProject/internal/rpc"
	account_service_v1 "test.com/toProject/pkg/service/account.service.v1"
	analysis_service_v1 "test.com/toProject/pkg/service/analysis_service.v1"
	auth_service_v1 "test.com/toProject/pkg/service/auth.service.v1"
	department_service_v1 "test.com/toProject/pkg/service/department.service.v1"
	file_service_v1 "test.com/toProject/pkg/service/file_service.v1"
	menu_service_v1 "test.com/toProject/pkg/service/menu.service.v1"
	project_service_v1 "test.com/toProject/pkg/service/project.service.v1"
	system_service_v1 "test.com/toProject/pkg/service/system_service.v1"
	task_service_v1 "test.com/toProject/pkg/service/task.service.v1"
	upload_service_v1 "test.com/toProject/pkg/service/upload.service.v1"
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
			project.RegisterProjectServiceServer(g, project_service_v1.New())
			task.RegisterTaskServiceServer(g, task_service_v1.New())
			account.RegisterAccountServiceServer(g, account_service_v1.New())
			department.RegisterDepartmentServiceServer(g, department_service_v1.New())
			auth.RegisterAuthServiceServer(g, auth_service_v1.New())
			menu.RegisterMenuServiceServer(g, menu_service_v1.New())
			file.RegisterFileServiceServer(g, file_service_v1.New())
			system.RegisterSystemServiceServer(g, system_service_v1.New())
			analysis.RegisterAnalysisServiceServer(g, analysis_service_v1.New())
			fileupload.RegisterChunkUploadServiceServer(g, upload_service_v1.New())
		}}
	s := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			common.InternalAuthServerInterceptor(),
			interceptor.RecoveryUnaryInterceptor(),
			interceptor.TenantUnaryInterceptor(),
			interceptor.NewRateLimitInterceptor(
				interceptor.DefaultTenantRule(),
				interceptor.DefaultMemberRule(),
			).Unary(),
			interceptor.New().CacheInterceptor(),
		)),
	)
	c.RegisterFunc(s)
	lis, err := net.Listen("tcp", c.Addr)
	if err != nil {
		log.Fatalf("grpc listen failed on %s: %v", c.Addr, err)
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

func InitUserRpc() {
	rpc.InitRpcUserClient()
}
