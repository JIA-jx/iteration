package rpc

import (
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"log"
	"test.com/api/config"
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
)

var ProjectServiceClient project.ProjectServiceClient
var TaskServiceClient task.TaskServiceClient
var AccountServiceClient account.AccountServiceClient
var DepartmentServiceClient department.DepartmentServiceClient
var AuthServiceClient auth.AuthServiceClient
var MenuServiceClient menu.MenuServiceClient
var FileServiceClient file.FileServiceClient
var SystemServiceClient system.SystemServiceClient
var AnalysisServiceClient analysis.AnalysisServiceClient
var ChunkUploadServiceClient fileupload.ChunkUploadServiceClient

func InitRpcIterationClient() {
	etcdRegister := discovery.NewResolver(config.C.EC.Addrs, logs.LG)
	resolver.Register(etcdRegister)

	conn, err := grpc.Dial(
		"etcd:///project",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithChainUnaryInterceptor(MetadataInterceptor()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
	)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	ProjectServiceClient = project.NewProjectServiceClient(conn)
	TaskServiceClient = task.NewTaskServiceClient(conn)
	AccountServiceClient = account.NewAccountServiceClient(conn)
	DepartmentServiceClient = department.NewDepartmentServiceClient(conn)
	AuthServiceClient = auth.NewAuthServiceClient(conn)
	MenuServiceClient = menu.NewMenuServiceClient(conn)
	FileServiceClient = file.NewFileServiceClient(conn)
	SystemServiceClient = system.NewSystemServiceClient(conn)
	AnalysisServiceClient = analysis.NewAnalysisServiceClient(conn)
	ChunkUploadServiceClient = fileupload.NewChunkUploadServiceClient(conn)
}
