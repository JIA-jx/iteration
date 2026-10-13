package project_service_v1

import (
	"context"
	"fmt"
	lru "github.com/hashicorp/golang-lru"
	"github.com/jinzhu/copier"
	"test.com/others/errs"
	"test.com/others/tenant"
	"test.com/rpcOrgrpc/project"
)

var nodeTreeLRU, _ = lru.NewARC(1000)

func (ps *ProjectService) NodeList(ctx context.Context, msg *project.ProjectRpcMessage) (*project.ProjectNodeResponseMessage, error) {
	cacheKey := fmt.Sprintf("project:node:tree:%d", tenant.IdFromContext(ctx))
	if val, ok := nodeTreeLRU.Get(cacheKey); ok {
		return val.(*project.ProjectNodeResponseMessage), nil
	}

	list, err := ps.nodeDomain.TreeList(ctx)
	if err != nil {
		return nil, errs.GrpcError(err)
	}

	var nodes []*project.ProjectNodeMessage
	copier.Copy(&nodes, list)

	resp := &project.ProjectNodeResponseMessage{
		Nodes: nodes,
	}

	nodeTreeLRU.Add(cacheKey, resp)

	return resp, nil
}
