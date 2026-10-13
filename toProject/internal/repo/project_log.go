package repo

import (
	"context"
	"test.com/toProject/internal/data"
)

type ProjectLogRepo interface {
	FindLogByTaskCode(ctx context.Context, taskCode int64, comment int) (list []*data.ProjectLog, total int64, err error)
	FindLogByTaskCodePage(ctx context.Context, taskCode int64, comment int, page int, pageSize int) (list []*data.ProjectLog, total int64, err error)
	SaveProjectLog(ctx context.Context, pl *data.ProjectLog) error
	FindLogByMemberCode(background context.Context, memberId int64, page int64, size int64) (list []*data.ProjectLog, total int64, err error)
}
