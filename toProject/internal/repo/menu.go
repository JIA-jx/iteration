package repo

import (
	"context"
	"test.com/toProject/internal/data"
)

type MenuRepo interface {
	FindMenus(ctx context.Context) ([]*data.ProjectMenu, error)
}
