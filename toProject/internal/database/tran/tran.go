package tran

import (
	"context"
	"test.com/toProject/internal/database"
)

type Transaction interface {
	Action(func(conn database.DbConn) error) error
	ActionWithCtx(context.Context, func(txCtx context.Context, conn database.DbConn) error) error
}
