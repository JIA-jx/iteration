package dao

import (
	"context"
	"test.com/others/errs"
	"test.com/toProject/internal/database"
	"test.com/toProject/internal/database/gorms"
)

type TransactionImpl struct {
	conn database.DbConn
}

func normalizeTxErr(err error) error {
	if err == nil {
		return nil
	}
	if mErr, ok := err.(*errs.MError); ok && mErr == nil {
		return nil
	}
	return err
}

func (t *TransactionImpl) Action(f func(conn database.DbConn) error) error {
	conn := gorms.NewTran()
	conn.Begin()
	if err := normalizeTxErr(f(conn)); err != nil {
		conn.Rollback()
		return err
	}
	if err := conn.Commit(); err != nil {
		conn.Rollback()
		return err
	}
	return nil
}

func (t *TransactionImpl) ActionWithCtx(ctx context.Context, f func(txCtx context.Context, conn database.DbConn) error) error {
	conn := gorms.NewTran()
	conn.Begin()
	txCtx := ctx
	if txCtx == nil {
		txCtx = context.Background()
	}
	if err := normalizeTxErr(f(txCtx, conn)); err != nil {
		conn.Rollback()
		return err
	}
	if err := conn.Commit(); err != nil {
		conn.Rollback()
		return err
	}
	return nil
}

func NewTransaction() *TransactionImpl {
	return &TransactionImpl{
		conn: gorms.NewTran(),
	}
}
