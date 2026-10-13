package dao

import (
	"test.com/toUser/internal/database"
	"test.com/toUser/internal/database/gorms"
)

type TransactionImpl struct {
	conn database.DbConn
}

func (t *TransactionImpl) Action(f func(conn database.DbConn) error) (err error) {
	conn := gorms.NewTran()
	conn.Begin()
	defer func() {
		if r := recover(); r != nil {
			conn.Rollback()
			panic(r)
		}
	}()
	if err = f(conn); err != nil {
		conn.Rollback()
		return err
	}
	if err = conn.Commit(); err != nil {
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
