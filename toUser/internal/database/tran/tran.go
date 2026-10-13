package tran

import "test.com/toUser/internal/database"

type Transaction interface {
	Action(func(conn database.DbConn) error) error
}
