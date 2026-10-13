package errs

import "fmt"

type ErrorCode int

type MError struct {
	Code ErrorCode
	Msg  string
}

func (e *MError) Error() string {
	return fmt.Sprintf("code:%v,msg:%s", e.Code, e.Msg)
}

func NewError(code ErrorCode, msg string) *MError {
	return &MError{
		Code: code,
		Msg:  msg,
	}
}
