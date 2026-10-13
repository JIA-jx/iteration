package domain

import (
	"context"
	"test.com/rpcOrgrpc/user/login"
	"test.com/toProject/internal/rpc"
	"time"
)

type UserRpcDomain struct {
	lc login.LoginServiceClient
}

func NewUserRpcDomain() *UserRpcDomain {
	return &UserRpcDomain{
		lc: rpc.LoginServiceClient,
	}
}

func (d *UserRpcDomain) MemberList(ctx context.Context, mIdList []int64) ([]*login.MemberMessage, map[int64]*login.MemberMessage, error) {
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	messageList, err := d.lc.FindMemInfoByIds(c, &login.UserMessage{MIds: mIdList})
	mMap := make(map[int64]*login.MemberMessage)
	if err != nil || messageList == nil {
		return nil, mMap, err
	}
	for _, v := range messageList.List {
		if v == nil {
			continue
		}
		mMap[v.Id] = v
	}
	return messageList.List, mMap, err
}

func (d *UserRpcDomain) MemberInfo(ctx context.Context, memberCode int64) (*login.MemberMessage, error) {
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	memberMessage, err := d.lc.FindMemInfoById(c, &login.UserMessage{MemId: memberCode})
	return memberMessage, err
}
