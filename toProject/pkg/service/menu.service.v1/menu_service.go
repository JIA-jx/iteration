package menu_service_v1

import (
	"context"
	lru "github.com/hashicorp/golang-lru"
	"github.com/jinzhu/copier"
	"test.com/others/errs"
	"test.com/rpcOrgrpc/menu"
	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/database/tran"
	"test.com/toProject/internal/domain"
	"test.com/toProject/internal/repo"
	"test.com/toProject/pkg/service/menu.service.v1/lua"
)

var menuLRU, _ = lru.NewARC(10000)

type MenuService struct {
	menu.UnimplementedMenuServiceServer
	cache       repo.Cache
	transaction tran.Transaction
	menuDomain  *domain.MenuDomain
}

func New() *MenuService {
	return &MenuService{
		cache:       dao.Rc,
		transaction: dao.NewTransaction(),
		menuDomain:  domain.NewMenuDomain(),
	}
}

func (d *MenuService) MenuList(ctx context.Context, msg *menu.MenuReqMessage) (*menu.MenuResponseMessage, error) {
	cacheKey := "menu:tree:list"
	if val, ok := menuLRU.Get(cacheKey); ok {
		if resp, ok := val.(*menu.MenuResponseMessage); ok {
			return resp, nil
		}
	}

	list, err := d.menuDomain.MenuTreeList(ctx)
	if err != nil {
		return nil, errs.GrpcError(err)
	}

	luaList, luaErr := lua.NewLuaEngine().FilterMenu(ctx, list)
	if luaErr != nil {
		return nil, errs.GrpcError(errs.NewError(errs.ErrorCode(500), luaErr.Error()))
	}

	var mList []*menu.MenuMessage
	copier.Copy(&mList, luaList)

	resp := &menu.MenuResponseMessage{List: mList}
	menuLRU.Add(cacheKey, resp)

	return resp, nil
}
