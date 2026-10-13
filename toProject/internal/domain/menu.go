package domain

import (
	"context"
	"test.com/others/errs"
	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/repo"
	"test.com/toProject/pkg/model"
	"time"
)

type MenuDomain struct {
	menuRepo repo.MenuRepo
}

func (d *MenuDomain) MenuTreeList(ctx context.Context) ([]*data.ProjectMenuChild, *errs.MError) {
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	menus, err := d.menuRepo.FindMenus(c)
	if err != nil {
		return nil, model.DBError
	}
	menuChildren := data.CovertChild(menus)
	return menuChildren, nil
}

func NewMenuDomain() *MenuDomain {
	return &MenuDomain{
		menuRepo: dao.NewMenuDao(),
	}
}
