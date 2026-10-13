package lua

import (
	"context"
	"github.com/yuin/gopher-lua"
	"test.com/toProject/internal/data"
)

type LuaEngine struct{}

func NewLuaEngine() *LuaEngine {
	return &LuaEngine{}
}

func (l *LuaEngine) FilterMenu(ctx context.Context, list []*data.ProjectMenuChild) ([]*data.ProjectMenuChild, error) {
	ls := lua.NewState()
	defer ls.Close()

	script := `
	-- 动态菜单逻辑：隐藏、权限、排序
	function filter(menus)
		return menus
	end
	`
	if err := ls.DoString(script); err != nil {
		return list, err
	}

	return list, nil
}
