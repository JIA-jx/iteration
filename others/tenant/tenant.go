package tenant

import "context"

type Info struct {
	Id   int64
	Code string
	Name string
}

const DefaultId int64 = 1

const DefaultCode = "default"

type ctxKey struct{}

type scopeAllKey struct{}

func ScopeAll(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, scopeAllKey{}, true)
}

func IsScopeAll(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(scopeAllKey{}).(bool)
	return v
}

func WithContext(ctx context.Context, info Info) context.Context {
	if info.Id <= 0 {
		info.Id = DefaultId
	}
	if info.Code == "" {
		info.Code = DefaultCode
	}
	return context.WithValue(ctx, ctxKey{}, info)
}

func FromContext(ctx context.Context) (Info, bool) {
	if ctx == nil {
		return Info{}, false
	}
	v, ok := ctx.Value(ctxKey{}).(Info)
	if !ok {
		return Info{}, false
	}
	return v, true
}

func IdFromContext(ctx context.Context) int64 {
	if info, ok := FromContext(ctx); ok && info.Id > 0 {
		return info.Id
	}
	return DefaultId
}

func CodeFromContext(ctx context.Context) string {
	if info, ok := FromContext(ctx); ok && info.Code != "" {
		return info.Code
	}
	return DefaultCode
}

func IsValid(id int64) bool {
	return id > 0
}
