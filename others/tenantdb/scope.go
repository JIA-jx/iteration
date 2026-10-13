package tenantdb

import (
	"context"
	"reflect"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"test.com/others/tenant"
)

var scopedTables = map[string]struct{}{
	"iteration":                      {},
	"iteration_auth":                 {},
	"iteration_auth_node":            {},
	"iteration_collection":           {},
	"iteration_department":           {},
	"iteration_file":                 {},
	"iteration_log":                  {},
	"iteration_member":               {},
	"iteration_member_account":       {},
	"iteration_member_task_stat":     {},
	"iteration_menu":                 {},
	"iteration_minio_file":           {},
	"iteration_node":                 {},
	"iteration_organization":         {},
	"iteration_project_member":       {},
	"iteration_source_link":          {},
	"iteration_task":                 {},
	"iteration_task_member":          {},
	"iteration_task_stages":          {},
	"iteration_task_tag":             {},
	"iteration_task_stages_template": {},
	"iteration_task_work_time":       {},
	"iteration_template":             {},
	"task_dependency":                {},
}

func Scoped(table string) bool {
	_, ok := scopedTables[table]
	return ok
}

func Register(db *gorm.DB) error {
	if db == nil {
		return nil
	}
	if err := db.Callback().Query().Before("gorm:query").Register("tenant:query", filterCallback); err != nil {
		return err
	}
	if err := db.Callback().Update().Before("gorm:update").Register("tenant:update", filterCallback); err != nil {
		return err
	}
	if err := db.Callback().Delete().Before("gorm:delete").Register("tenant:delete", filterCallback); err != nil {
		return err
	}
	return db.Callback().Create().Before("gorm:create").Register("tenant:create", createCallback)
}

func of(ctx context.Context) int64 {
	if ctx == nil {
		return tenant.DefaultId
	}
	return tenant.IdFromContext(ctx)
}

func scoped(db *gorm.DB) bool {
	stmt := db.Statement
	if stmt == nil || stmt.Context == nil || stmt.Schema == nil {
		return false
	}
	if stmt.SQL.Len() > 0 || stmt.Table == "" {
		return false
	}
	return Scoped(stmt.Table)
}

func filterCallback(db *gorm.DB) {
	if !scoped(db) {
		return
	}
	stmt := db.Statement
	if tenant.IsScopeAll(stmt.Context) {
		return
	}
	stmt.AddClause(clause.Where{Exprs: []clause.Expression{
		clause.Eq{
			Column: clause.Column{Table: stmt.Table, Name: "tenant_id"},
			Value:  of(stmt.Context),
		},
	}})
}

func createCallback(db *gorm.DB) {
	if !scoped(db) {
		return
	}
	if tenant.IsScopeAll(db.Statement.Context) {
		return
	}
	setField(db.Statement.Dest, of(db.Statement.Context))
}

func setField(dest interface{}, id int64) {
	if dest == nil {
		return
	}
	rv := reflect.ValueOf(dest)
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return
		}
		rv = rv.Elem()
	}

	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < rv.Len(); i++ {
			setStructField(rv.Index(i), id)
		}
	default:
		setStructField(rv, id)
	}
}

func setStructField(v reflect.Value, id int64) {
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return
	}

	f := v.FieldByName("TenantId")
	if !f.IsValid() || !f.CanSet() || f.Kind() != reflect.Int64 {
		return
	}
	if f.Int() == 0 {
		f.SetInt(id)
	}
}
