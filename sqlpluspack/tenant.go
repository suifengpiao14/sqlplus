package sqlpluspack

import (
	"context"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/suifengpiao14/sqlplus"
	"github.com/suifengpiao14/stream"
)

var (
	tenantIDKey        sqlplus.ContextKey = "tenantIDKey" //ctx 上下文中的key
	TenantJsonKey                         = "tenantId"    // json 数据中的key
	TenantColumnConfig                    = sqlplus.TableColumn{
		Name: "tenant_id",
		Type: sqlparser.StrVal,
	}
)

// TenantPackHandlerSetContent 从输入流中提取tenantId 到ctx中，在输出流中自动添加tenantId
func TenantPackHandlerSetContent(getTenantIDFn sqlplus.GetValueFn, setTenantIDFn sqlplus.SetValueFn) (packHandler stream.PackHandler, err error) {
	return sqlplus.SqlPlusPackHandlerSetContent(sqlplus.SetContext{
		ContextKey: tenantIDKey,
		JsonKey:    TenantJsonKey,
		GetFn:      getTenantIDFn,
		SetFn:      setTenantIDFn,
	})
}

// TenantPackHandler 柯里化多租户插件
func TenantPackHandler(tenantID string) (packHandler stream.PackHandler) {
	tableColumn := TenantColumnConfig
	tableColumn.DynamicValue = tenantID
	// 查询、更新条件、删除条件，新增 时增加租户条件
	scenes := sqlplus.Scenes{
		sqlplus.Scene_Select_Where,
		sqlplus.Scene_Update_Where,
		sqlplus.Scene_Delete_Where,
		sqlplus.Scene_Insert_Column,
	}
	return sqlplus.PlusPackHandler(scenes, tableColumn)
}

// GetTenantIDFromContext 从上下文获取租户ID
func GetTenantIDFromContext(ctx context.Context) (tenantID string, err error) {
	return sqlplus.GetKeyValue(ctx, tenantIDKey)
}
