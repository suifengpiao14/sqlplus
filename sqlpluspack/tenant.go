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
	return sqlplus.CurdPackHandler(tableColumn)
}

// GetTenantIDFromContext 从上下文或缺租户ID
func GetTenantIDFromContext(ctx context.Context) (tenantID string, err error) {
	return sqlplus.GetKeyValue(ctx, tenantIDKey)
}
