package sqlpluspack

import (
	"context"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/suifengpiao14/sqlplus"
	"github.com/suifengpiao14/stream"
)

var (
	tenantIDKey        sqlplus.ContextKey = "tenantIDKey"
	TenantColumnConfig                    = sqlplus.TableColumn{
		Name: "tenant_id",
		Type: sqlparser.StrVal,
	}
)

// TenantPackHandlerSetContent 从输入流中提取tenantId 到ctx中，在输出流中自动添加tenantId
func TenantPackHandlerSetContent(getTenantIDFn sqlplus.GetValueFn, setTenantIDFn sqlplus.SetValueFn) (packHandler stream.PackHandler, err error) {
	return sqlplus.SqlPlusPackHandlerSetContent(sqlplus.SetContext{
		Key:   tenantIDKey,
		GetFn: getTenantIDFn,
		SetFn: setTenantIDFn,
	})
}

var (
	Stream_Tenant_json_key = "tenantId" // 多住户json key
)

// GetTenantIDJsonFn 从json字符串中获取 tenantId
func GetTenantIDJsonFn(ctx context.Context, input []byte) (tendId string, err error) {
	return sqlplus.GetKeyValueJsonFn(ctx, Stream_Tenant_json_key, input)
}

// SetTenantIDJsonFn 将 tenantId 设置到json字符串中
func SetTenantIDJsonFn(ctx context.Context, input []byte, tenantId string) (out []byte, err error) {
	return sqlplus.SetKeyValueJsonFn(ctx, Stream_Tenant_json_key, tenantId, input)
}

// TenantPackHandler 柯里化多租户插件
func TenantPackHandler(tenantID string) (packHandler stream.PackHandler) {
	tableColumn := TenantColumnConfig
	tableColumn.DynamicValue = tenantID
	return sqlplus.CurdPackHandler(tableColumn)
}
