package sqlpluspack

import (
	"context"
	"encoding/json"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/suifengpiao14/sqlplus"
	"github.com/suifengpiao14/stream"
)

var (
	tenantIDKey        stream.ContextKey = "tenantIDKey" //ctx 上下文中的key
	TenantJsonKey                        = "tenantId"    // json 数据中的key
	TenantColumnConfig                   = sqlplus.TableColumn{
		Name: "tenant_id",
		Type: sqlparser.StrVal,
	}
)

type _SetContextTenantPackHandler struct {
	stream.SetContextPacketHandler
}

// NewSetContextTenantPackHandler 从输入流中提取tenantId 到ctx中，在输出流中自动添加tenantId
func NewSetContextTenantPackHandler(getTenantIDFn stream.GetValueFn, setTenantIDFn stream.SetValueFn) (packHandler stream.PacketHandlerI) {
	setContext := stream.SetContext{
		ContextKey: tenantIDKey,
		JsonKey:    TenantJsonKey,
		GetFn:      getTenantIDFn,
		SetFn:      setTenantIDFn,
	}
	packetHandler := stream.NewSetContextPacketHandler(setContext)
	setContextPacketHandler := packetHandler.(stream.SetContextPacketHandler)
	return &_SetContextTenantPackHandler{
		SetContextPacketHandler: setContextPacketHandler,
	}
}

func (packet *_SetContextTenantPackHandler) Name() string {
	return stream.GeneratePacketHandlerName(packet)
}
func (packet *_SetContextTenantPackHandler) Description() string {
	return `设置多租户值到上下文`
}

type _TenantPacketHandler struct {
	TenantID string `json:"tenantID"`
	sqlplus.SqlPlusPacketHandler
}

func NewTenantPacketHandler(tenatID string) (packHandler stream.PacketHandlerI) {
	tableColumn := TenantColumnConfig
	tableColumn.DynamicValue = tenatID
	// 查询、更新条件、删除条件，新增 时增加租户条件
	scenes := sqlplus.Scenes{
		sqlplus.Scene_Select_Where,
		sqlplus.Scene_Update_Where,
		sqlplus.Scene_Delete_Where,
		sqlplus.Scene_Insert_Column,
	}
	basic := sqlplus.NewSqlPlusPacketHandler(scenes, tableColumn)
	sqlplusHandler := basic.(*sqlplus.SqlPlusPacketHandler)
	handler := &_TenantPacketHandler{
		TenantID:             tenatID,
		SqlPlusPacketHandler: *sqlplusHandler,
	}
	return handler
}

func (packet *_TenantPacketHandler) Name() string {
	return stream.GeneratePacketHandlerName(packet)
}

func (packet *_TenantPacketHandler) Description() string {
	return `在查询、修改、删除的条件中增加多租户条件，在新增字段中增加多租户条件`
}

func (packet *_TenantPacketHandler) String() string {
	b, _ := json.Marshal(packet)
	s := string(b)
	return s
}

// GetTenantIDFromContext 从上下文获取租户ID
func GetTenantIDFromContext(ctx context.Context) (tenantID string, err error) {
	return stream.GetKeyValue(ctx, tenantIDKey)
}
