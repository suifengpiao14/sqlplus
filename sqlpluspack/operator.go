package sqlpluspack

import (
	"context"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/suifengpiao14/sqlplus"
	"github.com/suifengpiao14/stream"
)

var (
	operatorIDKey   sqlplus.ContextKey = "operatorIDKey"
	operatorNameKey sqlplus.ContextKey = "operatorNameKey"
	OperatorColumn                     = NewOperatorColumn(
		&sqlplus.TableColumn{
			Name: "operator_id",
			Type: sqlparser.StrVal,
		},
		&sqlplus.TableColumn{
			Name: "operator_name",
			Type: sqlparser.StrVal,
		},
	)
)

type _OperatorColumn struct {
	ID   *sqlplus.TableColumn `json:"id"`
	Name *sqlplus.TableColumn `json:"name"`
}

func NewOperatorColumn(id *sqlplus.TableColumn, name *sqlplus.TableColumn) _OperatorColumn {
	return _OperatorColumn{
		ID:   id,
		Name: name,
	}
}

// OperatorPackHandlerSetContent 从输入流中提取operatorId 到ctx中，在输出流中自动添加operatorId
func OperatorPackHandlerSetContent(getOperatorFn sqlplus.GetValueFn, setOperatorFn sqlplus.SetValueFn) (packHandler stream.PackHandler, err error) {
	setContexts := make([]sqlplus.SetContext, 0)
	if OperatorColumn.ID != nil {
		setContexts = append(setContexts, sqlplus.SetContext{
			Key:   operatorIDKey,
			GetFn: getOperatorFn,
			SetFn: setOperatorFn,
		})
	}
	if OperatorColumn.Name != nil {
		setContexts = append(setContexts, sqlplus.SetContext{
			Key:   operatorNameKey,
			GetFn: getOperatorFn,
			SetFn: setOperatorFn,
		})
	}
	return sqlplus.SqlPlusPackHandlerSetContent(setContexts...)
}

var (
	Stream_Operator_json_key = "operatorId" // 多住户json key
)

// GetOperatorIDJsonFn 从json字符串中获取 operatorId
func GetOperatorIDJsonFn(ctx context.Context, input []byte) (tendId string, err error) {
	return sqlplus.GetKeyValueJsonFn(ctx, Stream_Operator_json_key, input)
}

// SetOperatorIDJsonFn 将 operatorId 设置到json字符串中
func SetOperatorIDJsonFn(ctx context.Context, input []byte, operatorId string) (out []byte, err error) {
	return sqlplus.SetKeyValueJsonFn(ctx, Stream_Operator_json_key, operatorId, input)
}

// OperatorPackHandler 柯里化操作人组件
func OperatorPackHandler(operatorID string, operatorName string) (packHandler stream.PackHandler) {
	tableColumns := make([]sqlplus.TableColumn, 0)
	if OperatorColumn.ID != nil {
		operatorIDtableColumn := OperatorColumn.ID
		operatorIDtableColumn.DynamicValue = operatorID
		tableColumns = append(tableColumns, *operatorIDtableColumn)
	}
	if OperatorColumn.Name != nil {
		operatorNametableColumn := OperatorColumn.Name
		operatorNametableColumn.DynamicValue = operatorName
		tableColumns = append(tableColumns, *operatorNametableColumn)
	}

	return sqlplus.CudPackHandler(tableColumns...)
}
