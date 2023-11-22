package sqlpluspack

import (
	"context"
	"encoding/json"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/suifengpiao14/sqlplus"
	"github.com/suifengpiao14/stream"
)

var (
	operatorContextKey sqlplus.ContextKey = "operatorKey"
	OperatorJsonKey                       = "operator"
	OperatorColumn                        = NewOperatorColumn(
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

type Operator struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// GetOperatorFromContext 从上下文获取操作者
func GetOperatorFromContext(ctx context.Context) (operator *Operator, err error) {
	value, err := sqlplus.GetKeyValue(ctx, operatorContextKey)
	if err != nil {
		return nil, err
	}
	operator = &Operator{}
	err = json.Unmarshal([]byte(value), operator)
	if err != nil {
		return nil, err
	}
	return operator, nil
}

type GetOperatorValueFn func(ctx context.Context, key string, input []byte) (value Operator, err error)
type SetOperatorValueFn func(ctx context.Context, key string, value Operator, input []byte) (out []byte, err error)

// OperatorPackHandlerSetContent 从输入流中提取operatorId 到ctx中，在输出流中自动添加operatorId
func OperatorPackHandlerSetContent(getOperatorFn sqlplus.GetValueFn, setOperatorFn SetOperatorValueFn) (packHandler stream.PackHandler, err error) {
	setContext := sqlplus.SetContext{
		ContextKey: operatorContextKey,
		JsonKey:    OperatorJsonKey,
		GetFn: func(ctx context.Context, key string, input []byte) (value string, err error) {
			operator, err := getOperatorFn(ctx, key, input)
			if err != nil {
				return "", err
			}
			b, err := json.Marshal(operator)
			if err != nil {
				return "", err
			}
			value = string(b)
			return value, nil
		},
		SetFn: func(ctx context.Context, key, value string, input []byte) (out []byte, err error) {
			operator := &Operator{}
			err = json.Unmarshal([]byte(value), operator)
			if err != nil {
				return nil, err
			}
			out, err = setOperatorFn(ctx, key, *operator, input)
			if err != nil {
				return nil, err
			}
			return out, nil
		},
	}

	return sqlplus.SqlPlusPackHandlerSetContent(setContext)
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
func OperatorPackHandler(operator Operator) (packHandler stream.PackHandler) {
	tableColumns := make([]sqlplus.TableColumn, 0)
	if OperatorColumn.ID != nil {
		operatorIDtableColumn := OperatorColumn.ID
		operatorIDtableColumn.DynamicValue = operator.ID
		tableColumns = append(tableColumns, *operatorIDtableColumn)
	}
	if OperatorColumn.Name != nil {
		operatorNametableColumn := OperatorColumn.Name
		operatorNametableColumn.DynamicValue = operator.Name
		tableColumns = append(tableColumns, *operatorNametableColumn)
	}
	return sqlplus.CudPackHandler(tableColumns...)
}
