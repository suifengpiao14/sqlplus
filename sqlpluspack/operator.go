package sqlpluspack

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/suifengpiao14/sqlplus"
	"github.com/suifengpiao14/stream"
)

var (
	operatorContextKey stream.ContextKey = "operatorContextKey"
	OperatorColumn                       = NewOperatorColumn(
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
	ID   *string `json:"operatorId"`
	Name *string `json:"operatorName"`
}

// GetOperatorFromContext 从上下文获取操作者
func GetOperatorFromContext(ctx context.Context) (operator *Operator, err error) {
	value, err := stream.GetKeyValue(ctx, operatorContextKey)
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

type GetOperatorValueFn func(ctx context.Context, input []byte) (value *Operator, err error)
type SetOperatorValueFn func(ctx context.Context, value Operator, input []byte) (out []byte, err error)

// GetOperatorJsonFn 从json字符串中获取 operator
func GetOperatorJsonFn(ctx context.Context, input []byte) (operator *Operator, err error) {
	operator = &Operator{}
	err = json.Unmarshal(input, operator)
	if err != nil {
		return nil, err
	}
	if OperatorColumn.ID != nil && operator.ID == nil {
		err = errors.New("opreatorId required")
		return nil, err
	}
	if OperatorColumn.Name != nil && operator.Name == nil {
		err = errors.New("opreatorName required")
		return nil, err
	}
	return operator, nil
}

// SetOperatorJsonFn 将 operator 设置到json字符串中
func SetOperatorJsonFn(ctx context.Context, operator Operator, input []byte) (out []byte, err error) {
	b, err := json.Marshal(operator)
	if err != nil {
		return nil, err
	}
	out, err = jsonpatch.MergePatch(input, b)
	if err != nil {
		return nil, err
	}
	return out, nil
}

type SetContextOperatorPackHandler struct {
	stream.SetContextPacketHandler
}

// OperatorPackHandlerSetContent 从输入流中提取operatorId 到ctx中，在输出流中自动添加operatorId
func NewSetContextOperatorPackHandler(getOperatorFn GetOperatorValueFn, setOperatorFn SetOperatorValueFn) (packHandler stream.PacketHandlerI) {
	setContext := stream.SetContext{
		ContextKey: operatorContextKey,
		JsonKey:    "",
		GetFn: func(ctx context.Context, key string, input []byte) (value string, err error) {
			if getOperatorFn == nil {
				return "", nil
			}
			operator, err := getOperatorFn(ctx, input)
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
		SetFn: func(ctx context.Context, key string, value string, input []byte) (out []byte, err error) {
			if setOperatorFn == nil {
				return input, err
			}
			operator := &Operator{}
			err = json.Unmarshal([]byte(value), operator)
			if err != nil {
				return nil, err
			}
			out, err = setOperatorFn(ctx, *operator, input)
			if err != nil {
				return nil, err
			}
			return out, nil
		},
	}

	basic := stream.NewSetContextPacketHandler(setContext)
	setContextPacket := basic.(stream.SetContextPacketHandler)
	return &SetContextOperatorPackHandler{
		SetContextPacketHandler: setContextPacket,
	}
}

// NewSetContentOperatorPackHandlerSetContent 从输入流中提取operatorId 到ctx中，在输出流中自动添加operatorId
func NewSetContentOperatorPackHandlerSetContent(getOperatorFn GetOperatorValueFn, setOperatorFn SetOperatorValueFn) (packHandler stream.PacketHandlerI) {
	setContext := stream.SetContext{
		ContextKey: operatorContextKey,
		JsonKey:    "",
		GetFn: func(ctx context.Context, key string, input []byte) (value string, err error) {
			if getOperatorFn == nil {
				return "", nil
			}
			operator, err := getOperatorFn(ctx, input)
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
		SetFn: func(ctx context.Context, key string, value string, input []byte) (out []byte, err error) {
			if setOperatorFn == nil {
				return input, err
			}
			operator := &Operator{}
			err = json.Unmarshal([]byte(value), operator)
			if err != nil {
				return nil, err
			}
			out, err = setOperatorFn(ctx, *operator, input)
			if err != nil {
				return nil, err
			}
			return out, nil
		},
	}

	return stream.NewSetContextPacketHandler(setContext)
}

type OperatorPackHandler struct {
	Operator Operator `json:"operator"`
	sqlplus.SqlPlusPacketHandler
}

func (packet *OperatorPackHandler) Name() string {
	return stream.GeneratePacketHandlerName(packet)
}

func (packet *OperatorPackHandler) Description() string {
	return `在新增、修改、删除操作的sql中增加操作人`
}

func (packet *OperatorPackHandler) String() string {
	b, _ := json.Marshal(packet)
	s := string(b)
	return s
}

// OperatorPackHandler 柯里化操作人组件
func NewOperatorPackHandler(operator Operator) (packHandler stream.PacketHandlerI) {
	tableColumns := make([]sqlplus.TableColumn, 0)
	if OperatorColumn.ID != nil {
		operatorIDtableColumn := OperatorColumn.ID
		if operator.ID != nil {
			operatorIDtableColumn.DynamicValue = *operator.ID
		}
		tableColumns = append(tableColumns, *operatorIDtableColumn)
	}
	if OperatorColumn.Name != nil {
		operatorNametableColumn := OperatorColumn.Name
		if operator.Name != nil {
			operatorNametableColumn.DynamicValue = *operator.Name
		}
		tableColumns = append(tableColumns, *operatorNametableColumn)
	}
	// 新增，修改时增加操作人
	scenes := sqlplus.Scenes{
		sqlplus.Scene_Insert_Column,
		sqlplus.Scene_Update_Column,
	}
	handler := sqlplus.NewSqlPlusPacketHandler(scenes, tableColumns...)
	sqlPlusPacket := handler.(*sqlplus.SqlPlusPacketHandler)
	return &OperatorPackHandler{
		Operator:             operator,
		SqlPlusPacketHandler: *sqlPlusPacket,
	}
}
