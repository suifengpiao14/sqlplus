package sqlplus

import (
	"context"

	"github.com/pkg/errors"
	"github.com/suifengpiao14/funcs"
	"github.com/suifengpiao14/stream"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
)

type TableColumn struct {
	TableName    string            `json:"tableName"`
	Name         string            `json:"name"`
	Type         sqlparser.ValType `json:"type"`
	DynamicValue string            `json:"value"`
}

var (
	Err_Unsupported_statement = errors.New("Unsupported statement type detected")
)

// withPlusWhere 增强条件
func withPlusWhere(where *sqlparser.Where, tableExprs sqlparser.TableExprs, tableColumns ...TableColumn) (newWhere *sqlparser.Where) {
	for _, tableColumn := range tableColumns {

		// 构建 WHERE 子句的左侧（字段名）
		colIdent := sqlparser.NewColIdent(tableColumn.Name)
		colName := &sqlparser.ColName{Name: colIdent}
		tableName, ok := getTableName(tableColumn.TableName, tableExprs...)
		if ok {
			colName.Qualifier = tableName
		}

		// 构建 WHERE 子句的右侧（值）
		valExpr := &sqlparser.SQLVal{Type: tableColumn.Type, Val: []byte(tableColumn.DynamicValue)}

		// 构建 WHERE 子句的条件表达式
		plusExpr := &sqlparser.ComparisonExpr{
			Operator: sqlparser.EqualStr,
			Left:     colName,
			Right:    valExpr,
		}
		if where == nil {
			where = sqlparser.NewWhere(sqlparser.WhereStr, plusExpr)
			continue
		}
		newExpr := &sqlparser.AndExpr{
			Left:  &sqlparser.ParenExpr{Expr: where.Expr},
			Right: plusExpr,
		}
		where = sqlparser.NewWhere(where.Type, newExpr)
	}

	return where
}

// withInsertPlusToColumns 新增sql中增加列
func withInsertPlusToColumns(insertExpr *sqlparser.Insert, tableName sqlparser.TableName, tableColumns ...TableColumn) {
	for _, tableColumn := range tableColumns {
		colIdent := sqlparser.NewColIdent(tableColumn.Name)
		insertExpr.Columns = append(insertExpr.Columns, colIdent)
		valExpr := &sqlparser.SQLVal{Type: tableColumn.Type, Val: []byte(tableColumn.DynamicValue)}
		for i := range insertExpr.Rows.(sqlparser.Values) {
			insertExpr.Rows.(sqlparser.Values)[i] = append(insertExpr.Rows.(sqlparser.Values)[i], valExpr)
		}
	}
}

// withUpdatePlusToColumns 修改sql中增加列
func withUpdatePlusToColumns(updateExpr *sqlparser.Update, tableExprs sqlparser.TableExprs, tableColumns ...TableColumn) {
	for _, tableColumn := range tableColumns {
		colIdent := sqlparser.NewColIdent(tableColumn.Name)
		column := &sqlparser.ColName{Name: colIdent}
		exits := false
		for _, expr := range updateExpr.Exprs {
			if expr.Name.Equal(column) {
				exits = true
				break
			}
		}
		if exits {
			continue
		}
		// 创建一个新的列更新，并添加到 UPDATE 语句中
		valExpr := &sqlparser.SQLVal{Type: tableColumn.Type, Val: []byte(tableColumn.DynamicValue)}
		assignment := &sqlparser.UpdateExpr{Name: column, Expr: valExpr}
		updateExpr.Exprs = append(updateExpr.Exprs, assignment)
	}
}

var (
	ERROR_SQL_EMPTY = errors.New("empty sql")
)

// WithPlusWhereScene 扩展sql 的where 条件
func WithPlusWhereScene(sqlStr string, tableColumns ...TableColumn) (newSql string, err error) {
	newSql = sqlStr // 设置默认值
	if sqlStr == "" {
		return "", errors.WithMessage(ERROR_SQL_EMPTY, funcs.GetCallFuncname(0))
	}
	stmt, err := sqlparser.Parse(sqlStr)
	if err != nil {
		return "", err
	}
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		stmt.Where = withPlusWhere(stmt.Where, stmt.From, tableColumns...)
		newSql = sqlparser.String(stmt)
		return newSql, nil
	case *sqlparser.Update:
		stmt.Where = withPlusWhere(stmt.Where, stmt.TableExprs, tableColumns...)
		newSql := sqlparser.String(stmt)
		return newSql, nil
	case *sqlparser.Delete:
		stmt.Where = withPlusWhere(stmt.Where, stmt.TableExprs, tableColumns...)
		newSql := sqlparser.String(stmt)
		return newSql, nil

	}
	return newSql, nil
}

// WithPlusColumnScene 扩展sql 新增、修改、删除时变更的字段
func WithPlusColumnScene(sqlStr string, tableColumns ...TableColumn) (newSql string, err error) {
	newSql = sqlStr
	if sqlStr == "" {
		return "", errors.WithMessage(ERROR_SQL_EMPTY, funcs.GetCallFuncname(0))
	}
	stmt, err := sqlparser.Parse(sqlStr)
	if err != nil {
		return "", err
	}
	switch stmt := stmt.(type) {
	case *sqlparser.Insert:
		withInsertPlusToColumns(stmt, stmt.Table, tableColumns...)
		newSql := sqlparser.String(stmt)
		return newSql, nil
	case *sqlparser.Update:
		withUpdatePlusToColumns(stmt, stmt.TableExprs, tableColumns...)
		newSql := sqlparser.String(stmt)
		return newSql, nil
	}
	// 其它类型不处理
	return newSql, nil
}

// AddWherePackHandler 柯里化增删改查sql插件(如多租户场景)
func AddWherePackHandler(tableColumns ...TableColumn) (packHandler stream.PackHandler) {
	packHandler = stream.NewPackHandler(func(ctx context.Context, input []byte) (out []byte, err error) {
		sql := string(input)
		newSql, err := WithPlusWhereScene(sql, tableColumns...)
		if err != nil {
			return nil, err
		}
		out = []byte(newSql)
		return out, nil
	}, nil)
	return packHandler
}

// AddColumnPackHandler 柯里化增改删sql插件,排除查询(如记录操作人场景)
func AddColumnPackHandler(tableColumns ...TableColumn) (packHandler stream.PackHandler) {
	packHandler = stream.NewPackHandler(func(ctx context.Context, input []byte) (out []byte, err error) {
		sql := string(input)
		newSql, err := WithPlusColumnScene(sql, tableColumns...)
		if err != nil {
			return nil, err
		}
		out = []byte(newSql)
		return out, nil
	}, nil)
	return packHandler
}
