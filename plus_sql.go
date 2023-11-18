package sqlplus

import (
	"context"

	"github.com/pkg/errors"
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

// withPlusToColumns 将多租户字段添加到列中
func withPlusToColumns(insertExpr *sqlparser.Insert, tableName sqlparser.TableName, tableColumns ...TableColumn) {
	for _, tableColumn := range tableColumns {
		colIdent := sqlparser.NewColIdent(tableColumn.Name)
		insertExpr.Columns = append(insertExpr.Columns, colIdent)
		valExpr := &sqlparser.SQLVal{Type: tableColumn.Type, Val: []byte(tableColumn.DynamicValue)}
		for i := range insertExpr.Rows.(sqlparser.Values) {
			insertExpr.Rows.(sqlparser.Values)[i] = append(insertExpr.Rows.(sqlparser.Values)[i], valExpr)
		}
	}

}

// WithPlusCurdScene 对增删改查sql 扩展数据
func WithPlusCurdScene(sqlStr string, tableColumns ...TableColumn) (newSqlStr string, err error) {
	stmt, err := sqlparser.Parse(sqlStr)
	if err != nil {
		return "", err
	}
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		stmt.Where = withPlusWhere(stmt.Where, stmt.From, tableColumns...)
		newSql := sqlparser.String(stmt)
		return newSql, nil
	}
	return WithPlusCudScene(sqlStr, tableColumns...)
}

// WithPlusCudScene 对增删改命令sql 扩展数据,排除查询
func WithPlusCudScene(sqlStr string, tableColumns ...TableColumn) (newSqlStr string, err error) {
	stmt, err := sqlparser.Parse(sqlStr)
	if err != nil {
		return "", err
	}
	switch stmt := stmt.(type) {
	case *sqlparser.Insert:

		withPlusToColumns(stmt, stmt.Table, tableColumns...)
		newSql := sqlparser.String(stmt)
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
	err = Err_Unsupported_statement
	return "", err
}

// CurdPackHandler 柯里化增删改查sql插件(如多租户场景)
func CurdPackHandler(tableColumns ...TableColumn) (packHandler stream.PackHandler) {
	packHandler = stream.NewPackHandler(func(ctx context.Context, input []byte) (out []byte, err error) {
		sql := string(input)
		newSql, err := WithPlusCurdScene(sql, tableColumns...)
		if err != nil {
			return nil, err
		}
		out = []byte(newSql)
		return out, nil
	}, nil)
	return packHandler
}

// CudPackHandler 柯里化增改删sql插件,排除查询(如记录操作人场景)
func CudPackHandler(tableColumns ...TableColumn) (packHandler stream.PackHandler) {
	packHandler = stream.NewPackHandler(func(ctx context.Context, input []byte) (out []byte, err error) {
		sql := string(input)
		newSql, err := WithPlusCudScene(sql, tableColumns...)
		if err != nil {
			return nil, err
		}
		out = []byte(newSql)
		return out, nil
	}, nil)
	return packHandler
}
