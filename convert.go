package sqlplus

import (
	"fmt"
	"strings"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/spf13/cast"
	"github.com/suifengpiao14/sqlexec/sqlexecparser"
)

func ConvertUpdateToSelect(stmt *sqlparser.Update) (selectSQL string) {
	// 将 UPDATE 语句中的 SET 子句转换为 SELECT 语句的字段列表
	// var selectFields []string
	// 缺少id
	// for _, expr := range stmt.Exprs {
	// 	selectFields = append(selectFields, sqlparser.String(expr.Name))
	// }
	tableName := sqlparser.String(stmt.TableExprs)
	//selectField := strings.Join(selectFields, ", ") //缺少Id，暂时用*代替
	where := sqlparser.String(stmt.Where)
	selectSQL = fmt.Sprintf("SELECT * FROM %s  %s", tableName, where)
	return selectSQL
}

func addBackticks(columnName string) (newName string) {
	newName = strings.TrimSpace(columnName)
	newName = strings.Trim(newName, "`")
	newName = fmt.Sprintf("`%s`", newName)
	return newName
}

// ConvertUpdateToInsert 将update 语句转为insert ,在模拟实现replace(set 场景)有用
func ConvertUpdateToInsert(stmt *sqlparser.Update) (insertSQL string) {
	tableName := sqlparser.String(stmt.TableExprs)
	columnValues := make(sqlexecparser.ColumnValues, 0)

	for _, expr := range stmt.Exprs {
		colName := expr.Name.Name.String()
		colValue := sqlparser.String(expr.Expr)
		columnValues.AddIgnore(sqlexecparser.ColumnValue{
			Column: sqlexecparser.ColumnName(colName),
			Value:  colValue,
		})
	}
	if stmt.Where != nil {
		whereColumnValues := ParseWhere(stmt.Where, "=")
		columnValues.AddIgnore(whereColumnValues...)
	}

	allColumns, allValues := columnValues.Array()
	allStrValues := make([]string, 0)
	for _, v := range allValues {
		allStrValues = append(allStrValues, cast.ToString(v))
	}
	insertSQL = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", tableName, strings.Join(allColumns.ToString(), ", "), strings.Join(allStrValues, ", "))
	return insertSQL
}

func ConvertDeleteToSelect(stmt *sqlparser.Delete) (selectSQL string) {
	// 获取 DELETE 语句的表名
	selectField := "*"
	tableName := sqlparser.String(stmt.TableExprs)
	where := sqlparser.String(stmt.Where)
	selectSQL = fmt.Sprintf("SELECT %s FROM %s WHERE %s", selectField, tableName, where)
	return selectSQL
}

func ConvertInsertToSelect(stmt *sqlparser.Insert, where sqlexecparser.ColumnValues) (selectSQL string) {
	// 获取 INSERT 语句的字段列表
	var selectFields []string
	for _, col := range stmt.Columns {
		selectFields = append(selectFields, sqlparser.String(col))
	}
	// 获取 INSERT 语句的表名
	tableName := sqlparser.String(stmt.Table)
	selectField := strings.Join(selectFields, ", ")

	selectSQL = fmt.Sprintf("SELECT %s FROM %s %s", selectField, tableName, sqlparser.String(where.WhereAndExpr()))
	return selectSQL
}
