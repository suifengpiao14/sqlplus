package sqlplus

import (
	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/pkg/errors"
)

var (
	Err_Unsupported_Table            = errors.New("Unsupported table type")
	Err_Unsupported_Table_SimpleExpr = errors.New("Unsupported table type simple expr")
)

func getTableName(tableNameStr string, tableExprs ...sqlparser.TableExpr) (tableName sqlparser.TableName, ok bool) {
	if tableNameStr == "" { // 查询的table name 为指明,并且有多个时,直接返回
		return tableName, true
	}

	for _, tableExpr := range tableExprs {
		switch tableExpr := tableExpr.(type) {
		case *sqlparser.JoinTableExpr:
			tableName, ok := getTableName(tableNameStr, tableExpr.LeftExpr, tableExpr.RightExpr)
			if ok {
				return tableName, true
			}
		case *sqlparser.AliasedTableExpr:
			if !tableExpr.As.IsEmpty() {
				tableName.Name = tableExpr.As
				return tableName, true
			}
			tableName, ok := getTablenameFromSimpleTableExpr(tableNameStr, tableExpr.Expr)
			if ok {
				return tableName, true
			}
		case *sqlparser.ParenTableExpr:
			tableName, ok := getTableName(tableNameStr, tableExpr.Exprs...)
			if ok {
				return tableName, true
			}
		case sqlparser.SimpleTableExpr:
			tableName, ok := getTablenameFromSimpleTableExpr(tableNameStr, tableExpr)
			if ok {
				return tableName, true
			}
		}
	}
	return tableName, false
}

func getTablenameFromSimpleTableExpr(tableNameStr string, simpleTableExpr sqlparser.SimpleTableExpr) (tableName sqlparser.TableName, ok bool) {
	switch simpleTableExpr := simpleTableExpr.(type) {
	case sqlparser.TableName:
		if simpleTableExpr.Name.String() == tableNameStr {
			return simpleTableExpr, true
		}
	}
	return tableName, false
}
