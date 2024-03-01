package sqlplus

import (
	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/suifengpiao14/sqlexec/sqlexecparser"
)

func ParseWhere(whereExpr *sqlparser.Where, operator string) (columnValues sqlexecparser.ColumnValues) {
	columnValues = make(sqlexecparser.ColumnValues, 0)
	whereExpr.WalkSubtree(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch expr := node.(type) {
		case *sqlparser.ComparisonExpr:
			if operator != "" && expr.Operator == operator {
				whereCol := sqlparser.String(expr.Left)
				whereVal := sqlparser.String(expr.Right)
				columnValues.AddIgnore(sqlexecparser.ColumnValue{
					Operator: operator,
					Column:   sqlexecparser.ColumnName(whereCol),
					Value:    whereVal,
				})
			}
		}
		return true, nil
	})
	return columnValues
}
