package sqlplus

import (
	"github.com/blastrain/vitess-sqlparser/sqlparser"
)

type ColumnValue struct {
	Column string
	Value  string
}

func ParseWhere(whereExpr *sqlparser.Where, operator string) (columnValues []ColumnValue) {
	columnValues = make([]ColumnValue, 0)
	whereExpr.WalkSubtree(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch expr := node.(type) {
		case *sqlparser.ComparisonExpr:
			if operator != "" && expr.Operator == operator {
				whereCol := sqlparser.String(expr.Left)
				whereVal := sqlparser.String(expr.Right)
				columnValues = append(columnValues, ColumnValue{
					Column: whereCol,
					Value:  whereVal,
				})
			}
		}
		return true, nil
	})
	return columnValues
}
