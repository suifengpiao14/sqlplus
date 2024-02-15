package sqlplus

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
)

type ColumnValue struct {
	Column string `json:"column"`
	Value  string `json:"value"`
}

type ColumnValues []ColumnValue

func (cvs *ColumnValues) Array() (columns []string, values []string) {
	columns = make([]string, 0)
	values = make([]string, 0)
	for _, cv := range *cvs {
		columns = append(columns, cv.Column)
		values = append(values, cv.Value)
	}
	return columns, values
}
func (cvs *ColumnValues) AddIgnore(columnValues ...ColumnValue) {
	for _, columnValue := range columnValues {
		columnValue.Column = addBackticks(columnValue.Column)
		_, ok := cvs.GetByColumn(columnValue.Column)
		if ok {
			continue
		}
		*cvs = append(*cvs, columnValue)
	}
}

func (c ColumnValues) GetByColumn(column string) (col *ColumnValue, ok bool) {
	for _, columnValue := range c {
		if columnValue.Column == column {
			return &columnValue, true
		}
	}
	return nil, false
}

func (c ColumnValues) String() (str string) {
	var w bytes.Buffer
	for i, kv := range c {
		if i > 0 {
			w.WriteString(" and ")
		}
		w.WriteString(fmt.Sprintf("`%s`=%s", strings.Trim(kv.Column, "`"), kv.Value))
	}
	return ""
}

func ParseWhere(whereExpr *sqlparser.Where, operator string) (columnValues ColumnValues) {
	columnValues = make(ColumnValues, 0)
	whereExpr.WalkSubtree(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch expr := node.(type) {
		case *sqlparser.ComparisonExpr:
			if operator != "" && expr.Operator == operator {
				whereCol := sqlparser.String(expr.Left)
				whereVal := sqlparser.String(expr.Right)
				columnValues.AddIgnore(ColumnValue{
					Column: whereCol,
					Value:  whereVal,
				})
			}
		}
		return true, nil
	})
	return columnValues
}
