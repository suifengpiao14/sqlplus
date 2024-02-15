package sqlplus

import (
	"strings"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/pkg/errors"
	"github.com/suifengpiao14/funcs"
	"github.com/suifengpiao14/sqlexec/sqlexecparser"
)

// WithPlusScene 扩展sql 的where 条件
func WithCheckUniqueue(database string, sqlStr string) (sqlSelects []UniqueueSelectSQL, err error) {
	if sqlStr == "" {
		return nil, errors.WithMessage(ERROR_SQL_EMPTY, funcs.GetCallFuncname(0))
	}
	stmt, err := sqlparser.Parse(sqlStr)
	if err != nil {
		return nil, err
	}
	switch stmt := stmt.(type) {
	case *sqlparser.Insert:
		sqlSelects, err = withInsertCheckUniqueue(database, stmt)
		if err != nil {
			return nil, err
		}

	}
	return sqlSelects, nil
}

type UniqueueSelectSQL struct {
	Sql   string       `json:"sql"`
	Where ColumnValues `json:"where"`
}

// withInsertCheckUniqueue
func withInsertCheckUniqueue(database string, stmt *sqlparser.Insert) (sqlSelects []UniqueueSelectSQL, err error) {

	// 获取 INSERT 语句的表名
	tableName := sqlparser.String(stmt.Table)
	table, err := sqlexecparser.GetTable(database, tableName)
	if err != nil {
		return nil, err
	}
	constraint, ok := table.Constraints.GetByType(sqlexecparser.Constraint_Type_Uniqueue)
	if !ok {
		return nil, nil
	}
	rowValues := stmt.Rows.(sqlparser.Values)
	rowColumnValues := make([]ColumnValues, 0)
	columnLen := len(stmt.Columns)

	for _, valTuple := range rowValues {
		columnValues := ColumnValues{}
		for _, colName := range constraint.ColumnNames {
			columnIndex := 0
			for columnIndex = 0; columnIndex < columnLen; columnIndex++ {
				col := stmt.Columns[columnIndex]
				if strings.EqualFold(sqlparser.String(col), colName) {
					sqlValue := valTuple[columnIndex].(*sqlparser.SQLVal)
					columnValue := ColumnValue{
						Column: sqlparser.String(col),
						Value:  sqlparser.String(sqlValue),
					}
					columnValues.AddIgnore(columnValue)
					break
				}
			}
			tableColumn, ok := table.Columns.GetByName(colName)
			if !ok {
				err = errors.Errorf("not found coulumn:%s from table:%s", colName, tableName)
				return nil, err
			}
			if columnIndex >= columnLen {
				defaultValue := tableColumn.DefaultValue

				columnValue := ColumnValue{
					Column: colName,
					Value:  defaultValue,
				}
				columnValues.AddIgnore(columnValue)
			}
		}
		rowColumnValues = append(rowColumnValues, columnValues)
	}
	sqlSelects = make([]UniqueueSelectSQL, 0)
	for _, columnValues := range rowColumnValues {
		selectSql := ConvertInsertToSelect(stmt, columnValues)
		uniqueueSelectSQL := UniqueueSelectSQL{
			Sql:   selectSql,
			Where: columnValues,
		}
		sqlSelects = append(sqlSelects, uniqueueSelectSQL)
	}

	return sqlSelects, nil
}
