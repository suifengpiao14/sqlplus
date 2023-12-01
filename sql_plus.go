package sqlplus

import (
	"github.com/pkg/errors"
	"github.com/suifengpiao14/funcs"

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

// WithPlusScene 扩展sql 的where 条件
func WithPlusScene(sqlStr string, scenes Scenes, tableColumns ...TableColumn) (newSql string, err error) {
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
		if scenes.Exists(Scene_Select_Where) {
			stmt.Where = withPlusWhere(stmt.Where, stmt.From, tableColumns...)
		}
	case *sqlparser.Update:
		if scenes.Exists(Scene_Update_Where) {
			stmt.Where = withPlusWhere(stmt.Where, stmt.TableExprs, tableColumns...)
		}
		if scenes.Exists(Scene_Update_Column) {
			withUpdatePlusToColumns(stmt, stmt.TableExprs, tableColumns...)
		}
	case *sqlparser.Insert:
		if scenes.Exists(Scene_Insert_Column) {
			withInsertPlusToColumns(stmt, stmt.Table, tableColumns...)
		}
	case *sqlparser.Delete:
		if scenes.Exists(Scene_Delete_Where) {
			stmt.Where = withPlusWhere(stmt.Where, stmt.TableExprs, tableColumns...)
		}
	}
	newSql = sqlparser.String(stmt)
	return newSql, nil
}

type Scene string

type Scenes []Scene

func (ss Scenes) Exists(scene Scene) bool {
	for _, s := range ss {
		if s == scene {
			return true
		}
	}
	return false
}

const (
	Scene_Select_Where  Scene = "select_where"
	Scene_Update_Where  Scene = "update_where"
	Scene_Delete_Where  Scene = "delete_where"
	Scene_Update_Column Scene = "update_column"
	Scene_Insert_Column Scene = "insert_column"
)
