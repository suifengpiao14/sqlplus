package sqlplus_test

import (
	"fmt"
	"testing"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/stretchr/testify/require"
	"github.com/suifengpiao14/sqlplus"
)

func TestW(t *testing.T) {
	updateSql := "update user set age=5,sex=1 where name='张三' and deleted_at is null;"
	stmt, err := sqlparser.Parse(updateSql)
	require.NoError(t, err)
	updateStmt := stmt.(*sqlparser.Update)
	columns := sqlplus.ParseWhere(updateStmt.Where, "=")
	fmt.Println(columns)
}
