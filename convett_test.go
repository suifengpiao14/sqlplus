package sqlplus_test

import (
	"fmt"
	"testing"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/stretchr/testify/require"
	"github.com/suifengpiao14/sqlplus"
)

func TestConvertUpdateToInsert(t *testing.T) {
	t.Run("set with not include where", func(t *testing.T) {
		updateSql := "update user set age=5,sex=1 where name='张三' and deleted_at is null;"
		stmt, err := sqlparser.Parse(updateSql)
		require.NoError(t, err)
		updateStmt := stmt.(*sqlparser.Update)
		insertSQL := sqlplus.ConvertUpdateToInsert(updateStmt)
		fmt.Println(insertSQL)
	})

	t.Run("set with where", func(t *testing.T) {
		updateSql := "update `service` set `name`='advertise1',`title`='广告服务',`document`='文档地址' where `name`='advertise1' and `deleted_at` is null;"
		stmt, err := sqlparser.Parse(updateSql)
		require.NoError(t, err)
		updateStmt := stmt.(*sqlparser.Update)
		insertSQL := sqlplus.ConvertUpdateToInsert(updateStmt)
		fmt.Println(insertSQL)
	})
}
