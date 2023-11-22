package sqlplus_test

import (
	"fmt"
	"testing"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/stretchr/testify/require"
	"github.com/suifengpiao14/sqlplus"
)

func TestParse(t *testing.T) {

	var (
		err         error
		sqlStr      string
		newSqlStr   string
		tableColumn = sqlplus.TableColumn{
			TableName:    "user",
			Name:         "operater_id",
			Type:         sqlparser.StrVal,
			DynamicValue: "11",
		}
	)
	t.Run("select with where", func(t *testing.T) {
		sqlStr = "select * from user where id=1 or id=2;"
		newSqlStr, err = sqlplus.WithPlusWhereScene(sqlStr, tableColumn)
		require.NoError(t, err)
		fmt.Println(newSqlStr)
	})
	t.Run("select with as ", func(t *testing.T) {
		sqlStr = "select * from user as u where u.id=1 or u.id=2;"
		newSqlStr, err = sqlplus.WithPlusWhereScene(sqlStr, tableColumn)
		require.NoError(t, err)
		fmt.Println(newSqlStr)
	})
	t.Run("select no where", func(t *testing.T) {
		sqlStr = "select * from user;"
		newSqlStr, err = sqlplus.WithPlusWhereScene(sqlStr, tableColumn)
		require.NoError(t, err)
		fmt.Println(newSqlStr)
	})
	t.Run("select left join", func(t *testing.T) {
		sqlStr = "select * from user left join class on class.user_id=user.id where user.id=1;"
		newSqlStr, err = sqlplus.WithPlusWhereScene(sqlStr, tableColumn)
		require.NoError(t, err)
		fmt.Println(newSqlStr)
	})
	t.Run("insert", func(t *testing.T) {
		var err error
		sqlStr := `insert into user(id,name)values(1,"a"),(2,"b");`
		newSqlStr, err = sqlplus.WithPlusWhereScene(sqlStr, tableColumn)
		require.NoError(t, err)
		fmt.Println(newSqlStr)
	})
}
