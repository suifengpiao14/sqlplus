package sqlplus_test

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/suifengpiao14/sqlexec/sqlexecparser"
	"github.com/suifengpiao14/sqlplus"
)

var database = "export"

func TestWithCheckUniqueue(t *testing.T) {
	sqlStr := `insert into export_template (template_name) values ("dispatch")`
	selectSqls, err := sqlplus.WithCheckUniqueue(database, sqlStr)
	require.NoError(t, err)
	b, err := json.Marshal(selectSqls)
	require.NoError(t, err)
	fmt.Println(string(b))

}

func init() {
	b, err := os.ReadFile("./test/exportservice.sql")
	if err != nil {
		panic(err)
	}
	ddl := string(b)
	err = sqlexecparser.RegisterTableByDDL(ddl)
	if err != nil {
		panic(err)
	}
}
