package quiteago

import (
	"fmt"
	"testing"

	"github.com/lertrel/godb"
)

func TestParsingJsonToResultSet(t *testing.T) {

	str := `
{
    "results": [
        {
            "columns": [
                "id",
                "name",
		"age"
            ],
            "types": [
                "integer",
                "text",
                "integer"
            ],
            "values": [
                [
                    1,
                    "fiona",
                    20
                ],
                [
                    2,
                    "shriek",
                    30
                ]
            ],
            "time": 0.0150043
        }
    ],
    "time": 0.0220043
}
`

	resultSets := RqliteJsonResultSets{}

	rs, err := resultSets.GetResultSet(str)
	if err != nil {
		panic(err)
	}

	fmt.Printf("rs = %v\n", rs)
	// fmt.Printf("rs.hasNext() = %v\n", rs.hasNext())

	for i := 0; i < rs.GetColumnCount(); i++ {
		fmt.Printf("Column[%v]{name: %v, type: %v}\n", i, rs.GetColumnName(i), rs.GetColumnType(i))
	}

	for rs.Next() {
		fmt.Printf("rs.GetFloat(0) = %v\n", rs.GetFloat(0))
		fmt.Printf("rs.GetString(1) = %v\n", rs.GetString(1))
		fmt.Printf("rs.GetFloat(2) = %v\n", rs.GetFloat(2))
		fmt.Printf("rs.GetBytes(1) = %v\n", rs.GetBytes(1))
	}
}

func TestQueryFooTable(t *testing.T) {

	driver := GetDriver()

	if driver.Name() != "rqlite" {
		panic("Driver name is not rqlite")
	}

	config := make(map[string]string)
	config["host"] = "localhost:4001"
	config["debug"] = "false"

	con, err := driver.Get(config)
	if err != nil {
		panic(err)
	}

	sql := "SELECT * FROM foo"
	stm := con.SQL(sql)

	rs, err := stm.Execute()
	if err != nil {
		panic(err)
	}

	fmt.Printf("rs = %v\n", rs)

	for i := 0; i < rs.GetColumnCount(); i++ {
		fmt.Printf("Column[%v]{name: %v, type: %v}\n", i, rs.GetColumnName(i), rs.GetColumnType(i))
	}

	for rs.Next() {
		fmt.Printf("rs.GetFloat(0) = %v\n", rs.GetFloat(0))
		fmt.Printf("rs.GetString(1) = %v\n", rs.GetString(1))
		fmt.Printf("rs.GetBytes(1) = %v\n", rs.GetBytes(1))
	}
}

func TestMappedQueryFooTable(t *testing.T) {

	driver := GetDriver()

	if driver.Name() != "rqlite" {
		panic("Driver name is not rqlite")
	}

	config := make(map[string]string)
	config["host"] = "localhost:4001"
	config["debug"] = "false"

	con, err := driver.Get(config)
	if err != nil {
		panic(err)
	}

	sql := "SELECT * FROM foo WHERE id=? and name=?"
	stm := con.MappedSQL(sql)

	queryByIDAndName(stm, 1, "name1")
	queryByIDAndName(stm, 2, "name2")
}

func queryByIDAndName(stm godb.Statement, id int, name string) {

	stm.SetInt(0, id)
	stm.SetString(1, name)

	rs, err := stm.Execute()
	if err != nil {
		panic(err)
	}

	fmt.Printf("rs = %v\n", rs)

	for i := 0; i < rs.GetColumnCount(); i++ {
		fmt.Printf("Column[%v]{name: %v, type: %v}\n", i, rs.GetColumnName(i), rs.GetColumnType(i))
	}

	for rs.Next() {
		fmt.Printf("rs.GetFloat(0) = %v\n", rs.GetFloat(0))
		fmt.Printf("rs.GetString(1) = %v\n", rs.GetString(1))
		fmt.Printf("rs.GetBytes(1) = %v\n", rs.GetBytes(1))
	}
}

//CGO_ENABLED=0 GOOS=linux go test -v
