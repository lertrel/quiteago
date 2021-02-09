# quiteago
A pure go package for connecting with rqlite DB

Quick Start:

```go
package main

import(
	"fmt"
	"github.com/lertrel/quiteago"
)

func main() {

    driver := quiteago.GetDriver()

    if (driver.Name() != "rqlite") {
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
```
