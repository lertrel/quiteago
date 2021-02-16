package quiteago

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"

	"github.com/lertrel/godb"
)

//RqliteSqlStatement godb.SqlStatement implmentation of rqlite
type RqliteSqlStatement struct {
	host  string
	sql   string
	debug bool
}

//IsSQL return true
func (s RqliteSqlStatement) IsSQL() bool { return true }

//IsDML return false
func (s RqliteSqlStatement) IsDML() bool { return false }

//IsMappable return false
func (s RqliteSqlStatement) IsMappable() bool { return false }

//Execute executing SQL statement used for constructing the current
//RqliteSqlStatement
func (s RqliteSqlStatement) Execute() (godb.ResultSet, error) {

	endpoint := "/db/query?pretty&timings&"
	sql := url.QueryEscape(s.sql)
	x := fmt.Sprintf("http://%s%sq=%s", s.host, endpoint, sql)

	if s.debug {
		log.Printf("RqliteSqlStatement.Execute() --> URL: %s", x)
	}

	response, err := http.Get(x)
	if err != nil {

		log.Printf("RqliteSqlStatement.Execute() --> The HTTP request (%s)\n failed with error %s\n", x, err)
		return nil, err
	}

	defer response.Body.Close()
	data, _ := ioutil.ReadAll(response.Body)

	resultSets := RqliteJsonResultSets{debug: s.debug}

	var rs godb.ResultSet
	if rs, err = resultSets.GetResultSet(string(data)); err != nil {

		log.Printf("RqliteSqlStatement.Execute() --> Error while parsing JSON result %s\n", err)

		if s.debug {
			log.Println(data)
		}

		return nil, err
	}

	return rs, nil
}

//Next will never be implemented
func (s RqliteSqlStatement) Next() error { panic("Never be implemented") }

//SetString will never be implemented
func (s RqliteSqlStatement) SetString(index int, val string) { panic("Never be implemented") }

//SetInt will never be implemented
func (s RqliteSqlStatement) SetInt(index int, val int) { panic("Never be implemented") }

//SetFloat will never be implemented
func (s RqliteSqlStatement) SetFloat(index int, val float64) { panic("Never be implemented") }

//SetBytes will never be implemented
func (s RqliteSqlStatement) SetBytes(index int, val []byte) { panic("Never be implemented") }
