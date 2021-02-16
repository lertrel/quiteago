package quiteago

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/lertrel/godb"
)

//RqliteMappedSqlStatement godb.SqlStatement implmentation of rqlite
//that allows sql parameters (?)
type RqliteMappedSqlStatement struct {
	host   string
	sql    string
	debug  bool
	params map[int]string
}

func (s *RqliteMappedSqlStatement) init() error {

	s.params = make(map[int]string)

	return nil
}

//IsSQL return true
func (s RqliteMappedSqlStatement) IsSQL() bool { return true }

//IsDML return false
func (s RqliteMappedSqlStatement) IsDML() bool { return false }

//IsMappable return true
func (s RqliteMappedSqlStatement) IsMappable() bool { return true }

//Execute executing SQL statement used for constructing the current
//RqliteMappedSqlStatement
func (s RqliteMappedSqlStatement) Execute() (godb.ResultSet, error) {

	endpoint := "/db/query?pretty&timings&"
	sql := s.sql
	postContent := make([]string, len(s.params)+1)
	postContent[0] = sql

	for k, v := range s.params {
		if v == "" {
			errMsg := fmt.Sprintf("SQL parameter %v is not set", k)
			return nil, errors.New(errMsg)
		}

		postContent[k+1] = v
	}

	postWrap := make([][]string, 1, len(postContent))
	postWrap[0] = postContent

	x := fmt.Sprintf("http://%s%s", s.host, endpoint)
	if s.debug {
		log.Printf("RqliteSqlStatement.Execute() --> URL: %s", x)
	}

	if s.debug {
		log.Printf("RqliteSqlStatement.Execute() --> RqliteSqlStatement.params = %v", s.params)
		log.Printf("RqliteSqlStatement.Execute() --> Converting %s to JSON", postWrap)
	}

	jsonBody, err := json.Marshal(postWrap)
	if err != nil {
		return nil, err
	}

	if s.debug {
		log.Printf("RqliteSqlStatement.Execute() --> JSON Body = %s", jsonBody)
	}

	response, err := http.Post(x, "application/json; charset=utf-8", bytes.NewBuffer(jsonBody))
	if err != nil {

		log.Printf("RqliteSqlStatement.Execute() --> The HTTP request (%s)\n failed with error %s\n", x, err)
		return nil, err
	}

	defer response.Body.Close()
	data, _ := ioutil.ReadAll(response.Body)

	resultSets := RqliteJsonResultSets{debug: s.debug}

	var rs godb.ResultSet
	if rs, err = resultSets.GetResultSet(string(data)); err != nil {

		log.Printf("RqliteSqlStatement.Execute() --> Error while parsing JSON result %v\n", err)

		if s.debug {
			log.Println(string(data))
		}

		return nil, err
	}

	return rs, nil
}

//Next will never be implemented
func (s RqliteMappedSqlStatement) Next() error { panic("Never be implemented") }

//SetString set parameter value referred by given index with given string value
func (s *RqliteMappedSqlStatement) SetString(index int, val string) {

	s.params[index] = val
}

//SetInt set parameter value referred by given index with given int value
func (s *RqliteMappedSqlStatement) SetInt(index int, val int) {
	s.params[index] = fmt.Sprintf(`%v`, val)
}

//SetFloat set parameter value referred by given index with given float value
func (s *RqliteMappedSqlStatement) SetFloat(index int, val float64) {
	s.params[index] = fmt.Sprintf(`%v`, val)
}

//SetBytes set parameter value referred by given index with given bytes value
func (s *RqliteMappedSqlStatement) SetBytes(index int, val []byte) {
	s.params[index] = string(val)
}
