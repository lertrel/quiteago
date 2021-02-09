package quiteago

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
    "io/ioutil"
	"encoding/json"
	"errors"
	"bytes"
	"github.com/lertrel/godb"
)

// ----------------------------------------------------------------------------
// Rqlite DB Driver Implementation --------------------------------------------
// ----------------------------------------------------------------------------
func GetDriver() godb.ConnectionDriver {

	return &RqliteConnectionDriver{name: "rqlite"}
}

//[RqliteConnectionDriver]
type RqliteConnectionDriver struct {

	name string
}

func (d RqliteConnectionDriver) Name() string {

	return d.name
}

func (d RqliteConnectionDriver) Get(config map[string]string) (godb.Connection, error) {

	var host string
	var found bool
	var debugStr string

	if host, found = config["host"]; !found {
		return nil, errors.New("No rqlite host specified")
	}

	debug := false
	if debugStr, found = config["debug"]; found {
		if debugStr == "true" {
			debug = true
		}
	}

	return RqliteConnection{host: host, debug: debug}, nil
}

//[RqliteConnection]
type RqliteConnection struct {

	host string
	debug bool
}

func (c RqliteConnection) SQL(sql string) godb.Statement {

	return RqliteSqlStatement{host: c.host, sql: sql, debug: c.debug}
}

func (c RqliteConnection) MappedSQL(sql string) godb.Statement {

	stm := &RqliteMappedSqlStatement{
				host: c.host, 
				sql: sql, 
				debug: c.debug}

	stm.init()

	return stm
}

func (c RqliteConnection) DML(dml string) godb.Statement {
	panic("Not yet supported")
}

func (c RqliteConnection) MappedDML(dml string) godb.Statement {
	panic("Not yet supported")
}

//[RqliteSqlStatement]
type RqliteSqlStatement struct {

	host string
	sql string
	debug bool
}

func (s RqliteSqlStatement) IsSQL() bool { return true }
func (s RqliteSqlStatement) IsDML() bool { return false }
func (s RqliteSqlStatement) IsMappable() bool { return false }

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

func (s RqliteSqlStatement) Next() error { panic("Never be implemented") }
func (s RqliteSqlStatement) SetString(index int, val string) { panic("Never be implemented") }
func (s RqliteSqlStatement) SetInt(index int, val int) { panic("Never be implemented") }
func (s RqliteSqlStatement) SetFloat(index int, val float64) { panic("Never be implemented") }
func (s RqliteSqlStatement) SetBytes(index int, val []byte) { panic("Never be implemented") }


//[RqliteMappedSqlStatement]
type RqliteMappedSqlStatement struct {

	host string
	sql string
	debug bool
	params map[int]string
}

func (s *RqliteMappedSqlStatement) init() error {
	
	s.params = make(map[int]string)
	
	return nil 
}

func (s RqliteMappedSqlStatement) IsSQL() bool { return true }
func (s RqliteMappedSqlStatement) IsDML() bool { return false }
func (s RqliteMappedSqlStatement) IsMappable() bool { return true }

func (s RqliteMappedSqlStatement) Execute() (godb.ResultSet, error) {

	endpoint := "/db/query?pretty&timings&"
	sql := s.sql
	postContent := make([]string, len(s.params) + 1)
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

func (s RqliteMappedSqlStatement) Next() error { panic("Never be implemented") }

func (s *RqliteMappedSqlStatement) SetString(index int, val string) {

	s.params[index] = val
}

func (s *RqliteMappedSqlStatement) SetInt(index int, val int) { 
	s.params[index] = fmt.Sprintf(`%v`, val)
}

func (s *RqliteMappedSqlStatement) SetFloat(index int, val float64) { 
	s.params[index] = fmt.Sprintf(`%v`, val)
}

func (s *RqliteMappedSqlStatement) SetBytes(index int, val []byte) { 
	s.params[index] = string(val)
}


//[JsonResultSets]
type RqliteJsonResultSets struct {
	debug bool
}

func (r RqliteJsonResultSets) GetResultSet(jsonStr string) (godb.ResultSet, error) {

	if r.debug {
		log.Printf("RqliteJsonResultSets.GetResultSet() --> parsing %s", jsonStr)
	}

	var data RqliteQueryRecords
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return &RqliteJsonResultSet{}, err
	}

	if len(data.Results[0].Columns) == 0 {

		return nil, errors.New(string(jsonStr))
	}

	if r.debug {
		log.Printf("RqliteJsonResultSets.GetResultSet() --> parsed result = %v", data)
	}

	resultSet := &RqliteJsonResultSet{data: data, index: 0, debug: r.debug}

	return resultSet, nil
}

//[ResultSet]
type RqliteJsonResultSet struct {
	data  RqliteQueryRecords
	index int
	debug bool
}

type RqliteQueryRecord struct {
	Columns []string      `json:"columns"`
	Types   []string      `json:"types"`
	Values  []interface{} `json:"values"`
	Time    float64       `json:"time"`
}

type RqliteQueryRecords struct {
	Results []RqliteQueryRecord `json:"results"`
	Time    float64  `json:"time"`
}

func (rs RqliteJsonResultSet) GetColumnCount() int {

	return len(rs.data.Results[0].Columns)
}

func (rs RqliteJsonResultSet) GetColumnName(index int) string {

	return rs.data.Results[0].Columns[index]
}

func (rs RqliteJsonResultSet) GetColumnType(index int) string {

	return rs.data.Results[0].Types[index]
}

func (rs RqliteJsonResultSet) hasNext() bool {

	return rs.index < len(rs.data.Results[0].Values)
}

func (rs *RqliteJsonResultSet) Next() bool {

	if rs.hasNext() {
		
		if rs.debug {
			log.Printf("RqliteJsonResultSet.Next() --> Increase rs.index from %v to ", rs.index)
		}
		rs.index++
		if rs.debug {
			log.Printf("RqliteJsonResultSet.Next() --> %v\n", rs.index)
		}

		return true
	}

	return false
}

func (rs RqliteJsonResultSet) GetInt(index int) int {

	row := rs.index - 1
	if rs.debug {
		log.Printf("RqliteJsonResultSet.GetInt() --> row = %v", row)
	}

	return rs.data.Results[0].Values[row].([]interface{})[index].(int)
}

func (rs RqliteJsonResultSet) GetFloat(index int) float64 {

	row := rs.index - 1
	if rs.debug {
		log.Printf("RqliteJsonResultSet.GetFloat() --> row = %v", row)
	}

	return rs.data.Results[0].Values[row].([]interface{})[index].(float64)
}

func (rs RqliteJsonResultSet) GetString(index int) string {

	row := rs.index - 1
	if rs.debug {
		log.Printf("RqliteJsonResultSet.GetString() --> row = %v", row)
	}

	return rs.data.Results[0].Values[row].([]interface{})[index].(string)
}

func (rs RqliteJsonResultSet) GetBytes(index int) []byte {

	row := rs.index - 1
	if rs.debug {
		log.Printf("RqliteJsonResultSet.GetBytes() --> row = %v", row)
	}

	return []byte(rs.data.Results[0].Values[row].([]interface{})[index].(string))
}
