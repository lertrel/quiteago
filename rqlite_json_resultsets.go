package quiteago

import (
	"encoding/json"
	"errors"
	"log"

	"github.com/lertrel/godb"
)

//RqliteJsonResultSets for converting rqlite JSON result
//to godb.ResultSet implementation of rqlite
type RqliteJsonResultSets struct {
	debug bool
}

//GetResultSet converting rqlite JSON result
//to godb.ResultSet implementation of rqlite
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
