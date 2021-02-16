package quiteago

import "log"

//RqliteJsonResultSet godb.ResultSet implmentation of rqlite
type RqliteJsonResultSet struct {
	data  RqliteQueryRecords
	index int
	debug bool
}

//RqliteQueryRecord internal data structure for containing 1 record
type RqliteQueryRecord struct {
	Columns []string      `json:"columns"`
	Types   []string      `json:"types"`
	Values  []interface{} `json:"values"`
	Time    float64       `json:"time"`
}

//RqliteQueryRecords internal data structure for containing multiple records
type RqliteQueryRecords struct {
	Results []RqliteQueryRecord `json:"results"`
	Time    float64             `json:"time"`
}

//GetColumnCount getting column count of the current resultset
func (rs RqliteJsonResultSet) GetColumnCount() int {

	return len(rs.data.Results[0].Columns)
}

//GetColumnName getting column name referred by the given index
func (rs RqliteJsonResultSet) GetColumnName(index int) string {

	return rs.data.Results[0].Columns[index]
}

//GetColumnType getting column type referred by the given index
func (rs RqliteJsonResultSet) GetColumnType(index int) string {

	return rs.data.Results[0].Types[index]
}

func (rs RqliteJsonResultSet) hasNext() bool {

	return rs.index < len(rs.data.Results[0].Values)
}

//Next checking if the current resultset is having more records to be fetched
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

//GetInt get an integer value of the column referred by given index
func (rs RqliteJsonResultSet) GetInt(index int) int {

	row := rs.index - 1
	if rs.debug {
		log.Printf("RqliteJsonResultSet.GetInt() --> row = %v", row)
	}

	return rs.data.Results[0].Values[row].([]interface{})[index].(int)
}

//GetFloat get an float value of the column referred by given index
func (rs RqliteJsonResultSet) GetFloat(index int) float64 {

	row := rs.index - 1
	if rs.debug {
		log.Printf("RqliteJsonResultSet.GetFloat() --> row = %v", row)
	}

	return rs.data.Results[0].Values[row].([]interface{})[index].(float64)
}

//GetString get an string value of the column referred by given index
func (rs RqliteJsonResultSet) GetString(index int) string {

	row := rs.index - 1
	if rs.debug {
		log.Printf("RqliteJsonResultSet.GetString() --> row = %v", row)
	}

	return rs.data.Results[0].Values[row].([]interface{})[index].(string)
}

//GetBytes get an bytes value of the column referred by given index
func (rs RqliteJsonResultSet) GetBytes(index int) []byte {

	row := rs.index - 1
	if rs.debug {
		log.Printf("RqliteJsonResultSet.GetBytes() --> row = %v", row)
	}

	return []byte(rs.data.Results[0].Values[row].([]interface{})[index].(string))
}
