package quiteago

import "github.com/lertrel/godb"

//RqliteConnection godb.Connection implmentation of rqlite
type RqliteConnection struct {
	host  string
	debug bool
}

//SQL godb.Connection.SQL() method implementation of rqlite
//This method will send the given sql through rqlite API
//and then converting the returned result JSON to godb.Statement
func (c RqliteConnection) SQL(sql string) godb.Statement {

	return RqliteSqlStatement{host: c.host, sql: sql, debug: c.debug}
}

//MappedSQL same as method SQL but allowed parameters in sql (?)
func (c RqliteConnection) MappedSQL(sql string) godb.Statement {

	stm := &RqliteMappedSqlStatement{
		host:  c.host,
		sql:   sql,
		debug: c.debug}

	stm.init()

	return stm
}

//DML not yet supported
func (c RqliteConnection) DML(dml string) godb.Statement {
	panic("Not yet supported")
}

//MappedDML not yet supported
func (c RqliteConnection) MappedDML(dml string) godb.Statement {
	panic("Not yet supported")
}
