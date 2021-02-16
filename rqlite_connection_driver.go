package quiteago

import (
	"errors"

	"github.com/lertrel/godb"
)

//RqliteConnectionDriver godb.ConnectionDriver implmentation of rqlite
type RqliteConnectionDriver struct {
	name string
}

//Name providing name of the current driver (simply returned 'rqlite')
func (d RqliteConnectionDriver) Name() string {

	return d.name
}

//Get a method for obtaining godb.Connection implementation of rqlite
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
