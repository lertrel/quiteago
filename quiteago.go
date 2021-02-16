package quiteago

import (
	"github.com/lertrel/godb"
)

// ----------------------------------------------------------------------------
// Rqlite DB Driver Implementation --------------------------------------------
// ----------------------------------------------------------------------------

//GetDriver Rqlite DB Driver Implementation
func GetDriver() godb.ConnectionDriver {

	return &RqliteConnectionDriver{name: "rqlite"}
}
