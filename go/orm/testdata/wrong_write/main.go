package wrong_write

import "github.com/neutron-build/neutron/go/orm"

type Record struct {
	Active bool `db:"active"`
}

var column orm.Column[Record, bool]
var invalid = orm.Set(column, orm.Some("false"))
