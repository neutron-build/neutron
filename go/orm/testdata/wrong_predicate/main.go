package wrong_predicate

import "github.com/neutron-build/neutron/go/orm"

type Record struct {
	ID int64 `db:"id"`
}

var column orm.Column[Record, int64]
var invalid = column.Eq("wrong")
