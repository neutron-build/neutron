package wrong_join

import "github.com/neutron-build/neutron/go/orm"

type Parent struct {
	ID int64 `db:"id"`
}
type Child struct {
	ParentID string `db:"parent_id"`
}

var parent orm.Column[Parent, int64]
var child orm.Column[Child, string]
var invalid = orm.Join(parent, child)
