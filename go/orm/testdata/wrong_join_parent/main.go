package wrong_join_parent

import "github.com/neutron-build/neutron/go/orm"

type Parent struct{}
type Child struct{}
type Foreign struct{}

var scope orm.InnerJoin[Parent, Child]
var column orm.Column[Foreign, int64]
var invalid, _ = orm.JoinParentField(scope, column)
