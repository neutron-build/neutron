package wrong_left_projection

import "github.com/neutron-build/neutron/go/orm"

type Parent struct{}
type Child struct{}

var scope orm.LeftJoin[Parent, Child]
var column orm.Column[Child, int64]
var invalid, _ = orm.InnerChildField(scope, column)
