package wrong_join_filter

import "github.com/neutron-build/neutron/go/orm"

type Parent struct{}
type Child struct{}
type Foreign struct{}

var query orm.JoinQuery[Parent, Child]
var predicate orm.Predicate[Foreign]
var invalid = query.WhereChild(predicate)
