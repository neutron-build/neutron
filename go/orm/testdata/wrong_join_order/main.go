package wrong_join_order

import "github.com/neutron-build/neutron/go/orm"

type Parent struct{}
type Child struct{}
type Foreign struct{}

var query orm.JoinQuery[Parent, Child]
var order orm.Order[Foreign]
var invalid = query.OrderChild(order)
