package null_nonnullable

import (
	"neutron.local/polyglot/go-codegen-spike/model"
	"neutron.local/polyglot/go-codegen-spike/typed"
)

var invalid = model.UserWrite{Name: typed.Some((*string)(nil))}
