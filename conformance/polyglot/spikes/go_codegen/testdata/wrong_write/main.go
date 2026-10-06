package wrong_write

import (
	"neutron.local/polyglot/go-codegen-spike/model"
	"neutron.local/polyglot/go-codegen-spike/typed"
)

var invalid = model.UserWrite{Active: typed.Some("false")}
