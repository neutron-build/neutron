package wrong_projection

import (
	"neutron.local/polyglot/go-codegen-spike/model"
	"neutron.local/polyglot/go-codegen-spike/typed"
)

var invalid []int = typed.Project([]model.User{}, model.UserColumns().Name)
