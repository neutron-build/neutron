package positive

import (
	"neutron.local/polyglot/go-codegen-spike/model"
	"neutron.local/polyglot/go-codegen-spike/typed"
)

var columns = model.UserColumns()
var idPredicate typed.Predicate[model.User] = columns.ID.Eq(int64(7))
var names []string = typed.Project([]model.User{{Name: "name"}}, columns.Name)
var nullableNotes []*string = typed.Project([]model.User{}, columns.Note)
var explicitNull = model.UserWrite{Note: typed.Some((*string)(nil))}
var zeroValues = model.UserWrite{Active: typed.Some(false), Score: typed.Some(0), Name: typed.Some("")}
var omitted = model.UserWrite{}
