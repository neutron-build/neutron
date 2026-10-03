package wrong_predicate

import "neutron.local/polyglot/go-codegen-spike/model"

var invalid = model.UserColumns().ID.Eq("not an integer")
