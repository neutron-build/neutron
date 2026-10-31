package wrong_model

import "github.com/neutron-build/neutron/go/orm"

type User struct {
	ID int64 `db:"id"`
}
type Project struct {
	ID int64 `db:"id"`
}

var column orm.Column[User, int64]
var invalid = orm.Query[Project]{}.Where(column.Eq(1))
