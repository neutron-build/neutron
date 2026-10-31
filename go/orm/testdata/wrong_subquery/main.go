package wrong_subquery

import "github.com/neutron-build/neutron/go/orm"

type Record struct {
	ID   int64  `db:"id"`
	Name string `db:"name"`
}

func invalidTypes() {
	table, _ := orm.NewTable[Record]("tenant", "records")
	id, _ := orm.NewColumn[Record, int64](table, "ID")
	name, _ := orm.NewColumn[Record, string](table, "Name")
	inner, _ := orm.NewScalarQuery(name, orm.Query[Record]{})
	var invalid = orm.InSubquery(id, inner)
	_ = invalid
}
