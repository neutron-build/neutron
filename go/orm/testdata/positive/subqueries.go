package positive

import (
	"context"
	"github.com/neutron-build/neutron/go/orm"
)

func subqueryConsumer(ctx context.Context, db orm.Executor) error {
	table, err := orm.NewTable[Record]("tenant", "records")
	if err != nil {
		return err
	}
	id, err := orm.NewColumn[Record, int64](table, "ID")
	if err != nil {
		return err
	}
	source, err := orm.NewScalarQuery(id, orm.Query[Record]{}.Where(id.Gt(0)))
	if err != nil {
		return err
	}
	var values []int64
	values, err = orm.SelectColumn(ctx, db, id, orm.Query[Record]{}.Where(orm.CompareSubquery(id, orm.Equal, source)))
	_ = values
	return err
}
