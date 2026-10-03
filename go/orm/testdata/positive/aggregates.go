package positive

import (
	"context"
	"github.com/neutron-build/neutron/go/orm"
)

func aggregateConsumer(ctx context.Context, db orm.Executor) error {
	table, err := orm.NewTable[Record]("tenant", "records")
	if err != nil {
		return err
	}
	id, _ := orm.NewColumn[Record, int64](table, "ID")
	name, _ := orm.NewColumn[Record, string](table, "Name")
	var total orm.Nullable[orm.Decimal]
	total, err = orm.AggregateOne(ctx, db, orm.SumInt64(id), orm.Query[Record]{})
	_ = total
	if err != nil {
		return err
	}
	var groups []orm.Pair[string, int64]
	count := orm.CountAll(table)
	groups, err = orm.SelectGrouped(ctx, db, name, count, orm.Query[Record]{}.OrderBy(name.Asc()), count.Compare(orm.Greater, int64(0)))
	_ = groups
	return err
}
