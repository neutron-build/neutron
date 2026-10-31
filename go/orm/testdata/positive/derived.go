package positive

import (
	"context"
	"github.com/neutron-build/neutron/go/orm"
)

func derivedConsumer(ctx context.Context, db orm.Executor) error {
	table, err := orm.NewTable[Record]("tenant", "records")
	if err != nil {
		return err
	}
	id, _ := orm.NewColumn[Record, int64](table, "ID")
	left, _ := orm.NewModelQuery(table, orm.Query[Record]{}.Where(id.Eq(1)))
	right, _ := orm.NewModelQuery(table, orm.Query[Record]{}.Where(id.Eq(2)))
	union, err := orm.UnionAll(left, right)
	if err != nil {
		return err
	}
	derived, err := orm.NewCTE("selected", union)
	if err != nil {
		return err
	}
	derivedID, err := orm.DerivedColumn[Record, int64](derived, "ID")
	if err != nil {
		return err
	}
	var values []int64
	values, err = orm.SelectDerivedColumn(ctx, db, derived, derivedID, orm.Query[Record]{}.OrderBy(derivedID.Asc()))
	_ = values
	return err
}
