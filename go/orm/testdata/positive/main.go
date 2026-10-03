package positive

import (
	"context"
	"github.com/neutron-build/neutron/go/orm"
)

type Record struct {
	ID   int64   `db:"id"`
	Name string  `db:"name"`
	Note *string `db:"note,nullable"`
}

func consumer(ctx context.Context, db orm.Executor) error {
	table, err := orm.NewTable[Record]("tenant", "records")
	if err != nil {
		return err
	}
	id, err := orm.NewColumn[Record, int64](table, "ID")
	if err != nil {
		return err
	}
	name, err := orm.NewColumn[Record, string](table, "Name")
	if err != nil {
		return err
	}
	note, err := orm.NewColumn[Record, *string](table, "Note")
	if err != nil {
		return err
	}
	query := orm.Query[Record]{}.Where(orm.And(id.Gt(0), name.Ne(""))).OrderBy(id.Asc()).Limit(20)
	var names []string
	names, err = orm.SelectColumn(ctx, db, name, query)
	_ = names
	if err != nil {
		return err
	}
	var pairs []orm.Pair[int64, *string]
	pairs, err = orm.SelectPair(ctx, db, id, note, query)
	_ = pairs
	if err != nil {
		return err
	}
	_, err = orm.InsertOne(ctx, db, table, orm.Set(name, orm.Some("")), orm.Set(note, orm.Some((*string)(nil))), orm.Set(id, orm.Default[int64]()))
	return err
}
