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
	_, err = orm.CompileSelect(table, query.Where(orm.And(id.In(1, 2), id.CompareAny(orm.Greater, 0), orm.Not(note.NotIn(nil)))).OrderBy(note.Desc().NullsFirst()))
	if err != nil {
		return err
	}
	var optional orm.Nullable[Record]
	optional, err = orm.SelectOptional(ctx, db, table, orm.Query[Record]{}.Where(id.Eq(1)))
	_ = optional
	if err != nil {
		return err
	}
	if err = orm.Stream(ctx, db, table, query, 20, func(record Record) (bool, error) { _ = record.ID; return false, nil }); err != nil {
		return err
	}
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

func associations(ctx context.Context, db orm.Executor, records []Record) error {
	table, err := orm.NewTable[Record]("tenant", "records")
	if err != nil {
		return err
	}
	id, err := orm.NewColumn[Record, int64](table, "ID")
	if err != nil {
		return err
	}
	relation, err := orm.NewRelation(table, table, orm.Join(id, id))
	if err != nil {
		return err
	}
	_, err = orm.LoadOne(ctx, db, orm.Inverse(relation), records, orm.Query[Record]{}, orm.LoadBudget{MaxParents: 100, MaxRows: 100, BatchSize: 20})
	return err
}

func joined(ctx context.Context, db orm.Executor, relation orm.Relation[Record, Record], parentID, childID orm.Column[Record, int64]) error {
	scope, err := orm.NewInnerJoin(relation)
	if err != nil {
		return err
	}
	first, err := orm.JoinParentField(scope, parentID)
	if err != nil {
		return err
	}
	second, err := orm.InnerChildField(scope, childID)
	if err != nil {
		return err
	}
	var inner []orm.Pair[int64, int64]
	inner, err = orm.SelectJoinedPair(ctx, db, first, second, scope.Query().WhereParent(parentID.Gt(0)).OrderChild(childID.Asc()))
	_ = inner
	if err != nil {
		return err
	}
	left, err := orm.NewLeftJoin(relation)
	if err != nil {
		return err
	}
	parent, err := orm.JoinParentField(left, parentID)
	if err != nil {
		return err
	}
	outer, err := orm.LeftChildField(left, childID)
	if err != nil {
		return err
	}
	var result []orm.Pair[int64, orm.Nullable[int64]]
	result, err = orm.SelectJoinedPair(ctx, db, parent, outer, left.Query())
	_ = result
	return err
}
