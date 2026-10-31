package positive

import (
	"context"
	"github.com/neutron-build/neutron/go/orm"
	"time"
)

type PolicyRecord struct {
	Tenant  string     `db:"tenant"`
	ID      int64      `db:"id"`
	Value   string     `db:"value"`
	Version int64      `db:"version"`
	Deleted *time.Time `db:"deleted,nullable"`
}

func policyConsumer(ctx context.Context, scope *orm.Scope) error {
	table, err := orm.NewTable[PolicyRecord]("tenant", "records")
	if err != nil {
		return err
	}
	tenant, _ := orm.NewColumn[PolicyRecord, string](table, "Tenant")
	id, _ := orm.NewColumn[PolicyRecord, int64](table, "ID")
	value, _ := orm.NewColumn[PolicyRecord, string](table, "Value")
	version, _ := orm.NewColumn[PolicyRecord, int64](table, "Version")
	deleted, _ := orm.NewColumn[PolicyRecord, *time.Time](table, "Deleted")
	scoped, err := orm.NewScopedTable(table, tenant.Eq("a"))
	if err != nil {
		return err
	}
	policy, err := orm.NewSoftDelete(scoped, deleted)
	if err != nil {
		return err
	}
	var records []PolicyRecord
	records, err = policy.Select(ctx, scope, orm.Query[PolicyRecord]{})
	_ = records
	if err != nil {
		return err
	}
	if _, err := policy.Remove(ctx, scope, id.Eq(1), time.Now()); err != nil {
		return err
	}
	if _, err := policy.Restore(ctx, scope, id.Eq(1)); err != nil {
		return err
	}
	return orm.VersionedUpdate(ctx, scope, table, orm.And(tenant.Eq("a"), id.Eq(1)), version, 0, orm.Set(value, orm.Some("")))
}
