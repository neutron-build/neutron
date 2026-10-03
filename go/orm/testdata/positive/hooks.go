package positive

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neutron-build/neutron/go/orm"
)

func ownedHookConsumer(ctx context.Context, pool *pgxpool.Pool) error {
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
	repo, err := orm.NewHookRepository(table, orm.HookSet[Record]{BeforeCreate: []orm.BeforeHook[Record]{func(ctx context.Context, h *orm.HookContext, intent orm.WriteIntent[Record]) error {
		input, err := orm.InspectHookValue(intent, name)
		if err != nil {
			return err
		}
		_ = input.Value
		_, err = h.Exec(ctx, "SELECT $1::bigint", int64(1))
		return err
	}}, AfterCommit: []orm.CommitHook[Record]{func(ctx context.Context, event orm.WriteEvent[Record]) error { _ = event.Model; return ctx.Err() }}})
	if err != nil {
		return err
	}
	return orm.WithHookTransaction(ctx, pool, orm.HookTransactionOptions{}, func(session *orm.WriteSession) error {
		var model Record
		model, err = orm.HookInsert(ctx, session, repo, orm.Set(id, orm.Some(int64(1))), orm.Set(name, orm.Some("first")))
		_ = model
		if err != nil {
			return err
		}
		return session.Savepoint(ctx, func(child *orm.WriteSession) error {
			_, err := orm.HookUpdate(ctx, child, repo, id.Eq(1), orm.Set(name, orm.Some("updated")))
			return err
		})
	})
}
