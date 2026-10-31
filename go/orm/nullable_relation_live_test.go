package orm

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestPostgresNullableRelationConnectDisconnectAndOwnerGuards(t *testing.T) {
	ctx, pool, admin, records := liveTransactionSetup(t)
	schemaSQL := strings.TrimSuffix(records, ".records")
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(schemaSQL, `"`), `"`), `""`, `"`)
	relation, id, name, fk := nullableMetadata(t, schema)
	if _, err := admin.Exec(ctx, "CREATE TABLE "+schemaSQL+`.parents (tenant text NOT NULL,id bigint NOT NULL,name text NOT NULL,PRIMARY KEY(tenant,id)); CREATE TABLE `+schemaSQL+`.nullable_children (tenant text NOT NULL,parent_id bigint,id bigint NOT NULL,name text NOT NULL,PRIMARY KEY(tenant,id),FOREIGN KEY(tenant,parent_id) REFERENCES `+schemaSQL+`.parents(tenant,id)); INSERT INTO `+schemaSQL+`.parents VALUES ('a',1,'A1'),('a',2,'A2'),('b',1,'B1'); INSERT INTO `+schemaSQL+`.nullable_children VALUES ('a',NULL,10,'unowned'),('a',2,11,'owned by A2'),('b',NULL,10,'foreign')`); err != nil {
		t.Fatal(err)
	}
	parentRepo, _ := NewHookRepository(relation.relation.parent, HookSet[assocParent]{})
	committed := 0
	childRepo, _ := NewHookRepository(relation.relation.child, HookSet[nullableChild]{AfterCommit: []CommitHook[nullableChild]{func(context.Context, WriteEvent[nullableChild]) error { committed++; return nil }}})
	tenant, _ := NewColumn[nullableChild, string](relation.relation.child, "Tenant")
	selector := func(value int64) Predicate[nullableChild] { return And(tenant.Eq("a"), id.Eq(value)) }
	parent := assocParent{"a", 1, "ignored stale name"}
	budget := GraphBudget{MaxChildren: 1, MaxCoreStatements: 3}
	err := WithHookTransaction(ctx, pool, HookTransactionOptions{}, func(session *WriteSession) error {
		if count, err := ConnectNullable(ctx, session, relation, parentRepo, childRepo, parent, selector(10), budget); err != nil || count != 1 {
			return errors.New("unowned connect failed")
		}
		if _, err := ConnectNullable(ctx, session, relation, parentRepo, childRepo, parent, selector(11), budget); !errors.Is(err, ErrAssociationOwner) {
			return errors.New("implicit reparent accepted")
		}
		if _, err := ReparentNullable(ctx, session, relation, parentRepo, childRepo, parent, And(tenant.Eq("b"), id.Eq(10)), budget); !errors.Is(err, ErrAssociationOwner) {
			return errors.New("cross-tenant reparent accepted")
		}
		if _, err := UpdateNullable(ctx, session, relation, parentRepo, childRepo, parent, selector(10), budget, Set(name, Some(""))); err != nil {
			return err
		}
		if _, err := UpdateNullable(ctx, session, relation, parentRepo, childRepo, parent, selector(10), budget, Set(fk, Some((*int64)(nil)))); err == nil {
			return errors.New("owned FK override accepted")
		}
		if _, err := ReparentNullable(ctx, session, relation, parentRepo, childRepo, parent, selector(11), budget); err != nil {
			return err
		}
		if _, err := DisconnectNullable(ctx, session, relation, parentRepo, childRepo, parent, selector(11), budget); err != nil {
			return err
		}
		return nil
	})
	if err != nil || committed != 4 {
		t.Fatal("nullable graph/events", committed, err)
	}
	var nativeFK *int64
	var nativeName string
	if err := admin.QueryRow(ctx, "SELECT parent_id,name FROM "+schemaSQL+".nullable_children WHERE tenant='a' AND id=10").Scan(&nativeFK, &nativeName); err != nil || nativeFK == nil || *nativeFK != 1 || nativeName != "" {
		t.Fatal("native connect/zero update oracle", err)
	}
	if err := admin.QueryRow(ctx, "SELECT parent_id,name FROM "+schemaSQL+".nullable_children WHERE tenant='b' AND id=10").Scan(&nativeFK, &nativeName); err != nil || nativeFK != nil || nativeName != "foreign" {
		t.Fatal("foreign child changed", err)
	}
	if err := admin.QueryRow(ctx, "SELECT parent_id FROM "+schemaSQL+".nullable_children WHERE tenant='a' AND id=11").Scan(&nativeFK); err != nil || nativeFK != nil {
		t.Fatal("disconnect deleted/retained ownership", err)
	}
	loaded, err := LoadNullableMany(ctx, admin, relation, []assocParent{parent}, Query[nullableChild]{}, LoadBudget{MaxParents: 1, MaxRows: 2, BatchSize: 1})
	if err != nil || len(loaded) != 1 || len(loaded[0].Children) != 1 || loaded[0].Children[0].ID != 10 {
		t.Fatal("nullable eager ownership", err)
	}
	err = WithHookTransaction(ctx, pool, HookTransactionOptions{}, func(session *WriteSession) error {
		_, err := DeleteNullableChild(ctx, session, relation, parentRepo, childRepo, parent, selector(10), budget)
		return err
	})
	if err != nil || committed != 5 {
		t.Fatal("explicit nullable orphan delete", err)
	}
	requirePoolReuse(t, ctx, pool)
}
