package orm

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestPostgresNullableConnectCreateNativeUniqueRace(t *testing.T) {
	ctx, pool, admin, records := liveTransactionSetup(t)
	schemaSQL := strings.TrimSuffix(records, ".records")
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(schemaSQL, `"`), `"`), `""`, `"`)
	relation, id, name, _ := nullableMetadata(t, schema)
	if _, err := admin.Exec(ctx, "CREATE TABLE "+schemaSQL+`.parents (tenant text NOT NULL,id bigint NOT NULL,name text NOT NULL,PRIMARY KEY(tenant,id)); CREATE TABLE `+schemaSQL+`.nullable_children (tenant text NOT NULL,parent_id bigint,id bigint NOT NULL,name text NOT NULL,PRIMARY KEY(tenant,id),FOREIGN KEY(tenant,parent_id) REFERENCES `+schemaSQL+`.parents(tenant,id)); INSERT INTO `+schemaSQL+`.parents VALUES ('a',1,'A1'),('a',2,'A2'); INSERT INTO `+schemaSQL+`.nullable_children VALUES ('a',2,11,'other owner')`); err != nil {
		t.Fatal(err)
	}
	tenant, _ := NewColumn[nullableChild, string](relation.relation.child, "Tenant")
	key, err := NewUniqueConstraint(ctx, admin, relation.relation.child, "nullable_children_pkey", BindColumn(tenant), BindColumn(id))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewUniqueConstraint(ctx, admin, relation.relation.child, "nullable_children_pkey", BindColumn(id)); err == nil {
		t.Fatal("incomplete native unique contract accepted")
	}
	parentRepo, _ := NewHookRepository(relation.relation.parent, HookSet[assocParent]{})
	createdEvents, updatedEvents, attempts := 0, 0, 0
	childRepo, _ := NewHookRepository(relation.relation.child, HookSet[nullableChild]{
		BeforeCreate: []BeforeHook[nullableChild]{func(context.Context, *HookContext, WriteIntent[nullableChild]) error {
			attempts++
			if attempts == 1 {
				// A separately committed native writer wins after ORM lookup but
				// before INSERT. NULL FK avoids waiting on the locked parent row.
				_, err := admin.Exec(ctx, "INSERT INTO "+schemaSQL+".nullable_children VALUES ('a',NULL,10,'native winner')")
				return err
			}
			return nil
		}},
		AfterCommit: []CommitHook[nullableChild]{func(_ context.Context, event WriteEvent[nullableChild]) error {
			if event.Operation == HookCreate {
				createdEvents++
			} else if event.Operation == HookModify {
				updatedEvents++
			}
			return nil
		}},
	})
	parent := assocParent{"a", 1, "stale"}
	budget := GraphBudget{MaxChildren: 1, MaxCoreStatements: 5}
	values := func(value int64) []Assignment[nullableChild] {
		return []Assignment[nullableChild]{Set(tenant, Some("a")), Set(id, Some(value))}
	}
	err = WithHookTransaction(ctx, pool, HookTransactionOptions{}, func(session *WriteSession) error {
		model, err := ConnectOrCreateNullable(ctx, session, relation, parentRepo, childRepo, parent, key, values(10), []Assignment[nullableChild]{Set(name, Some("attempt"))}, budget)
		if err != nil {
			return err
		}
		if model.ParentID == nil || *model.ParentID != 1 || model.Name != "native winner" {
			return errors.New("racing winner snapshot mismatch")
		}
		if _, err := ConnectOrCreateNullable(ctx, session, relation, parentRepo, childRepo, parent, key, values(11), []Assignment[nullableChild]{Set(name, Some("must not replace"))}, budget); !errors.Is(err, ErrAssociationOwner) {
			return errors.New("connect-or-create took foreign owner")
		}
		model, err = ConnectOrCreateNullable(ctx, session, relation, parentRepo, childRepo, parent, key, values(12), []Assignment[nullableChild]{Set(name, Some("new"))}, budget)
		if err != nil {
			return err
		}
		if model.ParentID == nil || *model.ParentID != 1 || model.Name != "new" {
			return errors.New("created model mismatch")
		}
		return nil
	})
	if err != nil || attempts != 2 || createdEvents != 1 || updatedEvents != 1 {
		t.Fatal("connect-create race/hooks", attempts, createdEvents, updatedEvents, err)
	}
	var owner int64
	var nativeName string
	if err := admin.QueryRow(ctx, "SELECT parent_id,name FROM "+schemaSQL+".nullable_children WHERE tenant='a' AND id=10").Scan(&owner, &nativeName); err != nil || owner != 1 || nativeName != "native winner" {
		t.Fatal("native winner connect oracle", err)
	}
	if err := admin.QueryRow(ctx, "SELECT parent_id,name FROM "+schemaSQL+".nullable_children WHERE tenant='a' AND id=11").Scan(&owner, &nativeName); err != nil || owner != 2 || nativeName != "other owner" {
		t.Fatal("foreign owner changed", err)
	}
	requirePoolReuse(t, ctx, pool)
}
