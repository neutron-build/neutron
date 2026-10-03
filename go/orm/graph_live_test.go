package orm

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestPostgresOwnedRelationGraphMutations(t *testing.T) {
	ctx, pool, admin, records := liveTransactionSetup(t)
	schemaSQL := strings.TrimSuffix(records, ".records")
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(schemaSQL, `"`), `"`), `""`, `"`)
	relation, childID := associationMetadata(t, schema)
	if _, err := admin.Exec(ctx, "CREATE TABLE "+schemaSQL+`.parents (tenant text NOT NULL,id bigint NOT NULL,name text NOT NULL,PRIMARY KEY(tenant,id)); CREATE TABLE `+schemaSQL+`.children (tenant text NOT NULL,parent_id bigint NOT NULL,id bigint NOT NULL,name text NOT NULL,PRIMARY KEY(tenant,id),FOREIGN KEY(tenant,parent_id) REFERENCES `+schemaSQL+`.parents(tenant,id)); INSERT INTO `+schemaSQL+`.parents VALUES ('b',1,'foreign'); INSERT INTO `+schemaSQL+`.children VALUES ('b',1,10,'foreign child')`); err != nil {
		t.Fatal(err)
	}
	parentTenant, _ := NewColumn[assocParent, string](relation.parent, "Tenant")
	parentID, _ := NewColumn[assocParent, int64](relation.parent, "ID")
	parentName, _ := NewColumn[assocParent, string](relation.parent, "Name")
	childName, _ := NewColumn[assocChild, string](relation.child, "Name")
	notifications := []string{}
	parentRepo, _ := NewHookRepository(relation.parent, HookSet[assocParent]{AfterCommit: []CommitHook[assocParent]{func(_ context.Context, event WriteEvent[assocParent]) error {
		notifications = append(notifications, "parent")
		return nil
	}}})
	childRepo, _ := NewHookRepository(relation.child, HookSet[assocChild]{AfterCommit: []CommitHook[assocChild]{func(_ context.Context, event WriteEvent[assocChild]) error {
		notifications = append(notifications, "child")
		return nil
	}}})
	parentPlan := func(id int64) []Assignment[assocParent] {
		return []Assignment[assocParent]{Set(parentTenant, Some("a")), Set(parentID, Some(id)), Set(parentName, Some("parent"))}
	}
	childPlan := func(ids ...int64) [][]Assignment[assocChild] {
		plans := make([][]Assignment[assocChild], len(ids))
		for i, id := range ids {
			plans[i] = []Assignment[assocChild]{Set(childID, Some(id)), Set(childName, Some("child"))}
		}
		return plans
	}
	budget := GraphBudget{MaxChildren: 3, MaxCoreStatements: 7}
	var graph Association[assocParent, assocChild]
	err := WithHookTransaction(ctx, pool, HookTransactionOptions{}, func(session *WriteSession) error {
		var err error
		graph, err = CreateRelated(ctx, session, relation, parentRepo, childRepo, parentPlan(1), childPlan(10, 11), budget)
		return err
	})
	if err != nil || len(graph.Children) != 2 || !reflect.DeepEqual(notifications, []string{"parent", "child", "child"}) {
		t.Fatal(graph, notifications, err)
	}
	for _, child := range graph.Children {
		if child.Tenant != "a" || child.ParentID != 1 {
			t.Fatal("derived tenant keys", child)
		}
	}
	assertChildren := func(tenant string, want []int64) {
		t.Helper()
		rows, err := admin.Query(ctx, "SELECT id FROM "+schemaSQL+".children WHERE tenant=$1 ORDER BY id", tenant)
		if err != nil {
			t.Fatal(err)
		}
		actual := []int64{}
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			actual = append(actual, id)
		}
		rows.Close()
		if rows.Err() != nil || !reflect.DeepEqual(actual, want) {
			t.Fatal(actual, want, rows.Err())
		}
	}
	assertChildren("a", []int64{10, 11})
	assertChildren("b", []int64{10})
	// Final child native failure must remove both newly inserted parent and first
	// child, even when the outer application swallows the graph error.
	notifications = nil
	err = WithHookTransaction(ctx, pool, HookTransactionOptions{}, func(session *WriteSession) error {
		_, err := CreateRelated(ctx, session, relation, parentRepo, childRepo, parentPlan(2), childPlan(20, 20), budget)
		var native *Error
		if !errors.As(err, &native) || native.SQLState() != "23505" {
			t.Fatal("final child native failure", err)
		}
		_, err = session.Exec(ctx, "SELECT 1")
		return err
	})
	if err != nil || len(notifications) != 0 {
		t.Fatal(err, notifications)
	}
	var count int
	if err := admin.QueryRow(ctx, "SELECT count(*) FROM "+schemaSQL+".parents WHERE tenant='a' AND id=2").Scan(&count); err != nil || count != 0 {
		t.Fatal("failed graph parent survived", count, err)
	}
	assertChildren("a", []int64{10, 11})
	// A final child hook failure has the same atomic rollback and event policy.
	marker := errors.New("final child hook failed")
	failing, _ := NewHookRepository(relation.child, HookSet[assocChild]{AfterCreate: []AfterHook[assocChild]{func(_ context.Context, _ *HookContext, event WriteEvent[assocChild]) error {
		if event.Model.ID == 31 {
			return marker
		}
		return nil
	}}, AfterCommit: childRepo.hooks.AfterCommit})
	err = WithHookTransaction(ctx, pool, HookTransactionOptions{}, func(session *WriteSession) error {
		_, err := CreateRelated(ctx, session, relation, parentRepo, failing, parentPlan(3), childPlan(30, 31), budget)
		if !errors.Is(err, marker) {
			t.Fatal(err)
		}
		return nil
	})
	if err != nil || len(notifications) != 0 {
		t.Fatal(err, notifications)
	}
	assertChildren("a", []int64{10, 11})
	// Replacement rollback restores the orphan rows deleted earlier in the graph.
	err = WithHookTransaction(ctx, pool, HookTransactionOptions{}, func(session *WriteSession) error {
		_, err := ReplaceRelated(ctx, session, relation, parentRepo, childRepo, graph.Parent, childPlan(40, 40), budget)
		if err == nil {
			t.Fatal("duplicate replacement accepted")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	assertChildren("a", []int64{10, 11})
	err = WithHookTransaction(ctx, pool, HookTransactionOptions{}, func(session *WriteSession) error {
		_, err := ReplaceRelated(ctx, session, relation, parentRepo, childRepo, graph.Parent, childPlan(40), budget)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	assertChildren("a", []int64{40})
	assertChildren("b", []int64{10})
	err = WithHookTransaction(ctx, pool, HookTransactionOptions{}, func(session *WriteSession) error {
		_, err := AppendRelated(ctx, session, relation, parentRepo, childRepo, graph.Parent, childPlan(41), budget)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	assertChildren("a", []int64{40, 41})
	err = WithHookTransaction(ctx, pool, HookTransactionOptions{}, func(session *WriteSession) error {
		saved, err := SaveRelated(ctx, session, relation, parentRepo, childRepo, graph.Parent, []Assignment[assocParent]{Set(parentName, Some(""))}, nil, budget)
		if err == nil && saved.Parent.Name != "" {
			t.Fatal("zero-safe graph parent update", saved)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	err = WithHookTransaction(ctx, pool, HookTransactionOptions{}, func(session *WriteSession) error {
		_, err := DeleteRelated(ctx, session, relation, parentRepo, childRepo, graph.Parent, GraphBudget{MaxChildren: 1, MaxCoreStatements: 4})
		if !errors.Is(err, ErrGraphBudget) {
			t.Fatal(err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	assertChildren("a", []int64{40, 41})
	err = WithHookTransaction(ctx, pool, HookTransactionOptions{}, func(session *WriteSession) error {
		count, err := DeleteRelated(ctx, session, relation, parentRepo, childRepo, graph.Parent, budget)
		if count != 2 {
			t.Fatal(count)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	assertChildren("a", []int64{})
	assertChildren("b", []int64{10})
	if err := admin.QueryRow(ctx, "SELECT count(*) FROM "+schemaSQL+".parents WHERE tenant='a'").Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	requirePoolReuse(t, ctx, pool)
}
