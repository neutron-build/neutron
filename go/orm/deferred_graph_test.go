package orm

import (
	"context"
	"errors"
	"testing"
)

type cycleA struct {
	ID   int64 `db:"id"`
	Peer int64 `db:"peer"`
}
type cycleB struct {
	ID   int64 `db:"id"`
	Peer int64 `db:"peer"`
}

func TestDeferredGraphStaticBudgetAndTypedResultOwnership(t *testing.T) {
	table, err := NewTable[cycleA]("owned", "cycle_a")
	if err != nil {
		t.Fatal(err)
	}
	repo, _ := NewHookRepository(table, HookSet[cycleA]{})
	id, _ := NewColumn[cycleA, int64](table, "ID")
	peer, _ := NewColumn[cycleA, int64](table, "Peer")
	node, err := NewGraphNode(repo, Set(id, Some(int64(1))), Set(peer, Some(int64(1))))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RunDeferredGraph(context.Background(), nil, []GraphStep{node}, []DeferredForeignKey{{"owned", "cycle_a", "fk", "owned", "cycle_a"}}, DeferredGraphBudget{1, 2}); !errors.Is(err, ErrGraphBudget) {
		t.Fatal("effect before graph budget refusal", err)
	}
	other, err := NewGraphNode(repo, Set(id, Some(int64(1))), Set(peer, Some(int64(1))))
	if err != nil {
		t.Fatal(err)
	}
	if node.id == other.id {
		t.Fatal("different graph handles aliased identity")
	}
	result := DeferredGraphResult{map[*graphNodeID]any{node.id: cycleA{1, 1}}}
	if _, err := GraphNodeModel(result, other); err == nil {
		t.Fatal("result accepted unrelated identical node")
	}
	model, err := GraphNodeModel(result, node)
	if err != nil || model.ID != 1 {
		t.Fatal(model, err)
	}
}

func TestDeferredConstraintControlsArePrivateAndRetainScopeOwnership(t *testing.T) {
	ctx := context.Background()
	driver := &lifecycleDriver{}
	_, scope, _, _ := fixtureOwner(ctx, driver)
	if _, err := scope.Exec(ctx, `SET CONSTRAINTS "owned"."fk" DEFERRED`); err == nil || driver.execs != 0 {
		t.Fatal("raw public control admitted", err)
	}
	session := newWriteSession(scope)
	keys := []DeferredForeignKey{{schema: "owned", table: "cycle_a", name: "fk", targetSchema: "owned", targetTable: "cycle_b"}}
	if err := session.setConstraints(ctx, keys, true); err != nil {
		t.Fatal("sealed defer refused", err)
	}
	if err := session.setConstraints(ctx, keys, false); err != nil {
		t.Fatal("sealed validation refused", err)
	}
	if len(driver.statements) != 2 || driver.statements[0] != `SET CONSTRAINTS "owned"."fk" DEFERRED` || driver.statements[1] != `SET CONSTRAINTS "owned"."fk" IMMEDIATE` {
		t.Fatal("sealed SQL changed", driver.statements)
	}
	scope.owner.mu.Lock()
	scope.closed = true
	scope.owner.mu.Unlock()
	if err := session.setConstraints(ctx, keys, true); !errors.Is(err, ErrScopeClosed) || driver.execs != 2 {
		t.Fatal("sealed control bypassed terminal ownership", err)
	}
}
