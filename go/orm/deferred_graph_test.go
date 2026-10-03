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
