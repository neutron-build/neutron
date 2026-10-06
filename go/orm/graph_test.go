package orm

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestGraphPrevalidationAndCompositeKeyDerivation(t *testing.T) {
	relation, id := associationMetadata(t, "owned")
	parentRepo, _ := NewHookRepository(relation.parent, HookSet[assocParent]{})
	childRepo, _ := NewHookRepository(relation.child, HookSet[assocChild]{})
	tenant, _ := NewColumn[assocChild, string](relation.child, "Tenant")
	name, _ := NewColumn[assocChild, string](relation.child, "Name")
	parent := assocParent{Tenant: "a", ID: 11}
	derived, err := derivedChildAssignments(relation, parent, []Assignment[assocChild]{Set(id, Some(int64(1))), Set(name, Some(""))})
	if err != nil {
		t.Fatal(err)
	}
	sql, args, err := insertSQL(relation.child, derived)
	if err != nil || !strings.Contains(sql, `"tenant", "parent_id"`) || len(args) != 4 || args[2] != "a" || args[3] != int64(11) {
		t.Fatal(sql, args, err)
	}
	budget := GraphBudget{MaxChildren: 2, MaxCoreStatements: 5}
	for _, children := range [][][]Assignment[assocChild]{
		{{Set(tenant, Some("foreign"))}},
		{{Set(id, Some(int64(1))), Set(id, Some(int64(2)))}},
		{{Set(id, Some(int64(1)))}, {Set(id, Some(int64(2)))}, {Set(id, Some(int64(3)))}},
	} {
		// A nil session would return ErrHookClosed if static validation accidentally
		// reached transaction acquisition; invalid graph admission must happen first.
		_, err := CreateRelated(context.Background(), nil, relation, parentRepo, childRepo, nil, children, budget)
		if err == nil || errors.Is(err, ErrHookClosed) {
			t.Fatal("invalid graph reached session", err)
		}
	}
	_, err = ReplaceRelated(context.Background(), nil, relation, parentRepo, childRepo, parent, [][]Assignment[assocChild]{{Set(id, Some(int64(1)))}}, GraphBudget{MaxChildren: 2, MaxCoreStatements: 3})
	if !errors.Is(err, ErrGraphBudget) {
		t.Fatal(err)
	}
	idParent, _ := NewColumn[assocParent, int64](relation.parent, "ID")
	self, _ := NewRelation(relation.parent, relation.parent, Join(idParent, idParent))
	_, err = CreateRelated(context.Background(), nil, self, parentRepo, parentRepo, nil, nil, budget)
	if err == nil || errors.Is(err, ErrHookClosed) {
		t.Fatal("self cycle did not refuse before effects", err)
	}
}
