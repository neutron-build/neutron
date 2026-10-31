package orm

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestMembershipNullOrderingAndCompilation(t *testing.T) {
	table, id, _, _, _, _, note := setupMetadata(t)
	text := "'; --"
	query := Query[testModel]{}.Where(And(id.In(1, 2), Not(note.NotIn(nil, &text)), id.CompareAll(Greater, 0, -1))).OrderBy(note.Desc().NullsFirst(), id.Asc().NullsLast())
	text = "mutated"
	compiled, err := CompileSelect(table, query)
	if err != nil || !strings.Contains(compiled.SQL, `("id" IN ($1, $2) AND NOT ("note" NOT IN ($3, $4)) AND ("id" > $5 AND "id" > $6))`) || !strings.HasSuffix(compiled.SQL, `ORDER BY "note" DESC NULLS FIRST, "id" ASC NULLS LAST`) || !reflect.DeepEqual(compiled.Args, []any{int64(1), int64(2), nil, "'; --", int64(0), int64(-1)}) {
		t.Fatal(compiled, err)
	}
	for _, item := range []struct {
		predicate Predicate[testModel]
		expected  string
	}{{id.In(), "FALSE"}, {id.NotIn(), "TRUE"}, {id.CompareAny(Equal), "FALSE"}, {id.CompareAll(Equal), "TRUE"}} {
		args := []any{}
		actual, err := renderPredicate(table.info, item.predicate.expr, &args)
		if err != nil || actual != item.expected || len(args) != 0 {
			t.Fatal(actual, args, err)
		}
	}
	if _, err := CompileSelect(table, Query[testModel]{}.Where(id.CompareAny(Comparison("= 1; --"), 1))); err == nil {
		t.Fatal("unchecked comparison interpolated")
	}
	other, otherID, _, _, _, _, _ := setupMetadata(t)
	_ = other
	if _, err := CompileSelect(table, Query[testModel]{}.Where(otherID.In())); err == nil {
		t.Fatal("empty list bypassed binding check")
	}
}

type streamFixtureExecutor struct {
	associationRefusingExecutor
	result *associationFixtureRows
}

func (db *streamFixtureExecutor) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return db.result, nil
}

func TestStreamBudgetAndEarlyCleanup(t *testing.T) {
	relation, _ := associationMetadata(t, "owned")
	for _, mode := range []string{"stop", "budget", "failure", "panic", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			db := &streamFixtureExecutor{result: &associationFixtureRows{rows: []assocChild{{ID: 1}, {ID: 2}, {ID: 3}}}}
			marker := errors.New("visitor failure")
			calls := 0
			var caught any
			var err error
			func() {
				defer func() { caught = recover() }()
				err = Stream(ctx, db, relation.child, Query[assocChild]{}, 2, func(model assocChild) (bool, error) {
					calls++
					if model.ID != int64(calls) {
						t.Fatal("model decode", model)
					}
					switch mode {
					case "stop":
						return false, nil
					case "failure":
						return false, marker
					case "panic":
						panic(marker)
					case "cancel":
						cancel()
					}
					return true, nil
				})
			}()
			if !db.result.closed {
				t.Fatal("native rows retained")
			}
			switch mode {
			case "stop":
				if err != nil || calls != 1 {
					t.Fatal(err, calls)
				}
			case "budget":
				if !errors.Is(err, ErrStreamBudget) || calls != 2 {
					t.Fatal(err, calls)
				}
			case "failure":
				if !errors.Is(err, marker) {
					t.Fatal(err)
				}
			case "panic":
				if caught != marker {
					t.Fatal(caught)
				}
			case "cancel":
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			}
		})
	}
}
