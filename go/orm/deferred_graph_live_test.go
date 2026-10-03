package orm

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestPostgresExplicitDeferredCyclicGraphAndValidationRollback(t *testing.T) {
	ctx, pool, admin, records := liveTransactionSetup(t)
	schemaSQL := strings.TrimSuffix(records, ".records")
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(schemaSQL, `"`), `"`), `""`, `"`)
	a, err := NewTable[cycleA](schema, "cycle_a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewTable[cycleB](schema, "cycle_b")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE TABLE "+a.info.sqlName()+" (id bigint PRIMARY KEY,peer bigint NOT NULL); CREATE TABLE "+b.info.sqlName()+" (id bigint PRIMARY KEY,peer bigint NOT NULL); ALTER TABLE "+a.info.sqlName()+" ADD CONSTRAINT cycle_a_to_b FOREIGN KEY(peer) REFERENCES "+b.info.sqlName()+"(id) DEFERRABLE INITIALLY IMMEDIATE; ALTER TABLE "+b.info.sqlName()+" ADD CONSTRAINT cycle_b_to_a FOREIGN KEY(peer) REFERENCES "+a.info.sqlName()+"(id) DEFERRABLE INITIALLY IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	af, err := NewDeferredForeignKey(ctx, admin, a, "cycle_a_to_b")
	if err != nil {
		t.Fatal(err)
	}
	bf, err := NewDeferredForeignKey(ctx, admin, b, "cycle_b_to_a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewDeferredForeignKey(ctx, admin, a, "cycle_b_to_a"); err == nil {
		t.Fatal("FK on different table admitted")
	}
	committed := 0
	ar, _ := NewHookRepository(a, HookSet[cycleA]{AfterCommit: []CommitHook[cycleA]{func(context.Context, WriteEvent[cycleA]) error { committed++; return nil }}})
	br, _ := NewHookRepository(b, HookSet[cycleB]{AfterCommit: []CommitHook[cycleB]{func(context.Context, WriteEvent[cycleB]) error { committed++; return nil }}})
	ai, _ := NewColumn[cycleA, int64](a, "ID")
	ap, _ := NewColumn[cycleA, int64](a, "Peer")
	bi, _ := NewColumn[cycleB, int64](b, "ID")
	bp, _ := NewColumn[cycleB, int64](b, "Peer")
	an, err := NewGraphNode(ar, Set(ai, Some(int64(1))), Set(ap, Some(int64(1))))
	if err != nil {
		t.Fatal(err)
	}
	bn, err := NewGraphNode(br, Set(bi, Some(int64(1))), Set(bp, Some(int64(1))))
	if err != nil {
		t.Fatal(err)
	}
	badA, err := NewGraphNode(ar, Set(ai, Some(int64(2))), Set(ap, Some(int64(999))))
	if err != nil {
		t.Fatal(err)
	}
	badB, err := NewGraphNode(br, Set(bi, Some(int64(2))), Set(bp, Some(int64(2))))
	if err != nil {
		t.Fatal(err)
	}
	var completed DeferredGraphResult
	err = WithHookTransaction(ctx, pool, HookTransactionOptions{}, func(session *WriteSession) error {
		var err error
		completed, err = RunDeferredGraph(ctx, session, []GraphStep{an, bn}, []DeferredForeignKey{af, bf}, DeferredGraphBudget{2, 4})
		if err != nil {
			return err
		}
		partial, err := RunDeferredGraph(ctx, session, []GraphStep{badA, badB}, []DeferredForeignKey{af, bf}, DeferredGraphBudget{2, 4})
		var native *Error
		if !errors.As(err, &native) || native.SQLState() != "23503" || len(partial.models) != 0 {
			return errors.New("deferred validation did not refuse incomplete cyclic graph")
		}
		_, err = session.Exec(ctx, "INSERT INTO "+records+" VALUES (7,'unrelated committed')")
		return err
	})
	if err != nil || committed != 2 {
		t.Fatal("deferred graph transaction/events", committed, err)
	}
	am, err := GraphNodeModel(completed, an)
	if err != nil || am.ID != 1 || am.Peer != 1 {
		t.Fatal("typed A result", err)
	}
	bm, err := GraphNodeModel(completed, bn)
	if err != nil || bm.ID != 1 || bm.Peer != 1 {
		t.Fatal("typed B result", err)
	}
	var countA, countB, cycles, unrelated int64
	if err := admin.QueryRow(ctx, "SELECT (SELECT count(*) FROM "+a.info.sqlName()+"),(SELECT count(*) FROM "+b.info.sqlName()+"),(SELECT count(*) FROM "+a.info.sqlName()+" a JOIN "+b.info.sqlName()+" b ON a.peer=b.id AND b.peer=a.id),(SELECT count(*) FROM "+records+" WHERE id=7)").Scan(&countA, &countB, &cycles, &unrelated); err != nil || countA != 1 || countB != 1 || cycles != 1 || unrelated != 1 {
		t.Fatal("independent native deferred graph oracle", countA, countB, cycles, unrelated, err)
	}
	requirePoolReuse(t, ctx, pool)
}
