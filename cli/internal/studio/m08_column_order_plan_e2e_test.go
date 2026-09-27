package studio

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// TestStudioM08ColumnOrderPlanE2E (M08 review-2, M09's Studio exit): after a
// column declared between existing ones was appended by PostgreSQL, the
// designer's next plan carries only its own change and converges. The
// designer plans from the live document, so its order is the database's.
//
// Skipped unless NEUTRON_E2E_DATABASE_URL points at a disposable Postgres
// server; NEUTRON_LIVE_REQUIRED=1 turns a missing URL into a failure. Uses a
// uniquely named m08s_* database, dropped afterwards.
func TestStudioM08ColumnOrderPlanE2E(t *testing.T) {
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; studio column-order plan e2e skipped")
	}
	ctx := context.Background()
	admin, err := db.Connect(ctx, base)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	t.Cleanup(admin.Close)
	dbName := fmt.Sprintf("m08s_%d_%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	if err := admin.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %q`, dbName)); err != nil {
		t.Fatalf("create database %s: %v", dbName, err)
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := admin.Exec(cctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, dbName)); err != nil {
			t.Errorf("drop database %s: %v", dbName, err)
		}
	})
	client, err := db.Connect(ctx, deriveStudioDatabaseURL(t, base, dbName))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Exec(ctx, `CREATE TABLE u (id integer PRIMARY KEY, a text, b text)`); err != nil {
		t.Fatal(err)
	}

	// The state a push of (id, a, mid, b) leaves: mid appended.
	live, err := client.IntrospectV2(ctx)
	if err != nil {
		t.Fatal(err)
	}
	m, err := db.ModelFromRoot(live.Root)
	if err != nil {
		t.Fatal(err)
	}
	u := m.Table(db.V2Identity{Schema: "public", Name: "u"})
	u.Columns = []db.V2Column{u.Columns[0], u.Columns[1], {Name: "mid", Type: db.V2ColumnType{Name: "int4", Codec: "number"}}, u.Columns[2]}
	desired, err := documentFromModel(m)
	if err != nil {
		t.Fatal(err)
	}
	push, err := db.DiffV2Document(ctx, desired, live, db.DiffV2Options{})
	if err != nil {
		t.Fatalf("push plan: %v", err)
	}
	for _, s := range push.Up {
		if err := client.Exec(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	if again, err := db.DiffV2Document(ctx, desired, mustIntrospect(t, client), db.DiffV2Options{}); err != nil || len(again.Up) != 0 {
		t.Fatalf("the same document against the appended order must plan nothing: %v %q", err, again.Up)
	}

	plan, err := PlanSchemaChanges(ctx, client, []SchemaChange{{Op: "add-column", Schema: "public", Table: "u", Column: "c", Type: "int4"}})
	if err != nil {
		t.Fatalf("the designer must plan: %v", err)
	}
	if len(plan.Up) != 1 || !strings.Contains(plan.Up[0], `add column "c"`) {
		t.Fatalf("the designer plans only its change: %q", plan.Up)
	}
	for _, s := range plan.Up {
		if err := client.Exec(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	residual, err := verifyPlanConvergence(ctx, client, plan.desired)
	if err != nil || len(residual) != 0 {
		t.Fatalf("the designer plan must converge: %v %q", err, residual)
	}
}

func mustIntrospect(t *testing.T, client *db.Client) *db.V2Document {
	t.Helper()
	doc, err := client.IntrospectV2(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return doc
}
