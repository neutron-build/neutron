package nucleus

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// Collection isolation, proven against a real engine.
//
// `collection` was a parameter every one of these methods accepted and none of
// them sent (GO-055): Find, Update and Delete took it, addressed one global
// document store, and returned or mutated everything. A caller could believe
// tenants were separated while they shared a namespace. A mock cannot catch
// that — the SDK and the mock would be written from the same wrong belief, and
// the old code passes any test that does not ask a live server what it stored.
//
// Run with a live Nucleus:
//
//	NEUTRON_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:55599/nucleus \
//	    go test ./nucleus/ -run Collection -v

func testDocs(t *testing.T) (*DocumentModel, context.Context) {
	t.Helper()

	url := os.Getenv("NEUTRON_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("NEUTRON_TEST_DATABASE_URL not set; skipping database integration test")
	}
	ctx := context.Background()
	client, err := Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(client.Close)
	return client.Document(), ctx
}

// uniqueCollection keeps a run from colliding with documents an earlier run
// left behind. The test database is persistent, and asserting on a count is
// otherwise a test that passes once and then fails forever.
func uniqueCollection(t *testing.T, name string) string {
	t.Helper()
	return fmt.Sprintf("%s_%d", name, time.Now().UnixNano())
}

func TestCollectionIsolationIntegration(t *testing.T) {
	docs, ctx := testDocs(t)

	// Identical documents, different collections — so nothing but the
	// collection can distinguish them.
	tenantA := uniqueCollection(t, "tenant_a")
	tenantB := uniqueCollection(t, "tenant_b")

	body := map[string]any{"name": "shared", "kind": "isolation-probe"}
	idA, err := docs.Insert(ctx, tenantA, body)
	if err != nil {
		t.Fatalf("insert a: %v", err)
	}
	idB, err := docs.Insert(ctx, tenantB, body)
	if err != nil {
		t.Fatalf("insert b: %v", err)
	}
	t.Cleanup(func() {
		_, _ = docs.Delete(ctx, tenantA, body)
		_, _ = docs.Delete(ctx, tenantB, body)
	})

	// Find must return only the caller's collection.
	found, err := docs.Find(ctx, tenantA, body)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("Find in the caller's collection returned %d documents, want 1 — the other collection leaked", len(found))
	}

	// A direct read across the boundary reports absent, even holding the id.
	other, err := docs.GetIn(ctx, tenantA, idB)
	if err != nil {
		t.Fatalf("get across: %v", err)
	}
	if other != nil {
		t.Fatalf("GetIn read the other collection's document %d from tenant_a: %v", idB, other)
	}

	// Update must not reach across.
	n, err := docs.Update(ctx, tenantA, body, map[string]any{"touched": true})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if n != 1 {
		t.Fatalf("Update in the caller's collection changed %d documents, want 1", n)
	}
	stillB, err := docs.GetIn(ctx, tenantB, idB)
	if err != nil {
		t.Fatalf("get b: %v", err)
	}
	if _, touched := stillB["touched"]; touched {
		t.Fatalf("an update scoped to one collection modified another's document: %v", stillB)
	}

	// Delete must not reach across either.
	if _, err := docs.Delete(ctx, tenantA, map[string]any{"kind": "isolation-probe"}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	survivor, err := docs.GetIn(ctx, tenantB, idB)
	if err != nil {
		t.Fatalf("get b after delete: %v", err)
	}
	if survivor == nil {
		t.Fatalf("a delete scoped to one collection removed another's document %d", idB)
	}
	gone, err := docs.GetIn(ctx, tenantA, idA)
	if err != nil {
		t.Fatalf("get a after delete: %v", err)
	}
	if gone != nil {
		t.Fatalf("the delete did not remove its own collection's document %d", idA)
	}
}

func TestCollectionCountAndPathAreScopedIntegration(t *testing.T) {
	docs, ctx := testDocs(t)

	tenant := uniqueCollection(t, "tenant_count")
	other := uniqueCollection(t, "tenant_other")

	probe := map[string]any{"kind": "count-probe", "nested": map[string]any{"leaf": "value"}}
	id, err := docs.Insert(ctx, tenant, probe)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	t.Cleanup(func() { _, _ = docs.Delete(ctx, tenant, map[string]any{"kind": "count-probe"}) })

	n, err := docs.CountIn(ctx, tenant)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("CountIn(%s) = %d, want 1", tenant, n)
	}

	// A path read is scoped like every other verb — otherwise it would be the
	// one way to pull a field out of any collection's document by id.
	val, err := docs.PathIn(ctx, tenant, id, "nested", "leaf")
	if err != nil {
		t.Fatalf("path: %v", err)
	}
	if val == nil || *val != `"value"` {
		t.Fatalf("PathIn returned %v, want \"value\"", val)
	}
	cross, err := docs.PathIn(ctx, other, id, "nested", "leaf")
	if err != nil {
		t.Fatalf("path across: %v", err)
	}
	if cross != nil {
		t.Fatalf("PathIn read across a collection boundary: %v", *cross)
	}

	// No keys is refused locally rather than sent as a malformed statement.
	if _, err := docs.PathIn(ctx, tenant, id); err == nil {
		t.Fatal("PathIn with no keys must be refused")
	}
}
