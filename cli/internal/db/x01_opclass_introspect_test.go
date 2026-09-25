package db

// X01: a non-default operator class is representable — introspection
// captures the catalog's opcname on the key part (it no longer blocks the
// table), and the diff renders it back. Live: needs NEUTRON_E2E_DATABASE_URL.

import (
	"context"
	"strings"
	"testing"
)

func TestX01OpclassIntrospection(t *testing.T) {
	h := newQ07Harness(t, "x01opc")
	h.exec(`CREATE TABLE x01_opc (id int4 PRIMARY KEY, memo text)`)
	// Non-default opclass (text_pattern_ops), plus reloptions on a btree.
	h.exec(`CREATE INDEX x01_opc_pattern ON x01_opc (memo text_pattern_ops)`)
	h.exec(`CREATE INDEX x01_opc_fill ON x01_opc (id) WITH (fillfactor = 37)`)

	actual, err := h.client.IntrospectV2(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	am, err := ModelFromRoot(actual.Root)
	if err != nil {
		t.Fatal(err)
	}
	tbl := am.Table(V2Identity{Schema: "public", Name: "x01_opc"})
	if tbl == nil {
		t.Fatal("a table whose only specialty is a non-default opclass must be representable (X01)")
	}
	var pattern *V2Index
	for i := range tbl.Indexes {
		if tbl.Indexes[i].Identity.Name == "x01_opc_pattern" {
			pattern = &tbl.Indexes[i]
		}
	}
	if pattern == nil {
		t.Fatal("x01_opc_pattern must introspect")
	}
	if len(pattern.Key) != 1 || pattern.Key[0].Opclass == nil || *pattern.Key[0].Opclass != "text_pattern_ops" {
		t.Fatalf("opclass must be captured verbatim: %+v", pattern.Key)
	}
	var fill *V2Index
	for i := range tbl.Indexes {
		if tbl.Indexes[i].Identity.Name == "x01_opc_fill" {
			fill = &tbl.Indexes[i]
		}
	}
	if fill == nil {
		t.Fatal("x01_opc_fill must introspect")
	}
	if fill.With == nil || fill.With["fillfactor"] != 37 {
		t.Fatalf("integer reloptions must round-trip into with: %+v", fill.With)
	}

	// DDL rendering: the diff emits the opclass back.
	down, err := indexBodySQL(*fill)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(down, `"id"`) || !strings.Contains(down, "with (fillfactor = 37)") {
		// Integer reloptions render on ANY method (introspected btree
		// fillfactor round-trips; the server validates the keys).
		t.Fatalf("btree fillfactor must render: %s", down)
	}
	rendered, err := indexBodySQL(*pattern)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rendered, `"memo" text_pattern_ops`) {
		t.Fatalf("opclass must render on the key part: %s", rendered)
	}

	// Introspected document validates as v2 (canonical round-trip).
	if _, err := ParseV2Document(actual.Canonical); err != nil {
		t.Fatalf("introspected document must validate: %v", err)
	}
}
