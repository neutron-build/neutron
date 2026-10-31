package db

import (
	"context"
	"strings"
	"testing"
)

func TestIntrospectV2PreservesCatalogTypeIdentity(t *testing.T) {
	h := newQ07Harness(t, "typeidentity")
	h.exec(`CREATE SCHEMA custom`)
	h.exec(`CREATE DOMAIN custom.int4 AS pg_catalog.int4 CHECK (VALUE > 0)`)
	h.exec(`CREATE DOMAIN custom.numeric AS pg_catalog.numeric CHECK (VALUE > 0)`)
	h.exec(`CREATE DOMAIN custom.text AS pg_catalog.text CHECK (VALUE <> '')`)
	h.exec(`CREATE TYPE custom.uuid AS (part pg_catalog.text)`)
	h.exec(`CREATE TYPE custom.vector AS (part pg_catalog.int4)`)
	h.exec(`CREATE TYPE custom.bool AS ENUM ('on', 'off')`)
	for _, stmt := range []string{
		`CREATE TABLE public.domain_scalar (id pg_catalog.int4 PRIMARY KEY, value custom.int4)`,
		`CREATE TABLE public.domain_numeric (id pg_catalog.int4 PRIMARY KEY, value custom.numeric)`,
		`CREATE TABLE public.domain_array (id pg_catalog.int4 PRIMARY KEY, value custom.int4[])`,
		`CREATE TABLE public.composite_scalar (id pg_catalog.int4 PRIMARY KEY, value custom.uuid)`,
		`CREATE TABLE public.composite_vector (id pg_catalog.int4 PRIMARY KEY, value custom.vector)`,
		`CREATE TABLE public.real_builtin (id pg_catalog.int4 PRIMARY KEY, value pg_catalog.text, nums pg_catalog.int4[])`,
		`CREATE TABLE public.real_enum (id pg_catalog.int4 PRIMARY KEY, value custom.bool, many custom.bool[])`,
	} {
		h.exec(stmt)
	}
	h.exec(`INSERT INTO public.domain_scalar VALUES (1, 42)`)
	assertIdentity := func(client *Client) {
		t.Helper()
		doc, err := client.IntrospectV2(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		m, err := ModelFromRoot(doc.Root)
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"domain_scalar", "domain_numeric", "domain_array", "composite_scalar", "composite_vector"} {
			id := V2Identity{Schema: "public", Name: name}
			if m.Table(id) != nil {
				t.Fatalf("unsupported type flattened into managed table %s", name)
			}
			entry := m.OpaqueEntry("unsupported-table", id)
			if entry == nil || !strings.Contains(entry.Reason, "type identity") {
				t.Fatalf("missing truthful opaque inventory for %s: %+v", name, entry)
			}
		}
		table := m.Table(V2Identity{Schema: "public", Name: "real_builtin"})
		if table == nil || table.Columns[1].Type.Name != "text" || table.Columns[2].Type.Name != "int4" || !table.Columns[2].Type.Array {
			t.Fatalf("builtin contract changed: %+v", table)
		}
		enum := m.Table(V2Identity{Schema: "public", Name: "real_enum"})
		if enum == nil || enum.Columns[1].Type.Enum == nil || *enum.Columns[1].Type.Enum != (V2Identity{Schema: "custom", Name: "bool"}) || !enum.Columns[2].Type.Array {
			t.Fatalf("enum identity changed: %+v", enum)
		}
	}
	assertIdentity(h.client)
	// Explicitly placing pg_catalog last makes user-schema catalog lookalikes
	// shadow unqualified metadata queries. New connections inherit this path.
	h.exec(`CREATE TABLE custom.pg_namespace (nspname pg_catalog.text)`)
	h.exec(`INSERT INTO custom.pg_namespace VALUES ('fabricated')`)
	h.exec(`ALTER DATABASE "` + h.dbName + `" SET search_path TO custom, public, pg_catalog`)
	hostile, err := Connect(context.Background(), h.client.url)
	if err != nil {
		t.Fatal(err)
	}
	defer hostile.Close()
	assertIdentity(hostile)
	if h.queryOne(`SELECT value::pg_catalog.text FROM public.domain_scalar WHERE id=1`) != "42" {
		t.Fatal("read-only introspection changed database")
	}
}

func TestIntrospectV2TypeMapperRejectsLookalikes(t *testing.T) {
	for _, kind := range []string{"b", "d", "c"} {
		if _, reason := introspectV2ColumnType("value", "custom", "text", kind, "S", nil, nil, nil, nil, -1, false, false); reason == "" {
			t.Fatalf("custom text kind %s admitted as builtin", kind)
		}
	}
	if _, reason := introspectV2ColumnType("value", "custom", "vector", "b", "U", nil, nil, nil, nil, -1, false, false); reason == "" {
		t.Fatal("unowned vector lookalike admitted")
	}
	if _, reason := introspectV2ColumnType("value", "extension_schema", "vector", "b", "U", nil, nil, nil, nil, 3, true, false); reason != "" {
		t.Fatalf("actual vector extension identity refused: %s", reason)
	}
}
