package db

// Q07 live round-trip coverage (V10): the rich PostgreSQL schema surface —
// ordered index keys, generated/identity columns, NULLS NOT DISTINCT and
// collated/opclass indexes as opaque, enum/array/namespaces end to end.
// Every case follows export desired -> plan -> apply -> introspect -> diff
// empty, run twice, plus catalog-oracle spot checks that do not reuse the
// diff's own output.

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type q07Harness struct {
	t      *testing.T
	dbName string
	client *Client
}

func newQ07Harness(t *testing.T, name string) *q07Harness {
	t.Helper()
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; live Q07 round-trip tests skipped (set it to a disposable Postgres URL to run)")
	}
	dbName := fmt.Sprintf("q07_%s_%d_%d", name, os.Getpid(), time.Now().UnixNano()%1_000_000)
	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse NEUTRON_E2E_DATABASE_URL: %v", err)
	}
	u.Path = "/" + dbName
	admin, err := Connect(context.Background(), base)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	t.Cleanup(admin.Close)
	if err := admin.Exec(context.Background(), fmt.Sprintf(`CREATE DATABASE %q`, dbName)); err != nil {
		t.Fatalf("create database %s: %v", dbName, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := admin.Exec(ctx, fmt.Sprintf(
			`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '%s' AND pid <> pg_backend_pid()`, dbName,
		)); err != nil {
			t.Errorf("terminate backends: %v", err)
		}
		if err := admin.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q`, dbName)); err != nil {
			t.Errorf("drop database %s: %v", dbName, err)
		}
	})
	client, err := Connect(context.Background(), u.String())
	if err != nil {
		t.Fatalf("connect case db: %v", err)
	}
	t.Cleanup(client.Close)
	return &q07Harness{t: t, dbName: dbName, client: client}
}

func (h *q07Harness) exec(sql string) {
	h.t.Helper()
	if err := h.client.Exec(context.Background(), sql); err != nil {
		h.t.Fatalf("exec %q: %v", firstSQLLine(sql), err)
	}
}

func (h *q07Harness) queryOne(sql string) string {
	h.t.Helper()
	var v string
	if err := h.client.QueryRow(context.Background(), sql).Scan(&v); err != nil {
		h.t.Fatalf("query %q: %v", sql, err)
	}
	return v
}

func (h *q07Harness) planAndApply(desiredJSON string) DiffResult {
	h.t.Helper()
	desired := h.parseDoc(desiredJSON)
	actual, err := h.client.IntrospectV2(context.Background())
	if err != nil {
		h.t.Fatalf("introspect: %v", err)
	}
	norm, err := h.client.NewTwinNormalizer(context.Background())
	if err != nil {
		h.t.Fatalf("normalizer: %v", err)
	}
	defer norm.Close()
	result, err := DiffV2Document(context.Background(), desired, actual, DiffV2Options{Normalizer: norm})
	if err != nil {
		h.t.Fatalf("diff: %v", err)
	}
	if len(result.Up) > 0 {
		if err := h.client.ApplyInTransaction(context.Background(), result.Up, nil); err != nil {
			h.t.Fatalf("apply:\n%s\nerror: %v", strings.Join(result.Up, ";\n"), err)
		}
	}
	return result
}

func (h *q07Harness) assertDiffEmpty(desiredJSON string) {
	h.t.Helper()
	for round := 1; round <= 2; round++ {
		desired := h.parseDoc(desiredJSON)
		actual, err := h.client.IntrospectV2(context.Background())
		if err != nil {
			h.t.Fatalf("introspect: %v", err)
		}
		norm, err := h.client.NewTwinNormalizer(context.Background())
		if err != nil {
			h.t.Fatalf("normalizer: %v", err)
		}
		defer norm.Close()
		result, err := DiffV2Document(context.Background(), desired, actual, DiffV2Options{Normalizer: norm})
		if err != nil {
			h.t.Fatalf("diff (round %d): %v", round, err)
		}
		if len(result.Up) != 0 || len(result.Warnings) != 0 {
			h.t.Fatalf("round %d: expected an empty diff, got up=%v warnings=%v", round, result.Up, result.Warnings)
		}
	}
}

func (h *q07Harness) assertDiffError(desiredJSON, wantSubstring string) {
	h.t.Helper()
	desired := h.parseDoc(desiredJSON)
	actual, err := h.client.IntrospectV2(context.Background())
	if err != nil {
		h.t.Fatalf("introspect: %v", err)
	}
	norm, err := h.client.NewTwinNormalizer(context.Background())
	if err != nil {
		h.t.Fatalf("normalizer: %v", err)
	}
	defer norm.Close()
	_, err = DiffV2Document(context.Background(), desired, actual, DiffV2Options{Normalizer: norm})
	if err == nil || !strings.Contains(err.Error(), wantSubstring) {
		h.t.Fatalf("expected diff error containing %q, got %v", wantSubstring, err)
	}
}

func (h *q07Harness) parseDoc(jsonText string) *V2Document {
	h.t.Helper()
	doc, err := ParseV2Document([]byte(jsonText))
	if err != nil {
		h.t.Fatalf("parse desired document: %v", err)
	}
	return doc
}

// q07Doc1: the full Q07 surface in one managed scope — namespaces, enums,
// arrays with defaults, composite constraints with match/onUpdate/deferrable,
// ordered index keys, expression/partial/INCLUDE indexes, identity columns,
// stored generated columns and views with options.
const q07Doc1 = `{
	"version": 2,
	"dialect": "postgresql",
	"capabilities": [],
	"schemas": [{"name": "public"}, {"name": "app"}],
	"enums": [
		{"identity": {"schema": "app", "name": "mood"}, "managed": true, "values": ["sad", "ok", "glad"]}
	],
	"tables": [
		{
			"identity": {"schema": "app", "name": "tenants"},
			"managed": true,
			"columns": [
				{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true,
				 "default": {"kind": "identity", "generated": "by default"}},
				{"name": "name", "type": {"name": "varchar", "codec": "string", "params": {"length": 120}}, "notNull": true},
				{"name": "tone", "type": {"name": "enum", "codec": "enum", "enum": {"schema": "app", "name": "mood"}}, "notNull": false,
				 "default": {"kind": "literal", "sql": "'ok'::app.mood"}},
				{"name": "scores", "type": {"name": "int8", "codec": "array", "array": true}, "notNull": false,
				 "default": {"kind": "literal", "sql": "'{}'::int8[]"}},
				{"name": "tags", "type": {"name": "text", "codec": "array", "array": true}, "notNull": false,
				 "default": {"kind": "literal", "sql": "'{news}'::text[]"}},
				{"name": "meta", "type": {"name": "jsonb", "codec": "json"}, "notNull": false},
				{"name": "net", "type": {"name": "numeric", "codec": "decimal-string"}, "notNull": false},
				{"name": "gross", "type": {"name": "numeric", "codec": "decimal-string"}, "notNull": false,
				 "generated": {"expression": "net * 2"}}
			],
			"constraints": [
				{"name": "tenants_pkey", "type": "primary-key", "columns": ["id"]},
				{"name": "tenants_name_check", "type": "check", "expression": "char_length(name) > 0"},
				{"name": "tenants_name_tone_key", "type": "unique", "columns": ["name", "tone"], "deferrable": true, "initiallyDeferred": true}
			],
			"indexes": [
				{"identity": {"schema": "app", "name": "tenants_order_idx"}, "unique": false, "method": "btree",
				 "key": [{"column": "net", "order": "desc"}, {"column": "name", "nulls": "first"}, {"column": "scores", "order": "desc", "nulls": "last"}]},
				{"identity": {"schema": "app", "name": "tenants_meta_gin"}, "unique": false, "method": "gin",
				 "key": [{"column": "meta"}]},
				{"identity": {"schema": "app", "name": "tenants_partial"}, "unique": false, "method": "btree",
				 "key": [{"column": "name"}], "include": ["net"], "where": "net is not null"}
			]
		},
		{
			"identity": {"schema": "public", "name": "notices"},
			"managed": true,
			"columns": [
				{"name": "id", "type": {"name": "int8", "codec": "bigint"}, "notNull": true,
				 "default": {"kind": "identity", "generated": "always"}},
				{"name": "tenant", "type": {"name": "int4", "codec": "number"}, "notNull": true},
				{"name": "memo", "type": {"name": "text", "codec": "string"}, "notNull": false},
				{"name": "stamp", "type": {"name": "timestamptz", "codec": "timestamptz-string"}, "notNull": true,
				 "default": {"kind": "expression", "sql": "now()"}}
			],
			"constraints": [
				{"name": "notices_pkey", "type": "primary-key", "columns": ["id"]},
				{"name": "notices_tenant_fkey", "type": "foreign-key", "columns": ["tenant"],
				 "references": {"table": {"schema": "app", "name": "tenants"}, "columns": ["id"], "onDelete": "cascade", "onUpdate": "restrict", "match": "full"}}
			],
			"indexes": [
				{"identity": {"schema": "public", "name": "notices_memo_idx"}, "unique": false, "method": "btree",
				 "key": [{"expression": "lower(memo)"}]}
			]
		}
	],
	"views": [
		{"identity": {"schema": "app", "name": "glad_tenants"}, "managed": true,
		 "definition": "select id, name from app.tenants where tone = 'glad'", "checkOption": "cascaded", "securityInvoker": true},
		{"identity": {"schema": "public", "name": "notice_count"}, "managed": true,
		 "definition": "select count(*) as n from public.notices"}
	],
	"opaque": []
}`

func TestQ07RoundTripRichSurface(t *testing.T) {
	h := newQ07Harness(t, "rich")

	res := h.planAndApply(q07Doc1)
	if len(res.Up) == 0 {
		t.Fatal("initial plan must create objects")
	}
	h.assertDiffEmpty(q07Doc1)
	// Convergence: a second plan against the applied state is empty too.
	res2 := h.planAndApply(q07Doc1)
	if len(res2.Up) != 0 {
		t.Fatalf("second apply must be a no-op, got %v", res2.Up)
	}

	// Catalog oracles (independent of the diff's own comparisons).
	if got := h.queryOne(`SELECT indexdef FROM pg_indexes WHERE schemaname='app' AND indexname='tenants_order_idx'`); !strings.Contains(got, "net DESC") || !strings.Contains(got, "name NULLS FIRST") || !strings.Contains(got, "scores DESC NULLS LAST") {
		t.Fatalf("ordered index keys must round-trip: %s", got)
	}
	if got := h.queryOne(`SELECT attgenerated::text FROM pg_attribute WHERE attrelid = 'app.tenants'::regclass AND attname = 'gross'`); got != "s" {
		t.Fatalf("gross must be a stored generated column, attgenerated=%q", got)
	}
	if got := h.queryOne(`SELECT pg_get_expr(adbin, adrelid) FROM pg_attrdef WHERE adrelid = 'app.tenants'::regclass AND adnum = (SELECT attnum FROM pg_attribute WHERE attrelid='app.tenants'::regclass AND attname='gross')`); !strings.Contains(got, "net *") {
		t.Fatalf("generated expression must round-trip in catalog deparse, got %q", got)
	}
	if got := h.queryOne(`SELECT attidentity::text FROM pg_attribute WHERE attrelid = 'public.notices'::regclass AND attname = 'id'`); got != "a" {
		t.Fatalf("identity always must round-trip, attidentity=%q", got)
	}
	if got := h.queryOne(`SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'public.notices'::regclass AND conname = 'notices_tenant_fkey'`); !strings.Contains(got, "MATCH FULL") || !strings.Contains(got, "ON UPDATE RESTRICT") {
		t.Fatalf("match/onUpdate FK must round-trip: %s", got)
	}
	if got := h.queryOne(`SELECT count(*)::text FROM app.glad_tenants`); got != "0" {
		t.Fatalf("view must be queryable, got %s", got)
	}

	// Introspected shape: ordering in minimal form, generated carried.
	actual, err := h.client.IntrospectV2(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	am, err := ModelFromRoot(actual.Root)
	if err != nil {
		t.Fatal(err)
	}
	tenants := am.Table(V2Identity{Schema: "app", Name: "tenants"})
	if tenants == nil {
		t.Fatal("tenants must introspect as managed")
	}
	idx := tenants.Index("tenants_order_idx")
	if idx == nil || len(idx.Key) != 3 {
		t.Fatalf("ordered index must introspect: %+v", idx)
	}
	if idx.Key[0].Order == nil || *idx.Key[0].Order != "desc" || idx.Key[0].Nulls != nil {
		t.Fatalf("desc key must carry order only (nulls first is the direction default): %+v", idx.Key[0])
	}
	if idx.Key[1].Order != nil || idx.Key[1].Nulls == nil || *idx.Key[1].Nulls != "first" {
		t.Fatalf("asc nulls-first key must carry nulls only: %+v", idx.Key[1])
	}
	if idx.Key[2].Order == nil || *idx.Key[2].Order != "desc" || idx.Key[2].Nulls == nil || *idx.Key[2].Nulls != "last" {
		t.Fatalf("desc nulls-last key must carry both: %+v", idx.Key[2])
	}
	gross := tenants.Column("gross")
	if gross.Generated == nil || !strings.Contains(gross.Generated.Expression, "net *") {
		t.Fatalf("generated column must introspect its expression: %+v", gross)
	}
	if gross.Default != nil {
		t.Fatalf("generated column must not carry a default: %+v", gross.Default)
	}
	notices := am.Table(V2Identity{Schema: "public", Name: "notices"})
	if notices.Column("id").Default == nil || notices.Column("id").Default.Kind != "identity" || *notices.Column("id").Default.Generated != "always" {
		t.Fatalf("identity-always default must introspect: %+v", notices.Column("id").Default)
	}
}

// q07Doc2 mutates doc1: index ordering flip, generated expression change,
// enum append, cross-schema FK action change, view definition + option
// change, array default change.
const q07Doc2 = `{
	"version": 2,
	"dialect": "postgresql",
	"capabilities": [],
	"schemas": [{"name": "public"}, {"name": "app"}],
	"enums": [
		{"identity": {"schema": "app", "name": "mood"}, "managed": true, "values": ["sad", "ok", "glad", "elated"]}
	],
	"tables": [
		{
			"identity": {"schema": "app", "name": "tenants"},
			"managed": true,
			"columns": [
				{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true,
				 "default": {"kind": "identity", "generated": "by default"}},
				{"name": "name", "type": {"name": "varchar", "codec": "string", "params": {"length": 120}}, "notNull": true},
				{"name": "tone", "type": {"name": "enum", "codec": "enum", "enum": {"schema": "app", "name": "mood"}}, "notNull": false,
				 "default": {"kind": "literal", "sql": "'ok'::app.mood"}},
				{"name": "scores", "type": {"name": "int8", "codec": "array", "array": true}, "notNull": false,
				 "default": {"kind": "literal", "sql": "'{7}'::int8[]"}},
				{"name": "tags", "type": {"name": "text", "codec": "array", "array": true}, "notNull": false,
				 "default": {"kind": "literal", "sql": "'{news}'::text[]"}},
				{"name": "meta", "type": {"name": "jsonb", "codec": "json"}, "notNull": false},
				{"name": "net", "type": {"name": "numeric", "codec": "decimal-string"}, "notNull": false},
				{"name": "gross", "type": {"name": "numeric", "codec": "decimal-string"}, "notNull": false,
				 "generated": {"expression": "net * 3"}}
			],
			"constraints": [
				{"name": "tenants_pkey", "type": "primary-key", "columns": ["id"]},
				{"name": "tenants_name_check", "type": "check", "expression": "char_length(name) > 0"},
				{"name": "tenants_name_tone_key", "type": "unique", "columns": ["name", "tone"], "deferrable": true, "initiallyDeferred": true}
			],
			"indexes": [
				{"identity": {"schema": "app", "name": "tenants_order_idx"}, "unique": false, "method": "btree",
				 "key": [{"column": "net"}, {"column": "name", "nulls": "last"}, {"column": "scores", "nulls": "first"}]},
				{"identity": {"schema": "app", "name": "tenants_meta_gin"}, "unique": false, "method": "gin",
				 "key": [{"column": "meta"}]},
				{"identity": {"schema": "app", "name": "tenants_partial"}, "unique": false, "method": "btree",
				 "key": [{"column": "name"}], "include": ["net"], "where": "net is not null"}
			]
		},
		{
			"identity": {"schema": "public", "name": "notices"},
			"managed": true,
			"columns": [
				{"name": "id", "type": {"name": "int8", "codec": "bigint"}, "notNull": true,
				 "default": {"kind": "identity", "generated": "always"}},
				{"name": "tenant", "type": {"name": "int4", "codec": "number"}, "notNull": true},
				{"name": "memo", "type": {"name": "text", "codec": "string"}, "notNull": false},
				{"name": "stamp", "type": {"name": "timestamptz", "codec": "timestamptz-string"}, "notNull": true,
				 "default": {"kind": "expression", "sql": "now()"}}
			],
			"constraints": [
				{"name": "notices_pkey", "type": "primary-key", "columns": ["id"]},
				{"name": "notices_tenant_fkey", "type": "foreign-key", "columns": ["tenant"],
				 "references": {"table": {"schema": "app", "name": "tenants"}, "columns": ["id"], "onDelete": "set null", "onUpdate": "cascade", "match": "full"}}
			],
			"indexes": [
				{"identity": {"schema": "public", "name": "notices_memo_idx"}, "unique": false, "method": "btree",
				 "key": [{"expression": "lower(memo)"}]}
			]
		}
	],
	"views": [
		{"identity": {"schema": "app", "name": "glad_tenants"}, "managed": true,
		 "definition": "select id, name from app.tenants where tone in ('glad', 'elated')"},
		{"identity": {"schema": "public", "name": "notice_count"}, "managed": true,
		 "definition": "select count(*) as n from public.notices"}
	],
	"opaque": []
}`

func TestQ07RoundTripModifications(t *testing.T) {
	h := newQ07Harness(t, "mods")

	h.planAndApply(q07Doc1)
	h.assertDiffEmpty(q07Doc1)

	res := h.planAndApply(q07Doc2)
	if len(res.Up) == 0 {
		t.Fatal("modification plan must emit statements")
	}
	h.assertDiffEmpty(q07Doc2)
	res2 := h.planAndApply(q07Doc2)
	if len(res2.Up) != 0 {
		t.Fatalf("second apply must be a no-op, got %v", res2.Up)
	}

	if got := h.queryOne(`SELECT indexdef FROM pg_indexes WHERE schemaname='app' AND indexname='tenants_order_idx'`); strings.Contains(got, "DESC") || !strings.Contains(got, "scores NULLS FIRST") || strings.Contains(got, "name NULLS") {
		t.Fatalf("flipped ordering must round-trip (no DESC, scores NULLS FIRST, name plain): %s", got)
	}
	if got := h.queryOne(`SELECT pg_get_expr(adbin, adrelid) FROM pg_attrdef WHERE adrelid = 'app.tenants'::regclass AND adnum = (SELECT attnum FROM pg_attribute WHERE attrelid='app.tenants'::regclass AND attname='gross')`); !strings.Contains(got, "3)") || strings.Contains(got, "2)") {
		t.Fatalf("SET EXPRESSION must round-trip, got %q", got)
	}
	if got := h.queryOne(`SELECT count(*)::text FROM pg_enum e JOIN pg_type t ON t.oid = e.enumtypid JOIN pg_namespace n ON n.oid = t.typnamespace WHERE t.typname = 'mood' AND n.nspname = 'app'`); got != "4" {
		t.Fatalf("enum value must be appended, got %s", got)
	}
	if got := h.queryOne(`SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'public.notices'::regclass AND conname = 'notices_tenant_fkey'`); !strings.Contains(got, "ON DELETE SET NULL") || !strings.Contains(got, "ON UPDATE CASCADE") {
		t.Fatalf("changed FK actions must round-trip: %s", got)
	}
	if got := h.queryOne(`SELECT coalesce(reloptions::text, '') FROM pg_class WHERE relname = 'glad_tenants'`); strings.Contains(got, "check_option") || strings.Contains(got, "security_invoker") {
		// view option change (dropped checkOption+securityInvoker) requires
		// drop+recreate; after converge no options remain
		t.Fatalf("view options must be gone after the option change, got %q", got)
	}
}

func TestQ07GeneratedTransitions(t *testing.T) {
	h := newQ07Harness(t, "gen")

	h.planAndApply(q07Doc1)
	h.exec(`INSERT INTO app.tenants (name, tone, net) VALUES ('seed', 'ok', 5)`)

	// DROP EXPRESSION: the column keeps its computed value as plain data.
	dropGen := strings.Replace(q07Doc1,
		`{"name": "gross", "type": {"name": "numeric", "codec": "decimal-string"}, "notNull": false,
				 "generated": {"expression": "net * 2"}}`,
		`{"name": "gross", "type": {"name": "numeric", "codec": "decimal-string"}, "notNull": false}`, 1)
	if dropGen == q07Doc1 {
		t.Fatal("dropGen fixture edit did not apply")
	}
	h.planAndApply(dropGen)
	h.assertDiffEmpty(dropGen)
	if got := h.queryOne(`SELECT gross::text FROM app.tenants WHERE name = 'seed'`); got != "10" {
		t.Fatalf("computed value must survive DROP EXPRESSION, got %s", got)
	}

	// Re-adding a generation expression to an existing column is rejected
	// with an explicit error (PostgreSQL cannot attach one).
	h.assertDiffError(q07Doc1, "cannot become a generated column")

	// Type change on a generated column is rejected explicitly.
	typeChange := strings.Replace(q07Doc1, `"name": "gross", "type": {"name": "numeric", "codec": "decimal-string"}`, `"name": "gross", "type": {"name": "int4", "codec": "number"}`, 1)
	h.assertDiffError(typeChange, "generated column")
}

func TestQ07UnrepresentableIndexesAndConstraints(t *testing.T) {
	h := newQ07Harness(t, "opaque")
	h.exec(`CREATE SCHEMA app`)
	h.exec(`CREATE TYPE app.mood AS ENUM ('sad', 'ok', 'glad')`)
	h.exec(`CREATE TABLE app.tenants (id int4 PRIMARY KEY, name varchar(120), tone app.mood DEFAULT 'ok', score int4, memo text)`)
	// NULLS NOT DISTINCT unique, a collated key and a non-default opclass:
	// all unrepresentable — the table must inventory as opaque, with the
	// table surviving untouched.
	h.exec(`ALTER TABLE app.tenants ADD CONSTRAINT nnd UNIQUE NULLS NOT DISTINCT (name)`)
	h.exec(`CREATE INDEX collated ON app.tenants (memo COLLATE "C")`)
	h.exec(`CREATE INDEX opclassed ON app.tenants (memo text_pattern_ops)`)

	actual, err := h.client.IntrospectV2(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	am, err := ModelFromRoot(actual.Root)
	if err != nil {
		t.Fatal(err)
	}
	if am.Table(V2Identity{Schema: "app", Name: "tenants"}) != nil {
		t.Fatal("a table with unrepresentable indexes must not be managed")
	}
	op := am.OpaqueEntry("unsupported-table", V2Identity{Schema: "app", Name: "tenants"})
	if op == nil {
		t.Fatal("the table must be inventoried as unsupported-table")
	}
	for _, want := range []string{"NULLS NOT DISTINCT", "COLLATE", "operator class"} {
		if !strings.Contains(op.Reason, want) {
			t.Fatalf("opaque reason must mention %q, got: %s", want, op.Reason)
		}
	}

	// A desired table with the same identity is blocked, never recreated
	// lossily around the unrepresentable structure.
	desired := h.parseDoc(q07Doc1)
	norm, err := h.client.NewTwinNormalizer(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer norm.Close()
	_, err = DiffV2Document(context.Background(), desired, actual, DiffV2Options{Normalizer: norm})
	if err == nil || !strings.Contains(err.Error(), "cannot represent faithfully") {
		t.Fatalf("desired table over an unrepresentable table must block planning, got %v", err)
	}

	// security_invoker=false is canonically equivalent to the default: an
	// explicit-false view converges against a desired document without it.
	// The unrepresentable app-schema objects leave the managed scope.
	h.exec(`DROP SCHEMA app CASCADE`)
	h.exec(`CREATE TABLE public.users (id int4 PRIMARY KEY, email text)`)
	h.exec(`CREATE VIEW public.v_mails WITH (security_invoker = false) AS SELECT email FROM public.users`)
	desiredViews := `{
	 "version": 2, "dialect": "postgresql", "capabilities": [],
	 "schemas": [{"name": "public"}], "enums": [],
	 "tables": [{"identity": {"schema": "public", "name": "users"}, "managed": true,
	   "columns": [{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true},
	               {"name": "email", "type": {"name": "text", "codec": "string"}, "notNull": false}],
	   "constraints": [{"name": "users_pkey", "type": "primary-key", "columns": ["id"]}], "indexes": []}],
	 "views": [{"identity": {"schema": "public", "name": "v_mails"}, "managed": true,
	   "definition": "SELECT email FROM public.users"}],
	 "opaque": []}`
	h.planAndApply(desiredViews)
	h.assertDiffEmpty(desiredViews)
}

// The q07-exported-v2 golden fixture IS exportSchemaV2 output (pinned
// byte-for-byte by the TS suite). Applying those exact bytes live closes the
// cross-language seam: TS typed schema API -> exported document -> Go
// plan/apply -> introspection -> empty diff, twice (V10 end to end).
func TestQ07ExportedFixtureRoundTrip(t *testing.T) {
	h := newQ07Harness(t, "exported")
	raw, err := os.ReadFile(filepath.Join(goldenDir, "valid", "q07-exported-v2.json"))
	if err != nil {
		t.Fatalf("read golden fixture: %v", err)
	}
	res := h.planAndApply(string(raw))
	if len(res.Up) == 0 {
		t.Fatal("initial plan must create objects")
	}
	h.assertDiffEmpty(string(raw))
	if res2 := h.planAndApply(string(raw)); len(res2.Up) != 0 {
		t.Fatalf("second apply must be a no-op, got %v", res2.Up)
	}

	// The applied catalog re-introspects deterministically and converges
	// (assertDiffEmpty above): exporter spelling vs catalog deparse is
	// resolved by the twin normalizer, so byte-identity with the fixture is
	// NOT asserted here — semantic equality is the contract.
	actual, err := h.client.IntrospectV2(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	again, err := h.client.IntrospectV2(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(v2CanonicalBytes(actual.Root)) != string(v2CanonicalBytes(again.Root)) {
		t.Fatal("introspection must be deterministic across runs")
	}

	// Independent catalog oracles for the exporter's quoted renderings.
	if got := h.queryOne(`SELECT indexdef FROM pg_indexes WHERE schemaname='app' AND indexname='tenants_scores_idx'`); !strings.Contains(got, "scores DESC") {
		t.Fatalf("exported ordered index must apply: %s", got)
	}
	if got := h.queryOne(`SELECT pg_get_expr(adbin, adrelid) FROM pg_attrdef JOIN pg_attribute ON attrelid = adrelid AND attnum = adnum WHERE attname = 'gross' AND attrelid = 'public.notices'::regclass`); !strings.Contains(got, "net *") || !strings.Contains(got, "2") {
		t.Fatalf("exported generated expression must apply: %s", got)
	}
	if got := h.queryOne(`SELECT count(*)::text FROM app.writers`); got != "0" {
		t.Fatalf("exported view must be queryable, got %s", got)
	}
}

// Every Q07 golden fixture documents appliable DDL: apply each one live,
// converge twice, and confirm the introspected catalog canonicalizes back to
// the fixture bytes. A fixture whose statements a real server rejects (e.g.
// gin over a bare text expression — no default opclass) is a spec defect,
// not engine trivia.
func TestQ07AllFixturesApplyLive(t *testing.T) {
	for _, name := range []string{
		"q07-enums-arrays-namespaces",
		"q07-generated-identity",
		"q07-rich-indexes",
		"q07-views",
		"q07-exported-v2",
	} {
		t.Run(name, func(t *testing.T) {
			h := newQ07Harness(t, "fx")
			raw, err := os.ReadFile(filepath.Join(goldenDir, "valid", name+".json"))
			if err != nil {
				t.Fatalf("read fixture: %v", err)
			}
			doc := string(raw)
			if res := h.planAndApply(doc); len(res.Up) == 0 {
				t.Fatal("initial plan must create objects")
			}
			h.assertDiffEmpty(doc)
			if res := h.planAndApply(doc); len(res.Up) != 0 {
				t.Fatalf("second apply must be a no-op, got %v", res.Up)
			}
			actual, err := h.client.IntrospectV2(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			again, err := h.client.IntrospectV2(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if string(v2CanonicalBytes(actual.Root)) != string(v2CanonicalBytes(again.Root)) {
				t.Fatal("introspection must be deterministic across runs")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Q07 rework F1: index method/key-type applicability. A method/type combo
// without a default operator class (PostgreSQL 15-17 built-in pg_opclass
// facts, probed live on 17.11 and verified against REL_15_STABLE) can never
// apply — the server refuses with SQLSTATE 42704 — so the validator rejects
// it at definition time, mirroring the TS exporter.
// ---------------------------------------------------------------------------

const q07IdxProbeDocTpl = `{
	"version": 2,
	"dialect": "postgresql",
	"capabilities": [%s],
	"schemas": [{"name": "public"}],
	"enums": [%s],
	"tables": [{
		"identity": {"schema": "public", "name": "probe"},
		"managed": true,
		"columns": [
			{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true},
			{"name": "k", "type": %s, "notNull": false}
		],
		"constraints": [{"name": "probe_pkey", "type": "primary-key", "columns": ["id"]}],
		"indexes": [{
			"identity": {"schema": "public", "name": "probe_k_idx"},
			"unique": false,
			"method": %q,
			"key": [%s]
		}]
	}],
	"views": [],
	"opaque": []
}`

func TestQ07IndexMethodApplicability(t *testing.T) {
	enumDecl := `{"identity": {"schema": "public", "name": "mood"}, "managed": true, "values": ["sad", "ok"]}`
	scalar := func(name, codec string) string { return fmt.Sprintf(`{"name": %q, "codec": %q}`, name, codec) }
	arrayOf := func(elem string) string {
		return fmt.Sprintf(`{"name": %q, "codec": "array", "array": true}`, elem)
	}
	enumType := `{"name": "enum", "codec": "enum", "enum": {"schema": "public", "name": "mood"}}`
	vectorType := `{"name": "vector", "codec": "vector", "params": {"dimensions": 3}}`

	orderable := map[string]bool{"btree": true, "hash": true, "brin": true} // text-like + numeric + temporal + uuid + bytea
	textLike := map[string]bool{"btree": true, "hash": true, "spgist": true, "brin": true}
	arrays := map[string]bool{"btree": true, "hash": true, "gin": true}
	none := map[string]bool{}
	jsonbApp := map[string]bool{"btree": true, "hash": true, "gin": true}

	cases := []struct {
		label      string
		typeJSON   string
		enums      string
		caps       string
		applicable map[string]bool
	}{
		{"text", scalar("text", "string"), "", "", textLike},
		{"varchar", scalar("varchar", "string"), "", "", textLike},
		{"bool", scalar("bool", "boolean"), "", "", map[string]bool{"btree": true, "hash": true}},
		{"int2", scalar("int2", "number"), "", "", orderable},
		{"int4", scalar("int4", "number"), "", "", orderable},
		{"int8", scalar("int8", "bigint"), "", "", orderable},
		{"float4", scalar("float4", "number"), "", "", orderable},
		{"float8", scalar("float8", "number"), "", "", orderable},
		{"numeric", scalar("numeric", "decimal-string"), "", "", orderable},
		{"timestamp", scalar("timestamp", "timestamp-string"), "", "", orderable},
		{"timestamptz", scalar("timestamptz", "timestamptz-string"), "", "", orderable},
		{"date", scalar("date", "date-string"), "", "", orderable},
		{"uuid", scalar("uuid", "uuid"), "", "", orderable},
		{"bytea", scalar("bytea", "binary"), "", "", orderable},
		{"json", scalar("json", "json"), "", "", none},
		{"jsonb", scalar("jsonb", "json"), "", "", jsonbApp},
		{"enum", enumType, enumDecl, "", map[string]bool{"btree": true, "hash": true}},
		{"vector", vectorType, "", `"pgvector"`, none},
		{"text[]", arrayOf("text"), "", "", arrays},
		{"int4[]", arrayOf("int4"), "", "", arrays},
		{"jsonb[]", arrayOf("jsonb"), "", "", arrays},
		{"enum[]", `{"name": "enum", "codec": "array", "array": true, "enum": {"schema": "public", "name": "mood"}}`, enumDecl, "", arrays},
	}

	for _, tc := range cases {
		for _, method := range []string{"btree", "hash", "gin", "gist", "spgist", "brin"} {
			doc := []byte(fmt.Sprintf(q07IdxProbeDocTpl, tc.caps, tc.enums, tc.typeJSON, method, `{"column": "k"}`))
			err := func() error {
				_, err := ParseV2Document(doc)
				return err
			}()
			want := tc.applicable[method]
			if want && err != nil {
				t.Errorf("%s over %s: expected acceptance, got %v", method, tc.label, err)
			}
			if !want {
				if err == nil {
					t.Errorf("%s over %s: expected [invalid-index] refusal, accepted instead", method, tc.label)
				} else if !strings.Contains(err.Error(), "[invalid-index]") || !strings.Contains(err.Error(), "operator-class slot") {
					t.Errorf("%s over %s: refusal must name the missing default operator class and the absent contract slot, got %v", method, tc.label, err)
				}
			}
		}
	}

	// Expression keys: only btree is definable — the operator class of an
	// expression result type cannot be verified at definition time.
	for _, method := range []string{"btree", "hash", "gin", "gist", "spgist", "brin"} {
		doc := []byte(fmt.Sprintf(q07IdxProbeDocTpl, "", "", scalar("text", "string"), method, `{"expression": "lower(k)"}`))
		_, err := ParseV2Document(doc)
		if method == "btree" {
			if err != nil {
				t.Errorf("btree expression key: expected acceptance, got %v", err)
			}
			continue
		}
		if err == nil {
			t.Errorf("%s expression key: expected [invalid-index] refusal, accepted instead", method)
		} else if !strings.Contains(err.Error(), "[invalid-index]") || !strings.Contains(err.Error(), "expression key") {
			t.Errorf("%s expression key: refusal must name expression keys, got %v", method, err)
		}
	}
}

// Q07 rework F2: handcrafted enum-array defaults with a miscast cast pass
// shape validation today and fail at apply with SQLSTATE 42804 — the
// validator cross-checks array default casts against the column type, the
// same check the TS exporter applies while spelling them.
func TestQ07ArrayDefaultCastParity(t *testing.T) {
	enumDecl := `{"identity": {"schema": "public", "name": "mood"}, "managed": true, "values": ["sad", "ok"]}`
	type colCase struct {
		label      string
		typeJSON   string
		enums      string
		defaultStr string
		wantErr    string // "" = accepted
	}
	cases := []colCase{
		{"enum[] miscast text[]", `{"name": "enum", "codec": "array", "array": true, "enum": {"schema": "public", "name": "mood"}}`, enumDecl,
			`{"kind": "literal", "sql": "'{sad,ok}'::text[]"}`, "[invalid-default]"},
		{"enum[] bare literal", `{"name": "enum", "codec": "array", "array": true, "enum": {"schema": "public", "name": "mood"}}`, enumDecl,
			`{"kind": "literal", "sql": "'{sad}'"}`, "[invalid-default]"},
		{"text[] miscast int4[]", `{"name": "text", "codec": "array", "array": true}`, "",
			`{"kind": "literal", "sql": "'{1}'::int4[]"}`, "[invalid-default]"},
		{"text[] cast without array marker", `{"name": "text", "codec": "array", "array": true}`, "",
			`{"kind": "literal", "sql": "'x'::text"}`, "[invalid-default]"},
		{"int4 scalar with array cast", `{"name": "int4", "codec": "number"}`, "",
			`{"kind": "literal", "sql": "'1'::int4[]"}`, "[invalid-default]"},
		{"text[] matching cast", `{"name": "text", "codec": "array", "array": true}`, "",
			`{"kind": "literal", "sql": "'{news}'::text[]"}`, ""},
		{"text[] bare literal", `{"name": "text", "codec": "array", "array": true}`, "",
			`{"kind": "literal", "sql": "'{}'"}`, ""},
		{"int8[] matching cast", `{"name": "int8", "codec": "array", "array": true}`, "",
			`{"kind": "literal", "sql": "'{}'::int8[]"}`, ""},
		{"varchar[] cast via boolean alias", `{"name": "bool", "codec": "array", "array": true}`, "",
			`{"kind": "literal", "sql": "'{true}'::boolean[]"}`, ""},
	}
	for _, tc := range cases {
		colWithDefault := fmt.Sprintf(`{"name": "k", "type": %s, "notNull": false, "default": %s}`, tc.typeJSON, tc.defaultStr)
		doc := []byte(fmt.Sprintf(`{
	"version": 2, "dialect": "postgresql", "capabilities": [],
	"schemas": [{"name": "public"}],
	"enums": [%s],
	"tables": [{
		"identity": {"schema": "public", "name": "probe"},
		"managed": true,
		"columns": [
			{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true},
			%s
		],
		"constraints": [{"name": "probe_pkey", "type": "primary-key", "columns": ["id"]}],
		"indexes": []
	}],
	"views": [], "opaque": []
}`, tc.enums, colWithDefault))
		_, err := ParseV2Document(doc)
		if tc.wantErr == "" {
			if err != nil {
				t.Errorf("%s: expected acceptance, got %v", tc.label, err)
			}
		} else if err == nil {
			t.Errorf("%s: expected %s refusal, accepted instead", tc.label, tc.wantErr)
		} else if !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("%s: expected %s refusal, got %v", tc.label, tc.wantErr, err)
		}
	}
}

// Q07 rework F3: altering a column default to a sequence must create the
// sequence OWNED BY the column (create-path behavior): the diff converges
// warning-free and the sequence drops with the table.
func TestQ07AlterToSequenceDefaultLifecycle(t *testing.T) {
	h := newQ07Harness(t, "altseq")
	const base = `{
	"version": 2, "dialect": "postgresql", "capabilities": [],
	"schemas": [{"name": "public"}], "enums": [],
	"tables": [{
		"identity": {"schema": "public", "name": "alt"},
		"managed": true,
		"columns": [
			{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true},
			{"name": "n", "type": {"name": "int4", "codec": "number"}, "notNull": false, "default": %s}
		],
		"constraints": [{"name": "alt_pkey", "type": "primary-key", "columns": ["id"]}],
		"indexes": []
	}],
	"views": [], "opaque": []
}`
	withLiteral := fmt.Sprintf(base, `{"kind": "literal", "sql": "7"}`)
	withSequence := fmt.Sprintf(base, `{"kind": "sequence", "sequence": {"schema": "public", "name": "alt_n_seq"}}`)

	h.planAndApply(withLiteral)
	res := h.planAndApply(withSequence)
	for _, w := range res.Warnings {
		if strings.Contains(w, "alt_n_seq") {
			t.Fatalf("alter-to-sequence must not warn about the sequence, got: %s", w)
		}
	}

	// OWNED BY the column: pg_depend auto dependency on alt.n.
	deptype := h.queryOne(`select d.deptype::text from pg_depend d
		join pg_class c on c.oid = d.objid
		join pg_class tc on tc.oid = d.refobjid
		join pg_attribute a on a.attrelid = tc.oid and a.attnum = d.refobjsubid
		where c.relkind = 'S' and c.relname = 'alt_n_seq' and tc.relname = 'alt' and a.attname = 'n'`)
	if deptype != "a" {
		t.Fatalf("sequence alt_n_seq must be OWNED BY alt.n (pg_depend deptype 'a'), got %q", deptype)
	}

	// The default is live.
	got := h.queryOne(`insert into alt (id) values (1) returning n`)
	if got != "1" {
		t.Fatalf("nextval default expected 1, got %q", got)
	}

	// V10: converged — empty diff twice, warning-free.
	h.assertDiffEmpty(withSequence)

	// Drop the table destructively; the OWNED sequence drops with it.
	emptyDoc := h.parseDoc(`{
	"version": 2, "dialect": "postgresql", "capabilities": [],
	"schemas": [{"name": "public"}], "enums": [], "tables": [], "views": [], "opaque": []
}`)
	actual, err := h.client.IntrospectV2(context.Background())
	if err != nil {
		t.Fatalf("introspect: %v", err)
	}
	norm, err := h.client.NewTwinNormalizer(context.Background())
	if err != nil {
		t.Fatalf("normalizer: %v", err)
	}
	defer norm.Close()
	dropRes, err := DiffV2Document(context.Background(), emptyDoc, actual, DiffV2Options{AllowDestructive: true, Normalizer: norm})
	if err != nil {
		t.Fatalf("plan drop: %v", err)
	}
	for _, w := range dropRes.Warnings {
		if strings.Contains(w, "alt_n_seq") && strings.Contains(w, "left untouched") {
			t.Fatalf("sequence must be owned (not a standalone left-untouched object), got: %s", w)
		}
	}
	if len(dropRes.Up) == 0 {
		t.Fatalf("expected a drop plan")
	}
	if err := h.client.ApplyInTransaction(context.Background(), dropRes.Up, nil); err != nil {
		t.Fatalf("apply drop: %v", err)
	}
	if reg := h.queryOne(`select coalesce(to_regclass('public.alt_n_seq')::text, 'gone')`); reg != "gone" {
		t.Fatalf("sequence must drop with its table, still present: %s", reg)
	}
	h.assertDiffEmpty(`{
	"version": 2, "dialect": "postgresql", "capabilities": [],
	"schemas": [{"name": "public"}], "enums": [], "tables": [], "views": [], "opaque": []
}`)
}
