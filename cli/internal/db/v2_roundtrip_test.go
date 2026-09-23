package db

// Live round-trip tests for M02 (V10: introspect/apply/diff). Skipped
// unless NEUTRON_E2E_DATABASE_URL points at a disposable Postgres server
// (or NEUTRON_LIVE_REQUIRED=1 makes a missing URL a failure). Every case
// owns a uniquely-named m02_* database and drops it afterwards; nothing
// touches other databases.
//
// The oracle discipline: desired documents are hand-written v2 JSON with
// deliberately non-deparse spellings ("slug <> ''" vs the catalog's
// "((slug)::text <> ''::text)", 'x' vs 'x'::varchar, CURRENT_TIMESTAMP vs
// now()), and emptiness is asserted through the real introspection + diff
// pipeline twice in a row (perpetual-diff detector). Database state is
// additionally verified with raw catalog queries, never by re-running the
// implementation.

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

type m02Harness struct {
	t      *testing.T
	dbName string
	dbURL  string
	admin  *Client
	client *Client
}

func newM02Harness(t *testing.T, name string) *m02Harness {
	t.Helper()
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; live M02 round-trip tests skipped (set it to a disposable Postgres URL to run)")
	}
	dbName := fmt.Sprintf("m02_%s_%d_%d", name, os.Getpid(), time.Now().UnixNano()%1_000_000)
	dbURL := deriveM02URL(t, base, dbName)
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
	client, err := Connect(context.Background(), dbURL)
	if err != nil {
		t.Fatalf("connect case db: %v", err)
	}
	t.Cleanup(client.Close)
	return &m02Harness{t: t, dbName: dbName, dbURL: dbURL, admin: admin, client: client}
}

func deriveM02URL(t *testing.T, base, dbName string) string {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse NEUTRON_E2E_DATABASE_URL: %v", err)
	}
	u.Path = "/" + dbName
	return u.String()
}

func (h *m02Harness) exec(sql string) {
	h.t.Helper()
	if err := h.client.Exec(context.Background(), sql); err != nil {
		h.t.Fatalf("exec %q: %v", firstSQLLine(sql), err)
	}
}

func (h *m02Harness) queryOne(sql string) string {
	h.t.Helper()
	var v string
	if err := h.client.QueryRow(context.Background(), sql).Scan(&v); err != nil {
		h.t.Fatalf("query %q: %v", sql, err)
	}
	return v
}

// planAndApply diffs desired against the live database (with the twin
// normalizer, exactly like db push) and applies the result atomically.
func (h *m02Harness) planAndApply(desiredJSON string, renames map[string]string, allowDestructive bool) DiffResult {
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
	result, err := DiffV2Document(context.Background(), desired, actual, DiffV2Options{
		Renames:          renames,
		AllowDestructive: allowDestructive,
		Normalizer:       norm,
	})
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

// assertDiffEmpty asserts that desired is already satisfied: the diff
// against the live database produces no statements and no error. Running it
// twice in a row catches perpetual diffs.
func (h *m02Harness) assertDiffEmpty(desiredJSON string, renames map[string]string, allowDestructive bool) {
	h.t.Helper()
	for round := 1; round <= 2; round++ {
		desired := h.parseDoc(desiredJSON)
		actual, err := h.client.IntrospectV2(context.Background())
		if err != nil {
			h.t.Fatalf("introspect (round %d): %v", round, err)
		}
		norm, err := h.client.NewTwinNormalizer(context.Background())
		if err != nil {
			h.t.Fatalf("normalizer: %v", err)
		}
		result, err := DiffV2Document(context.Background(), desired, actual, DiffV2Options{
			Renames:          renames,
			AllowDestructive: allowDestructive,
			Normalizer:       norm,
		})
		norm.Close()
		if err != nil {
			h.t.Fatalf("diff (round %d): %v", round, err)
		}
		if len(result.Up) != 0 {
			h.t.Fatalf("round %d: diff must be empty, got:\n%s\n(warnings: %v)", round, strings.Join(result.Up, ";\n"), result.Warnings)
		}
	}
}

func (h *m02Harness) assertDiffError(desiredJSON, wantSubstring string, renames map[string]string) {
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
	_, err = DiffV2Document(context.Background(), desired, actual, DiffV2Options{
		AllowDestructive: true,
		Normalizer:       norm,
		Renames:          renames,
	})
	if err == nil || !strings.Contains(err.Error(), wantSubstring) {
		h.t.Fatalf("expected error containing %q, got: %v", wantSubstring, err)
	}
}

func (h *m02Harness) parseDoc(jsonText string) *V2Document {
	h.t.Helper()
	doc, err := ParseV2Document([]byte(jsonText))
	if err != nil {
		h.t.Fatalf("desired document must be contract-valid: %v", err)
	}
	return doc
}

// ---------------------------------------------------------------------------
// V10 fixture documents. Spellings are deliberately NOT the catalog's
// deparse form: 'x' vs 'x'::varchar, slug <> '' vs ((slug)::text <> ''::text),
// CURRENT_TIMESTAMP vs now(), lowercase view text vs pg_get_viewdef's
// pretty-printed definition.
// ---------------------------------------------------------------------------

const m02Doc1 = `{
	"version": 2,
	"dialect": "postgresql",
	"capabilities": [],
	"schemas": [{"name": "public"}, {"name": "app"}],
	"tables": [
		{
			"identity": {"schema": "public", "name": "users"},
			"managed": true,
			"columns": [
				{"name": "id", "type": {"name": "int8", "codec": "bigint"}, "notNull": true,
				 "default": {"kind": "identity", "generated": "always"}},
				{"name": "email", "type": {"name": "varchar", "codec": "string", "params": {"length": 80}}, "notNull": true,
				 "default": {"kind": "literal", "sql": "'x'"}},
				{"name": "role", "type": {"name": "enum", "codec": "enum", "enum": {"schema": "public", "name": "mood"}}, "notNull": true,
				 "default": {"kind": "literal", "sql": "'ok'"}},
				{"name": "tags", "type": {"name": "text", "codec": "array", "array": true}, "notNull": true,
				 "default": {"kind": "literal", "sql": "'{}'"}},
				{"name": "score", "type": {"name": "numeric", "codec": "decimal-string", "params": {"precision": 8, "scale": 2}}, "notNull": true,
				 "default": {"kind": "literal", "sql": "0"}},
				{"name": "seen_at", "type": {"name": "timestamptz", "codec": "timestamptz-string"}, "notNull": true,
				 "default": {"kind": "expression", "sql": "CURRENT_TIMESTAMP"}},
				{"name": "seq_num", "type": {"name": "int4", "codec": "number"}, "notNull": false,
				 "default": {"kind": "sequence", "sequence": {"schema": "public", "name": "m02_seq"}}}
			],
			"constraints": [
				{"name": "users_pkey", "type": "primary-key", "columns": ["id"]},
				{"name": "users_id_email_key", "type": "unique", "columns": ["id", "email"]}
			],
			"indexes": []
		},
		{
			"identity": {"schema": "public", "name": "posts"},
			"managed": true,
			"columns": [
				{"name": "author_id", "type": {"name": "int8", "codec": "bigint"}, "notNull": true},
				{"name": "author_email", "type": {"name": "varchar", "codec": "string", "params": {"length": 80}}, "notNull": true},
				{"name": "slug", "type": {"name": "varchar", "codec": "string", "params": {"length": 120}}, "notNull": true},
				{"name": "body", "type": {"name": "text", "codec": "string"}, "notNull": true}
			],
			"constraints": [
				{"name": "posts_pk", "type": "primary-key", "columns": ["author_id", "slug"]},
				{"name": "posts_slug_check", "type": "check", "expression": "slug <> ''"},
				{"name": "posts_author_fkey", "type": "foreign-key", "columns": ["author_id", "author_email"],
				 "references": {"table": {"schema": "public", "name": "users"}, "columns": ["id", "email"], "onDelete": "cascade"},
				 "deferrable": true, "initiallyDeferred": true}
			],
			"indexes": [
				{"identity": {"schema": "public", "name": "posts_body_idx"}, "unique": false, "method": "hash",
				 "key": [{"column": "body"}]},
				{"identity": {"schema": "public", "name": "posts_active_idx"}, "unique": false, "method": "btree",
				 "key": [{"column": "author_id"}], "include": ["body"], "where": "slug <> ''"},
				{"identity": {"schema": "public", "name": "posts_slug_lower_idx"}, "unique": false, "method": "btree",
				 "key": [{"expression": "lower(slug)"}]}
			]
		},
		{
			"identity": {"schema": "app", "name": "comments"},
			"managed": true,
			"columns": [
				{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true,
				 "default": {"kind": "identity", "generated": "by default"}},
				{"name": "post_author", "type": {"name": "int8", "codec": "bigint"}, "notNull": true},
				{"name": "post_slug", "type": {"name": "varchar", "codec": "string", "params": {"length": 120}}, "notNull": true},
				{"name": "moods", "type": {"name": "enum", "codec": "array", "array": true, "enum": {"schema": "public", "name": "mood"}}, "notNull": true,
				 "default": {"kind": "literal", "sql": "'{}'"}}
			],
			"constraints": [
				{"name": "comments_pkey", "type": "primary-key", "columns": ["id"]},
				{"name": "comments_post_fkey", "type": "foreign-key", "columns": ["post_author", "post_slug"],
				 "references": {"table": {"schema": "public", "name": "posts"}, "columns": ["author_id", "slug"], "onDelete": "cascade"}}
			],
			"indexes": []
		}
	],
	"enums": [
		{"identity": {"schema": "public", "name": "mood"}, "managed": true, "values": ["sad", "ok", "happy"]}
	],
	"views": [
		{"identity": {"schema": "public", "name": "v_active_posts"}, "managed": true,
		 "definition": "select author_id, slug from posts where slug <> ''"},
		{"identity": {"schema": "public", "name": "v_checked"}, "managed": true, "checkOption": "cascaded", "securityInvoker": true,
		 "definition": "select id, email from users where id > 0"}
	],
	"opaque": []
}`

// TestV2RoundTripCreateAndConverge is the primary V10 exit: desired (with
// non-deparse spellings) -> plan -> apply -> introspect -> diff empty,
// repeated twice; plus independent catalog assertions.
func TestV2RoundTripCreateAndConverge(t *testing.T) {
	h := newM02Harness(t, "rt1")
	h.exec(`CREATE SEQUENCE public.m02_seq`)

	res := h.planAndApply(m02Doc1, nil, false)
	if len(res.Up) == 0 {
		t.Fatal("initial plan must create objects")
	}
	h.assertDiffEmpty(m02Doc1, nil, false)

	// Independent catalog oracle: the database really holds what the
	// document describes, in the shapes the contract claims.
	if got := h.queryOne(`SELECT atttypmod::text FROM pg_attribute WHERE attrelid = 'public.users'::regclass AND attname = 'email'`); got != "84" {
		t.Fatalf("varchar(80) must survive: atttypmod=%s", got)
	}
	if got := h.queryOne(`SELECT pg_get_expr(adbin, adrelid) FROM pg_attrdef WHERE adrelid = 'public.users'::regclass AND adnum = (SELECT attnum FROM pg_attribute WHERE attrelid='public.users'::regclass AND attname='seen_at')`); got != "CURRENT_TIMESTAMP" {
		t.Fatalf("the expression default must round-trip in the catalog's deparse spelling, got %q", got)
	}
	if got := h.queryOne(`SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'public.posts'::regclass AND conname = 'posts_author_fkey'`); !strings.Contains(got, "FOREIGN KEY (author_id, author_email) REFERENCES users(id, email) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED") {
		t.Fatalf("composite deferrable FK must round-trip: %s", got)
	}
	if got := h.queryOne(`SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'app.comments'::regclass AND conname = 'comments_post_fkey'`); !strings.Contains(got, "FOREIGN KEY (post_author, post_slug) REFERENCES posts(author_id, slug) ON DELETE CASCADE") {
		t.Fatalf("cross-schema composite FK must round-trip: %s", got)
	}
	if got := h.queryOne(`SELECT indexdef FROM pg_indexes WHERE schemaname='public' AND indexname='posts_active_idx'`); !strings.Contains(got, "WHERE") || !strings.Contains(got, "INCLUDE") {
		t.Fatalf("partial INCLUDE index must round-trip: %s", got)
	}
	if got := h.queryOne(`SELECT count(*)::text FROM pg_enum e JOIN pg_type t ON t.oid = e.enumtypid WHERE t.typname = 'mood'`); got != "3" {
		t.Fatalf("enum must have 3 values: %s", got)
	}
	if got := h.queryOne(`SELECT count(*)::text FROM pg_class WHERE relname = 'v_active_posts' AND relkind = 'v'`); got != "1" {
		t.Fatalf("view must exist")
	}
	if got := h.queryOne(`SELECT reloptions::text FROM pg_class WHERE relname = 'v_checked'`); !strings.Contains(got, "check_option=cascaded") || !strings.Contains(got, "security_invoker=true") {
		t.Fatalf("view options must round-trip: %s", got)
	}

	// Introspected document carries the exact contract shape (spot checks
	// against hand-expected values, not against the diff's own output).
	actual, err := h.client.IntrospectV2(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	am, err := ModelFromRoot(actual.Root)
	if err != nil {
		t.Fatal(err)
	}
	// The schema qualification lives in the introspected document, where
	// the contract models it structurally.
	if cm := am.Table(V2Identity{Schema: "app", Name: "comments"}); cm == nil {
		t.Fatal("comments must introspect")
	} else {
		fk := cm.Constraint("comments_post_fkey")
		if fk == nil || fk.References == nil || fk.References.Table != (V2Identity{Schema: "public", Name: "posts"}) {
			t.Fatalf("cross-schema FK target must carry its qualified identity: %+v", fk)
		}
	}
	users := am.Table(V2Identity{Schema: "public", Name: "users"})
	if users == nil {
		t.Fatal("users must be introspected as a managed table")
	}
	var colOrder []string
	for _, c := range users.Columns {
		colOrder = append(colOrder, c.Name)
	}
	if strings.Join(colOrder, ",") != "id,email,role,tags,score,seen_at,seq_num" {
		t.Fatalf("columns must be the attnum-ordered tuple, got %v", colOrder)
	}
	if users.Column("id").Default == nil || users.Column("id").Default.Kind != "identity" || *users.Column("id").Default.Generated != "always" {
		t.Fatalf("identity default must introspect: %+v", users.Column("id").Default)
	}
	if users.Column("seq_num").Default == nil || users.Column("seq_num").Default.Kind != "sequence" ||
		*users.Column("seq_num").Default.Sequence != (V2Identity{Schema: "public", Name: "m02_seq"}) {
		t.Fatalf("sequence default must introspect: %+v", users.Column("seq_num").Default)
	}
	if users.Column("tags").Type.Name != "text" || !users.Column("tags").Type.Array || users.Column("tags").Type.Codec != "array" {
		t.Fatalf("array type must introspect: %+v", users.Column("tags").Type)
	}
	comments := am.Table(V2Identity{Schema: "app", Name: "comments"})
	if comments == nil || comments.Column("moods").Type.Enum == nil || *comments.Column("moods").Type.Enum != (V2Identity{Schema: "public", Name: "mood"}) || !comments.Column("moods").Type.Array {
		t.Fatalf("enum array must introspect: %+v", comments)
	}
	if len(am.Views) != 2 || am.Views[0].CheckOption == nil && am.Views[1].CheckOption == nil {
		t.Fatalf("views must introspect with options: %+v", am.Views)
	}
}

// m02Doc2 modifies doc1: FK action cascade->restrict, same-name index
// predicate change, default change, nullability change, enum append, view
// definition change, varchar length change, new column with FK.
const m02Doc2 = `{
	"version": 2,
	"dialect": "postgresql",
	"capabilities": [],
	"schemas": [{"name": "public"}, {"name": "app"}],
	"tables": [
		{
			"identity": {"schema": "public", "name": "users"},
			"managed": true,
			"columns": [
				{"name": "id", "type": {"name": "int8", "codec": "bigint"}, "notNull": true,
				 "default": {"kind": "identity", "generated": "always"}},
				{"name": "email", "type": {"name": "varchar", "codec": "string", "params": {"length": 120}}, "notNull": true,
				 "default": {"kind": "literal", "sql": "'y'"}},
				{"name": "role", "type": {"name": "enum", "codec": "enum", "enum": {"schema": "public", "name": "mood"}}, "notNull": true,
				 "default": {"kind": "literal", "sql": "'ok'"}},
				{"name": "tags", "type": {"name": "text", "codec": "array", "array": true}, "notNull": true,
				 "default": {"kind": "literal", "sql": "'{}'"}},
				{"name": "score", "type": {"name": "numeric", "codec": "decimal-string", "params": {"precision": 8, "scale": 2}}, "notNull": false,
				 "default": {"kind": "literal", "sql": "1.5"}},
				{"name": "seen_at", "type": {"name": "timestamptz", "codec": "timestamptz-string"}, "notNull": true,
				 "default": {"kind": "expression", "sql": "CURRENT_TIMESTAMP"}},
				{"name": "seq_num", "type": {"name": "int4", "codec": "number"}, "notNull": false,
				 "default": {"kind": "sequence", "sequence": {"schema": "public", "name": "m02_seq"}}}
			],
			"constraints": [
				{"name": "users_pkey", "type": "primary-key", "columns": ["id"]},
				{"name": "users_id_email_key", "type": "unique", "columns": ["id", "email"]}
			],
			"indexes": []
		},
		{
			"identity": {"schema": "public", "name": "posts"},
			"managed": true,
			"columns": [
				{"name": "author_id", "type": {"name": "int8", "codec": "bigint"}, "notNull": true},
				{"name": "author_email", "type": {"name": "varchar", "codec": "string", "params": {"length": 80}}, "notNull": true},
				{"name": "slug", "type": {"name": "varchar", "codec": "string", "params": {"length": 120}}, "notNull": true},
				{"name": "body", "type": {"name": "text", "codec": "string"}, "notNull": true},
				{"name": "pinned", "type": {"name": "bool", "codec": "boolean"}, "notNull": true,
				 "default": {"kind": "literal", "sql": "false"}}
			],
			"constraints": [
				{"name": "posts_pk", "type": "primary-key", "columns": ["author_id", "slug"]},
				{"name": "posts_slug_check", "type": "check", "expression": "slug <> ''"},
				{"name": "posts_author_fkey", "type": "foreign-key", "columns": ["author_id", "author_email"],
				 "references": {"table": {"schema": "public", "name": "users"}, "columns": ["id", "email"], "onDelete": "restrict"},
				 "deferrable": true, "initiallyDeferred": true}
			],
			"indexes": [
				{"identity": {"schema": "public", "name": "posts_body_idx"}, "unique": false, "method": "hash",
				 "key": [{"column": "body"}]},
				{"identity": {"schema": "public", "name": "posts_active_idx"}, "unique": false, "method": "btree",
				 "key": [{"column": "author_id"}], "include": ["body"], "where": "pinned = false"},
				{"identity": {"schema": "public", "name": "posts_slug_lower_idx"}, "unique": false, "method": "btree",
				 "key": [{"expression": "lower(slug)"}]}
			]
		},
		{
			"identity": {"schema": "app", "name": "comments"},
			"managed": true,
			"columns": [
				{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true,
				 "default": {"kind": "identity", "generated": "by default"}},
				{"name": "post_author", "type": {"name": "int8", "codec": "bigint"}, "notNull": true},
				{"name": "post_slug", "type": {"name": "varchar", "codec": "string", "params": {"length": 120}}, "notNull": true},
				{"name": "moods", "type": {"name": "enum", "codec": "array", "array": true, "enum": {"schema": "public", "name": "mood"}}, "notNull": true,
				 "default": {"kind": "literal", "sql": "'{}'"}},
				{"name": "author", "type": {"name": "int8", "codec": "bigint"}, "notNull": true}
			],
			"constraints": [
				{"name": "comments_pkey", "type": "primary-key", "columns": ["id"]},
				{"name": "comments_post_fkey", "type": "foreign-key", "columns": ["post_author", "post_slug"],
				 "references": {"table": {"schema": "public", "name": "posts"}, "columns": ["author_id", "slug"], "onDelete": "cascade"}},
				{"name": "comments_author_fkey", "type": "foreign-key", "columns": ["author"],
				 "references": {"table": {"schema": "public", "name": "users"}, "columns": ["id"], "onDelete": "cascade"}}
			],
			"indexes": []
		}
	],
	"enums": [
		{"identity": {"schema": "public", "name": "mood"}, "managed": true, "values": ["sad", "ok", "happy", "elated"]}
	],
	"views": [
		{"identity": {"schema": "public", "name": "v_active_posts"}, "managed": true,
		 "definition": "select author_id, pinned from posts where pinned = false"},
		{"identity": {"schema": "public", "name": "v_checked"}, "managed": true, "checkOption": "cascaded", "securityInvoker": true,
		 "definition": "select id, email from users where id > 0"}
	],
	"opaque": []
}`

func TestV2RoundTripModifications(t *testing.T) {
	h := newM02Harness(t, "rt2")
	h.exec(`CREATE SEQUENCE public.m02_seq`)
	h.planAndApply(m02Doc1, nil, false)

	// Seed data the modifications must not destroy.
	h.exec(`INSERT INTO public.users (email) VALUES ('a@x.com')`)
	h.exec(`INSERT INTO public.posts (author_id, author_email, slug, body) SELECT id, email, 's1', 'b1' FROM public.users`)

	res := h.planAndApply(m02Doc2, nil, false)
	if len(res.Up) == 0 {
		t.Fatal("modification plan must contain statements")
	}
	h.assertDiffEmpty(m02Doc2, nil, false)

	if got := h.queryOne(`SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='public.posts'::regclass AND conname='posts_author_fkey'`); !strings.Contains(got, "ON DELETE RESTRICT") {
		t.Fatalf("FK action must change to RESTRICT: %s", got)
	}
	if got := h.queryOne(`SELECT indexdef FROM pg_indexes WHERE indexname='posts_active_idx'`); !strings.Contains(got, "(pinned = false)") {
		t.Fatalf("same-name index predicate must change: %s", got)
	}
	if got := h.queryOne(`SELECT enumlabel FROM pg_enum e JOIN pg_type t ON t.oid=e.enumtypid WHERE t.typname='mood' ORDER BY enumsortorder DESC LIMIT 1`); got != "elated" {
		t.Fatalf("enum value must be appended: %s", got)
	}
	if got := h.queryOne(`SELECT count(*)::text FROM public.posts`); got != "1" {
		t.Fatalf("seeded post must survive modifications, got %s rows", got)
	}
	if got := h.queryOne(`SELECT atttypmod::text FROM pg_attribute WHERE attrelid='public.users'::regclass AND attname='email'`); got != "124" {
		t.Fatalf("varchar length change must apply (typmod 124 = varchar(120)): %s", got)
	}
}

func TestV2RenamePreservesSeededDataAndRejectsAmbiguity(t *testing.T) {
	h := newM02Harness(t, "rename")
	h.exec(`CREATE SEQUENCE public.m02_seq`)
	h.planAndApply(m02Doc1, nil, false)
	h.exec(`INSERT INTO public.users (email) VALUES ('a@x.com')`)

	renamedDoc := strings.Replace(m02Doc2, `{"name": "seen_at"`, `{"name": "last_seen_at"`, 1)
	if renamedDoc == m02Doc2 {
		t.Fatal("fixture rewrite did not apply")
	}

	// Ambiguity: the rename target does not exist in the desired schema
	// (forged flag), and a flag whose source is still declared must be
	// rejected as ambiguous.
	h.assertDiffError(m02Doc2, "ambiguous", map[string]string{"public.users.seen_at": "email"})
	h.assertDiffError(m02Doc2, "does not exist", map[string]string{"public.users.last_seen_at": "seen_at"})

	// Correct rename: data preserved.
	// The rename map is consumed by the applying diff; afterwards the
	// document matches the database without it (a stale map is a loud
	// error, pinned by the forged-source probe above).
	renames := map[string]string{"public.users.last_seen_at": "seen_at"}
	h.planAndApply(renamedDoc, renames, false)
	h.assertDiffEmpty(renamedDoc, nil, false)
	if got := h.queryOne(`SELECT count(*)::text FROM public.users WHERE last_seen_at IS NOT NULL`); got != "1" {
		t.Fatalf("renamed column must keep its data, got %s surviving rows", got)
	}
	if got := h.queryOne(`SELECT count(*)::text FROM pg_attribute WHERE attrelid='public.users'::regclass AND attname='seen_at'`); got != "0" {
		t.Fatalf("old column name must be gone")
	}
}

func TestV2DropOrderingAndCycles(t *testing.T) {
	h := newM02Harness(t, "drops")
	h.exec(`CREATE SEQUENCE public.m02_seq`)
	h.planAndApply(m02Doc1, nil, false)

	// Add an explicit FK cycle: users -> comments closes the loop that
	// already runs comments -> posts -> users.
	h.exec(`ALTER TABLE public.users ADD COLUMN fav_comment int4 REFERENCES app.comments (id)`)

	const emptyDoc = `{
		"version": 2, "dialect": "postgresql", "capabilities": [],
		"schemas": [{"name": "public"}, {"name": "app"}],
		"tables": [], "enums": [], "views": [], "opaque": []
	}`

	// Without acknowledgement nothing drops.
	res := h.planAndApply(emptyDoc, nil, false)
	for _, stmt := range res.Up {
		if strings.Contains(stmt, "drop table") {
			t.Fatalf("drops must require --allow-destructive: %v", res.Up)
		}
	}

	h.planAndApply(emptyDoc, nil, true)
	h.assertDiffEmpty(emptyDoc, nil, true)
	if got := h.queryOne(`SELECT count(*)::text FROM pg_tables WHERE tablename IN ('users','posts') OR (schemaname='app' AND tablename='comments')`); got != "0" {
		t.Fatalf("all managed tables must drop, %s remain", got)
	}
	if got := h.queryOne(`SELECT count(*)::text FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE t.typname='mood' AND n.nspname='public'`); got != "0" {
		t.Fatalf("enum must drop after the tables that used it")
	}
	// The standalone sequence is opaque inventory: untouched.
	if got := h.queryOne(`SELECT count(*)::text FROM pg_class WHERE relname='m02_seq' AND relkind='S'`); got != "1" {
		t.Fatalf("opaque standalone sequence must never be dropped")
	}
}

func TestV2UnsupportedObjectsUntouchedAndBlocking(t *testing.T) {
	h := newM02Harness(t, "unsup")
	h.exec(`CREATE TABLE gen_t (pos int, g int GENERATED ALWAYS AS (pos * 2) STORED)`)
	h.exec(`CREATE MATERIALIZED VIEW mat_v AS SELECT pos FROM gen_t`)
	h.exec(`CREATE SEQUENCE leftover_seq`)
	h.exec(`CREATE TABLE trig_t (id int PRIMARY KEY)`)
	h.exec(`CREATE FUNCTION noop_trg() RETURNS trigger AS $$ BEGIN RETURN NULL; END $$ LANGUAGE plpgsql`)
	h.exec(`CREATE TRIGGER trig AFTER INSERT ON trig_t FOR EACH ROW EXECUTE FUNCTION noop_trg()`)
	h.exec(`CREATE TABLE rls_t (id int PRIMARY KEY)`)
	h.exec(`ALTER TABLE rls_t ENABLE ROW LEVEL SECURITY`)
	h.exec(`CREATE TABLE part_parent (id int, v int) PARTITION BY RANGE (id)`)
	h.exec(`CREATE TABLE part_child PARTITION OF part_parent FOR VALUES FROM (0) TO (100)`)
	h.exec(`CREATE EXTENSION IF NOT EXISTS pg_stat_statements`)

	actual, err := h.client.IntrospectV2(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	am, err := ModelFromRoot(actual.Root)
	if err != nil {
		t.Fatal(err)
	}
	inventory := map[string]bool{}
	for _, o := range am.Opaque {
		inventory[o.Kind+" "+o.Identity.String()] = true
	}
	for _, expect := range []string{
		"unsupported-table public.gen_t",
		"unsupported-object public.mat_v",
		"unsupported-object public.leftover_seq",
		"unsupported-table public.rls_t",
		"unsupported-table public.part_parent",
		"unsupported-table public.part_child",
		"unsupported-table public.trig_t",
		"extension-object public.pg_stat_statements",
	} {
		if !inventory[expect] {
			t.Fatalf("%s must be inventoried as opaque; inventory: %v", expect, inventory)
		}
	}
	for _, o := range am.Opaque {
		if o.Identity.Name == "gen_t" && !strings.Contains(o.Reason, "generated column") {
			t.Fatalf("gen_t reason must name the generated column: %s", o.Reason)
		}
		if o.Identity.Name == "trig_t" && !strings.Contains(o.Reason, "trigger") {
			t.Fatalf("trig_t must stay unrepresentable via its trigger: %s", o.Reason)
		}
	}
	if am.OpaqueEntry("unsupported-table", V2Identity{Schema: "public", Name: "trig_t"}) == nil {
		t.Fatal("trigger table must be blocked")
	}

	// A desired document that manages an unrelated table plans and applies
	// cleanly while leaving every unsupported object untouched.
	const okDoc = `{
		"version": 2, "dialect": "postgresql", "capabilities": [],
		"schemas": [{"name": "public"}],
		"tables": [{
			"identity": {"schema": "public", "name": "fresh"}, "managed": true,
			"columns": [{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true}],
			"constraints": [{"name": "fresh_pkey", "type": "primary-key", "columns": ["id"]}],
			"indexes": []
		}],
		"enums": [], "views": [], "opaque": []
	}`
	h.planAndApply(okDoc, nil, false)
	h.assertDiffEmpty(okDoc, nil, false)
	if got := h.queryOne(`SELECT count(*)::text FROM pg_class WHERE relname IN ('gen_t','mat_v','leftover_seq','rls_t','part_parent','part_child','trig_t')`); got != "7" {
		t.Fatalf("unsupported objects must be untouched, found %s of 7", got)
	}
	if got := h.queryOne(`SELECT count(*)::text FROM pg_matviews`); got != "1" {
		t.Fatalf("materialized view must survive")
	}

	// Declaring unsupported/extension objects as managed blocks the plan.
	h.assertDiffError(strings.Replace(okDoc, `"name": "fresh"`, `"name": "gen_t"`, 1), "generated column", nil)
	// Extension-owned object declared as a desired view: blocked (B03 invariant).
	extViewDoc := strings.Replace(okDoc, `"views": []`, `"views": [{"identity": {"schema": "public", "name": "pg_stat_statements"}, "managed": true, "definition": "select 1"}]`, 1)
	h.assertDiffError(extViewDoc, "extension-owned", nil)
	// Desired table colliding with an extension-owned object: blocked
	// before any kind-conflict consideration (B03 invariant).
	h.assertDiffError(strings.Replace(okDoc, `"name": "fresh"`, `"name": "pg_stat_statements"`, 1), "extension-owned", nil)
}

// TestV2RoundTripNoActionSimpleFKConverge pins the MAJOR-1 rework (V10
// run-twice convergence): contract-valid "onDelete": "no action" and
// "match": "simple" spellings are equivalent to omitted clauses in the
// catalog and must converge instead of replaying drop+add FK forever.
func TestV2RoundTripNoActionSimpleFKConverge(t *testing.T) {
	h := newM02Harness(t, "noact")
	const doc = `{
		"version": 2, "dialect": "postgresql", "capabilities": [],
		"schemas": [{"name": "public"}],
		"tables": [
			{
				"identity": {"schema": "public", "name": "users"}, "managed": true,
				"columns": [
					{"name": "id", "type": {"name": "int8", "codec": "bigint"}, "notNull": true,
					 "default": {"kind": "identity", "generated": "always"}}
				],
				"constraints": [{"name": "users_pkey", "type": "primary-key", "columns": ["id"]}],
				"indexes": []
			},
			{
				"identity": {"schema": "public", "name": "orders"}, "managed": true,
				"columns": [
					{"name": "id", "type": {"name": "int8", "codec": "bigint"}, "notNull": true,
					 "default": {"kind": "identity", "generated": "always"}},
					{"name": "user_id", "type": {"name": "int8", "codec": "bigint"}, "notNull": true}
				],
				"constraints": [
					{"name": "orders_pkey", "type": "primary-key", "columns": ["id"]},
					{"name": "orders_user_fkey", "type": "foreign-key", "columns": ["user_id"],
					 "references": {"table": {"schema": "public", "name": "users"}, "columns": ["id"], "onDelete": "no action"}}
				],
				"indexes": []
			},
			{
				"identity": {"schema": "public", "name": "invoices"}, "managed": true,
				"columns": [
					{"name": "id", "type": {"name": "int8", "codec": "bigint"}, "notNull": true,
					 "default": {"kind": "identity", "generated": "always"}},
					{"name": "order_id", "type": {"name": "int8", "codec": "bigint"}, "notNull": true}
				],
				"constraints": [
					{"name": "invoices_pkey", "type": "primary-key", "columns": ["id"]},
					{"name": "invoices_order_fkey", "type": "foreign-key", "columns": ["order_id"],
					 "references": {"table": {"schema": "public", "name": "orders"}, "columns": ["id"], "onDelete": "cascade", "match": "simple"}}
				],
				"indexes": []
			}
		],
		"enums": [], "views": [], "opaque": []
	}`

	res := h.planAndApply(doc, nil, false)
	if len(res.Up) == 0 {
		t.Fatal("initial plan must create objects")
	}
	h.assertDiffEmpty(doc, nil, false)

	// Independent oracle: the actions really landed as the catalog default
	// (no ON DELETE clause on the no-action FK; CASCADE on the other).
	if got := h.queryOne(`SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='public.orders'::regclass AND conname='orders_user_fkey'`); strings.Contains(got, "ON DELETE") {
		t.Fatalf("no-action FK must deparse without an ON DELETE clause (catalog default), got: %s", got)
	}
	if got := h.queryOne(`SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='public.invoices'::regclass AND conname='invoices_order_fkey'`); !strings.Contains(got, "ON DELETE CASCADE") || strings.Contains(got, "MATCH") {
		t.Fatalf("cascade+simple FK must deparse as ON DELETE CASCADE without MATCH clause, got: %s", got)
	}
}

// TestV2ViewWithInsteadOfTriggerOpaqueAndBlocking pins the MAJOR-4 rework:
// views carrying INSTEAD-OF triggers (or security_barrier) introspect as
// opaque unsupported objects — inventoried, reported, never planned — and
// a desired view over them blocks at plan time instead of silently
// destroying the trigger through drop+recreate.
func TestV2ViewWithInsteadOfTriggerOpaqueAndBlocking(t *testing.T) {
	h := newM02Harness(t, "vtrig")
	h.exec(`CREATE TABLE t (id int4 PRIMARY KEY, note text)`)
	h.exec(`CREATE VIEW v_trig AS SELECT id FROM t`)
	h.exec(`CREATE FUNCTION io_trg() RETURNS trigger AS $$ BEGIN RETURN NULL; END $$ LANGUAGE plpgsql`)
	h.exec(`CREATE TRIGGER v_trig_io INSTEAD OF INSERT ON v_trig FOR EACH ROW EXECUTE FUNCTION io_trg()`)
	h.exec(`CREATE VIEW v_sb WITH (security_barrier = true) AS SELECT id FROM t`)

	actual, err := h.client.IntrospectV2(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	am, err := ModelFromRoot(actual.Root)
	if err != nil {
		t.Fatal(err)
	}
	if len(am.Views) != 0 {
		t.Fatalf("trigger/security-barrier views must not introspect as representable, got %+v", am.Views)
	}
	opaqueTrig := am.OpaqueEntry("unsupported-object", V2Identity{Schema: "public", Name: "v_trig"})
	if opaqueTrig == nil || !strings.Contains(opaqueTrig.Reason, "1 user trigger") {
		t.Fatalf("v_trig must be opaque with its trigger named, got %+v", am.Opaque)
	}
	opaqueSB := am.OpaqueEntry("unsupported-object", V2Identity{Schema: "public", Name: "v_sb"})
	if opaqueSB == nil || !strings.Contains(opaqueSB.Reason, "security_barrier") {
		t.Fatalf("v_sb must be opaque with security_barrier named, got %+v", am.Opaque)
	}

	const manageT = `{
		"version": 2, "dialect": "postgresql", "capabilities": [],
		"schemas": [{"name": "public"}],
		"tables": [{
			"identity": {"schema": "public", "name": "t"}, "managed": true,
			"columns": [
				{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true},
				{"name": "note", "type": {"name": "text", "codec": "string"}, "notNull": false}
			],
			"constraints": [{"name": "t_pkey", "type": "primary-key", "columns": ["id"]}],
			"indexes": []
		}],
		"enums": [], "views": [], "opaque": []
	}`

	// Declaring the trigger view as a managed (plain) view blocks at plan
	// time — including as a definition change, which used to apply and
	// silently destroy the trigger.
	declareView := func(def string) string {
		return strings.Replace(manageT, `"views": []`,
			`"views": [{"identity": {"schema": "public", "name": "v_trig"}, "managed": true, "definition": "`+def+`"}]`, 1)
	}
	h.assertDiffError(declareView("SELECT id FROM t"), "unrepresentable", nil)
	h.assertDiffError(declareView("SELECT id, note FROM t"), "unrepresentable", nil)
	if got := h.queryOne(`SELECT count(*)::text FROM pg_trigger WHERE tgrelid = 'public.v_trig'::regclass AND NOT tgisinternal`); got != "1" {
		t.Fatalf("the INSTEAD OF trigger must be intact after blocked pushes, got %s", got)
	}

	// An unrelated table alteration leaves the opaque views untouched and
	// reports them (V10: unsupported objects remain untouched + reported).
	const alterT = `{
		"version": 2, "dialect": "postgresql", "capabilities": [],
		"schemas": [{"name": "public"}],
		"tables": [{
			"identity": {"schema": "public", "name": "t"}, "managed": true,
			"columns": [
				{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true},
				{"name": "note", "type": {"name": "text", "codec": "string"}, "notNull": false},
				{"name": "extra", "type": {"name": "int4", "codec": "number"}, "notNull": false}
			],
			"constraints": [{"name": "t_pkey", "type": "primary-key", "columns": ["id"]}],
			"indexes": []
		}],
		"enums": [], "views": [], "opaque": []
	}`
	res := h.planAndApply(alterT, nil, false)
	foundReport := false
	for _, w := range res.Warnings {
		if strings.Contains(w, "unsupported-object public.v_trig") && strings.Contains(w, "left untouched") {
			foundReport = true
		}
	}
	if !foundReport {
		t.Fatalf("opaque views must be reported as untouched; warnings: %v", res.Warnings)
	}
	h.assertDiffEmpty(alterT, nil, false)
	if got := h.queryOne(`SELECT count(*)::text FROM pg_trigger WHERE tgrelid = 'public.v_trig'::regclass AND NOT tgisinternal`); got != "1" {
		t.Fatalf("the INSTEAD OF trigger must survive unrelated alterations, got %s", got)
	}
	if got := h.queryOne(`SELECT count(*)::text FROM pg_class WHERE relname IN ('v_trig','v_sb') AND relkind='v'`); got != "2" {
		t.Fatalf("both views must survive, got %s", got)
	}
}

// TestV2TwinFailureFlagsEquivalentCheckAndDefault pins the MAJOR-2 rework
// live: when the twin normalizer cannot materialize the desired table
// (here: a sequence default naming a nonexistent sequence), textual
// comparisons of equivalent check/default/index expressions must be
// flagged "equivalence not verified" — with a working twin the same
// document compares equal (control). Spellings are hand-written forms the
// catalog normalizes: 'x' vs 'x'::varchar, bare check/index text vs the
// deparsed parenthesized forms.
func TestV2TwinFailureFlagsEquivalentCheckAndDefault(t *testing.T) {
	h := newM02Harness(t, "twinfail")
	h.exec(`CREATE TABLE t (slug text NOT NULL, note varchar(10) NOT NULL DEFAULT 'x')`)
	h.exec(`ALTER TABLE t ADD CONSTRAINT t_slug_check CHECK (length(slug) > 0)`)
	h.exec(`CREATE INDEX t_low_idx ON t (lower(note)) WHERE slug <> ''`)

	// Same expressions in hand-written spellings; optionally plus a column
	// whose sequence default forces the twin CREATE to fail.
	doc := func(withBrokenColumn bool) string {
		broken := ""
		if withBrokenColumn {
			broken = `,{"name": "bad", "type": {"name": "int4", "codec": "number"}, "notNull": false,
				"default": {"kind": "sequence", "sequence": {"schema": "public", "name": "no_such_seq"}}}`
		}
		return `{
			"version": 2, "dialect": "postgresql", "capabilities": [],
			"schemas": [{"name": "public"}],
			"tables": [{
				"identity": {"schema": "public", "name": "t"}, "managed": true,
				"columns": [
					{"name": "slug", "type": {"name": "text", "codec": "string"}, "notNull": true},
					{"name": "note", "type": {"name": "varchar", "codec": "string", "params": {"length": 10}}, "notNull": true,
					 "default": {"kind": "literal", "sql": "'x'"}}` + broken + `
				],
				"constraints": [{"name": "t_slug_check", "type": "check", "expression": "length(slug) > 0"}],
				"indexes": [
					{"identity": {"schema": "public", "name": "t_low_idx"}, "unique": false, "method": "btree",
					 "key": [{"expression": "lower(note)"}], "where": "slug <> ''"}
				]
			}],
			"enums": [], "views": [], "opaque": []
		}`
	}

	// Control: with a working twin the equivalent spellings compare equal.
	h.assertDiffEmpty(doc(false), nil, false)

	// Forced twin failure: plan (do not apply — the broken column would
	// fail at apply) must carry unverified warnings for the check, the
	// default and BOTH index fields.
	desired := h.parseDoc(doc(true))
	actual, err := h.client.IntrospectV2(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	norm, err := h.client.NewTwinNormalizer(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer norm.Close()
	result, err := DiffV2Document(context.Background(), desired, actual, DiffV2Options{Normalizer: norm})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"equivalence not verified for check constraint t_slug_check expression",
		"equivalence not verified for column public.t.note default",
		"equivalence not verified for index public.t_low_idx key part",
		"equivalence not verified for index public.t_low_idx predicate",
	} {
		found := false
		for _, w := range result.Warnings {
			if strings.Contains(w, want) {
				found = true
			}
		}
		if !found {
			t.Fatalf("missing unverified warning %q; warnings: %v", want, result.Warnings)
		}
	}
}

// TestV2UnchangedViewGrantLossWarned pins the MINOR-1 rework live: an
// unchanged view recreated around unrelated table alterations loses its
// grants (PostgreSQL drops them with the view) — the plan must say so.
func TestV2UnchangedViewGrantLossWarned(t *testing.T) {
	h := newM02Harness(t, "grants")
	const base = `{
		"version": 2, "dialect": "postgresql", "capabilities": [],
		"schemas": [{"name": "public"}],
		"tables": [
			{"identity": {"schema": "public", "name": "t1"}, "managed": true,
			 "columns": [{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true}],
			 "constraints": [{"name": "t1_pkey", "type": "primary-key", "columns": ["id"]}], "indexes": []},
			{"identity": {"schema": "public", "name": "t2"}, "managed": true,
			 "columns": [{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true}],
			 "constraints": [{"name": "t2_pkey", "type": "primary-key", "columns": ["id"]}], "indexes": []}
		],
		"enums": [],
		"views": [{"identity": {"schema": "public", "name": "v_t2"}, "managed": true,
			"definition": "select id from public.t2"}],
		"opaque": []
	}`
	h.planAndApply(base, nil, false)
	h.exec(`GRANT SELECT ON public.v_t2 TO PUBLIC`)
	if got := h.queryOne(`SELECT has_table_privilege('public', 'public.v_t2', 'SELECT')::text`); got != "true" {
		t.Fatalf("grant precondition failed")
	}

	// Only t1 changes; v_t2 is unchanged but rides the drop+recreate.
	altered := strings.Replace(base, `{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true}],`+"\n"+`			 "constraints": [{"name": "t1_pkey"`,
		`{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true},{"name": "extra", "type": {"name": "int4", "codec": "number"}, "notNull": false}],`+"\n"+`			 "constraints": [{"name": "t1_pkey"`, 1)
	if altered == base {
		t.Fatal("fixture rewrite did not apply")
	}
	res := h.planAndApply(altered, nil, false)
	found := false
	for _, w := range res.Warnings {
		if strings.Contains(w, "view public.v_t2 is unchanged but is dropped and recreated") {
			found = true
		}
	}
	if !found {
		t.Fatalf("grant-losing view recreation must be warned; warnings: %v", res.Warnings)
	}
	// Documented behavior: the grant IS lost (that is what the warning says).
	if got := h.queryOne(`SELECT has_table_privilege('public', 'public.v_t2', 'SELECT')::text`); got != "false" {
		t.Fatalf("grant loss is the documented behavior, got %s", got)
	}
	h.assertDiffEmpty(altered, nil, false)
}

// TestV2UnqualifiedViewRefNoted pins the MINOR-3 rework: a desired view
// definition the twin normalizer cannot materialize (unqualified reference
// to a relation outside the search path) is reported with a steering note
// instead of failing only at apply.
func TestV2UnqualifiedViewRefNoted(t *testing.T) {
	h := newM02Harness(t, "vnote")
	h.exec(`CREATE SCHEMA app`)
	h.exec(`CREATE TABLE app.orders (id int4 PRIMARY KEY)`)
	const doc = `{
		"version": 2, "dialect": "postgresql", "capabilities": [],
		"schemas": [{"name": "public"}, {"name": "app"}],
		"tables": [],
		"enums": [],
		"views": [{"identity": {"schema": "public", "name": "v_orders"}, "managed": true,
			"definition": "select id from orders"}],
		"opaque": []
	}`
	desired := h.parseDoc(doc)
	actual, err := h.client.IntrospectV2(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	norm, err := h.client.NewTwinNormalizer(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer norm.Close()
	result, err := DiffV2Document(context.Background(), desired, actual, DiffV2Options{Normalizer: norm})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, w := range result.Warnings {
		if strings.Contains(w, "view public.v_orders definition could not be normalized") {
			found = true
		}
	}
	if !found {
		t.Fatalf("unnormalizable view definition must be noted; warnings: %v", result.Warnings)
	}
}
