package db

// Offline unit tests for the v2 diff engine (no database): plan shapes,
// ordering, blocking rules and rename validation against hand-built
// documents validated through the real contract parser. Expression
// equivalence (twin normalization) is covered by the live round-trip tests.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func docFromModelForTest(t *testing.T, m V2DocumentModel) *V2Document {
	t.Helper()
	// The contract requires every collection field present; nil slices
	// marshal to null, so normalize them to empty arrays.
	for i := range m.Tables {
		if m.Tables[i].Indexes == nil {
			m.Tables[i].Indexes = []V2Index{}
		}
		if m.Tables[i].Constraints == nil {
			m.Tables[i].Constraints = []V2Constraint{}
		}
	}
	root, err := RootFromModel(m)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := ParseV2Document(raw)
	if err != nil {
		t.Fatalf("fixture document must be contract-valid: %v", err)
	}
	return doc
}

func emptyDBDoc(t *testing.T) *V2Document {
	t.Helper()
	return docFromModelForTest(t, V2DocumentModel{
		Version: 2, Dialect: "postgresql", Capabilities: []string{},
		Schemas: []V2SchemaDecl{{Name: "public"}},
		Tables:  []V2Table{}, Enums: []V2EnumDecl{}, Views: []V2View{}, Opaque: []V2Opaque{},
	})
}

func intColumn(name string, notNull bool) V2Column {
	return V2Column{Name: name, Type: V2ColumnType{Name: "int4", Codec: "number"}, NotNull: notNull}
}

func textColumn(name string, notNull bool) V2Column {
	return V2Column{Name: name, Type: V2ColumnType{Name: "text", Codec: "string"}, NotNull: notNull}
}

func TestDiffV2CreateTableQualifiedWithCompositeConstraints(t *testing.T) {
	users := V2Table{
		Identity: V2Identity{Schema: "public", Name: "users"},
		Managed:  true,
		Columns: []V2Column{
			{Name: "id", Type: V2ColumnType{Name: "int8", Codec: "bigint"}, NotNull: true,
				Default: &V2ColumnDefault{Kind: "identity", Generated: strPtrV2("always")}},
			{Name: "email", Type: V2ColumnType{Name: "varchar", Codec: "string", Params: map[string]int64{"length": 80}}, NotNull: true,
				Default: &V2ColumnDefault{Kind: "literal", SQL: strPtrV2("'x'::varchar")}},
		},
		Constraints: []V2Constraint{
			{Name: "users_pkey", Type: "primary-key", Columns: []string{"id"}},
			{Name: "users_id_email_key", Type: "unique", Columns: []string{"id", "email"}},
		},
	}
	posts := V2Table{
		Identity: V2Identity{Schema: "public", Name: "posts"},
		Managed:  true,
		Columns: []V2Column{
			intColumn("author_id", true), intColumn("author_email", true), textColumn("slug", true),
		},
		Constraints: []V2Constraint{
			{Name: "posts_pk", Type: "primary-key", Columns: []string{"author_id", "slug"}},
			{Name: "posts_slug_check", Type: "check", Columns: nil, Expression: strPtrV2("length(slug) > 0")},
			{Name: "posts_author_fkey", Type: "foreign-key", Columns: []string{"author_id", "author_email"},
				References: &V2FKReference{
					Table:    V2Identity{Schema: "public", Name: "users"},
					Columns:  []string{"id", "email"},
					OnDelete: strPtrV2("cascade"),
				},
				Deferrable:        boolPtr(true),
				InitiallyDeferred: boolPtr(true)},
		},
	}
	desired := docFromModelForTest(t, V2DocumentModel{
		Version: 2, Dialect: "postgresql", Capabilities: []string{},
		Schemas: []V2SchemaDecl{{Name: "public"}},
		Tables:  []V2Table{posts, users},
		Enums:   []V2EnumDecl{}, Views: []V2View{}, Opaque: []V2Opaque{},
	})

	result, err := DiffV2Document(context.Background(), desired, emptyDBDoc(t), DiffV2Options{})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(result.Up, "\n")
	if !strings.Contains(joined, `create table "public"."users"`) {
		t.Fatalf("users must be created before posts (FK dependency), got:\n%s", joined)
	}
	if strings.Index(joined, `create table "public"."users"`) > strings.Index(joined, `create table "public"."posts"`) {
		t.Fatalf("posts must follow users:\n%s", joined)
	}
	if !strings.Contains(joined, `"id" bigint generated always as identity not null`) {
		t.Fatalf("identity column DDL missing:\n%s", joined)
	}
	if !strings.Contains(joined, `"email" varchar(80) default 'x'::varchar not null`) {
		t.Fatalf("varchar default DDL missing:\n%s", joined)
	}
	if !strings.Contains(joined, `constraint "posts_pk" primary key ("author_id", "slug")`) {
		t.Fatalf("composite PK missing:\n%s", joined)
	}
	if !strings.Contains(joined, `constraint "posts_author_fkey" foreign key ("author_id", "author_email") references "public"."users" ("id", "email") on delete cascade deferrable initially deferred`) {
		t.Fatalf("composite FK DDL missing:\n%s", joined)
	}
}

func TestDiffV2CyclicFKCreatesDeferConstraints(t *testing.T) {
	a := V2Table{
		Identity: V2Identity{Schema: "public", Name: "a"},
		Managed:  true,
		Columns:  []V2Column{intColumn("id", true), intColumn("b_id", false)},
		Constraints: []V2Constraint{
			{Name: "a_pkey", Type: "primary-key", Columns: []string{"id"}},
			{Name: "a_b_id_fkey", Type: "foreign-key", Columns: []string{"b_id"},
				References: &V2FKReference{Table: V2Identity{Schema: "public", Name: "b"}, Columns: []string{"id"}}},
		},
	}
	b := V2Table{
		Identity: V2Identity{Schema: "public", Name: "b"},
		Managed:  true,
		Columns:  []V2Column{intColumn("id", true), intColumn("a_id", false)},
		Constraints: []V2Constraint{
			{Name: "b_pkey", Type: "primary-key", Columns: []string{"id"}},
			{Name: "b_a_id_fkey", Type: "foreign-key", Columns: []string{"a_id"},
				References: &V2FKReference{Table: V2Identity{Schema: "public", Name: "a"}, Columns: []string{"id"}}},
		},
	}
	desired := docFromModelForTest(t, V2DocumentModel{
		Version: 2, Dialect: "postgresql", Capabilities: []string{},
		Schemas: []V2SchemaDecl{{Name: "public"}},
		Tables:  []V2Table{a, b},
		Enums:   []V2EnumDecl{}, Views: []V2View{}, Opaque: []V2Opaque{},
	})
	result, err := DiffV2Document(context.Background(), desired, emptyDBDoc(t), DiffV2Options{})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(result.Up, "\n")
	// Both tables must exist before either cyclic FK is added.
	for _, create := range []string{`create table "public"."a"`, `create table "public"."b"`} {
		if !strings.Contains(joined, create) {
			t.Fatalf("missing %s in:\n%s", create, joined)
		}
	}
	if !strings.Contains(joined, `alter table "public"."a" add constraint "a_b_id_fkey" foreign key ("b_id") references "public"."b" ("id")`) &&
		!strings.Contains(joined, `alter table "public"."b" add constraint "b_a_id_fkey" foreign key ("a_id") references "public"."a" ("id")`) {
		t.Fatalf("at least one cyclic FK must be deferred to ALTER:\n%s", joined)
	}
	// Deferred FK ALTERs come after both creates.
	if strings.LastIndex(joined, `create table`) > strings.Index(joined, "add constraint") {
		t.Fatalf("deferred FK added before all tables exist:\n%s", joined)
	}
}

func TestDiffV2DropOrderAndCycleConstraintStrategy(t *testing.T) {
	// a -> b -> c FK chain plus c -> a closing a cycle.
	mk := func(name, fkTo string) V2Table {
		cols := []V2Column{intColumn("id", true)}
		cons := []V2Constraint{{Name: name + "_pkey", Type: "primary-key", Columns: []string{"id"}}}
		if fkTo != "" {
			cols = append(cols, intColumn(fkTo+"_id", false))
			cons = append(cons, V2Constraint{
				Name: name + "_" + fkTo + "_id_fkey", Type: "foreign-key", Columns: []string{fkTo + "_id"},
				References: &V2FKReference{Table: V2Identity{Schema: "public", Name: fkTo}, Columns: []string{"id"}},
			})
		}
		return V2Table{Identity: V2Identity{Schema: "public", Name: name}, Managed: true, Columns: cols, Constraints: cons}
	}
	actual := docFromModelForTest(t, V2DocumentModel{
		Version: 2, Dialect: "postgresql", Capabilities: []string{},
		Schemas: []V2SchemaDecl{{Name: "public"}},
		Tables:  []V2Table{mk("a", "b"), mk("b", "c"), mk("c", "a")},
		Enums:   []V2EnumDecl{}, Views: []V2View{}, Opaque: []V2Opaque{},
	})
	result, err := DiffV2Document(context.Background(), emptyDBDoc(t), actual, DiffV2Options{AllowDestructive: true})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(result.Up, "\n")
	// The cycle cannot be ordered: its FK constraints are dropped first.
	if !strings.Contains(joined, "drop constraint") {
		t.Fatalf("cyclic drops must first drop the cycle's FK constraints:\n%s", joined)
	}
	drops := []int{
		strings.Index(joined, `drop table if exists "public"."a"`),
		strings.Index(joined, `drop table if exists "public"."b"`),
		strings.Index(joined, `drop table if exists "public"."c"`),
	}
	for _, d := range drops {
		if d < 0 {
			t.Fatalf("all tables must drop:\n%s", joined)
		}
	}
	constraintDrop := strings.Index(joined, "drop constraint")
	if constraintDrop == -1 || constraintDrop > drops[0] {
		t.Fatalf("constraint drops must precede table drops:\n%s", joined)
	}
}

func TestDiffV2DropsRequireAcknowledgement(t *testing.T) {
	actual := docFromModelForTest(t, V2DocumentModel{
		Version: 2, Dialect: "postgresql", Capabilities: []string{},
		Schemas: []V2SchemaDecl{{Name: "public"}},
		Tables: []V2Table{{
			Identity: V2Identity{Schema: "public", Name: "orphan"}, Managed: true,
			Columns: []V2Column{intColumn("id", true)},
			Constraints: []V2Constraint{
				{Name: "orphan_pkey", Type: "primary-key", Columns: []string{"id"}},
			},
		}},
		Enums: []V2EnumDecl{}, Views: []V2View{}, Opaque: []V2Opaque{},
	})
	result, err := DiffV2Document(context.Background(), emptyDBDoc(t), actual, DiffV2Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Up) != 0 {
		t.Fatalf("no drop without acknowledgement: %v", result.Up)
	}
	if len(result.Warnings) != 1 || !strings.Contains(result.Warnings[0], "--allow-destructive") {
		t.Fatalf("expected left-untouched warning, got %v", result.Warnings)
	}
}

func TestDiffV2EnumValueRules(t *testing.T) {
	enumsDoc := func(values []string) *V2Document {
		return docFromModelForTest(t, V2DocumentModel{
			Version: 2, Dialect: "postgresql", Capabilities: []string{},
			Schemas: []V2SchemaDecl{{Name: "public"}},
			Tables: []V2Table{{
				Identity: V2Identity{Schema: "public", Name: "t"}, Managed: true,
				Columns: []V2Column{{
					Name: "id", NotNull: true,
					Type: V2ColumnType{Name: "enum", Codec: "enum", Enum: &V2Identity{Schema: "public", Name: "mood"}},
				}},
				Constraints: []V2Constraint{{Name: "t_pkey", Type: "primary-key", Columns: []string{"id"}}},
			}},
			Enums: []V2EnumDecl{{Identity: V2Identity{Schema: "public", Name: "mood"}, Managed: true, Values: values}},
			Views: []V2View{}, Opaque: []V2Opaque{},
		})
	}
	actual := enumsDoc([]string{"sad", "ok"})

	// Append at the end.
	res, err := DiffV2Document(context.Background(), enumsDoc([]string{"sad", "ok", "happy"}), actual, DiffV2Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Up) != 1 || !strings.Contains(res.Up[0], `alter type "public"."mood" add value 'happy'`) {
		t.Fatalf("expected plain append, got %v", res.Up)
	}

	// Insert before an existing value.
	res, err = DiffV2Document(context.Background(), enumsDoc([]string{"meh", "sad", "ok"}), actual, DiffV2Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Up) != 1 || !strings.Contains(res.Up[0], `add value 'meh' before 'sad'`) {
		t.Fatalf("expected anchored insert, got %v", res.Up)
	}

	// Removal is unrepresentable.
	_, err = DiffV2Document(context.Background(), enumsDoc([]string{"ok"}), actual, DiffV2Options{})
	if err == nil || !strings.Contains(err.Error(), "cannot remove enum values") {
		t.Fatalf("expected removal rejection, got %v", err)
	}

	// Reorder is unrepresentable.
	_, err = DiffV2Document(context.Background(), enumsDoc([]string{"ok", "sad"}), actual, DiffV2Options{})
	if err == nil || !strings.Contains(err.Error(), "cannot reorder") {
		t.Fatalf("expected reorder rejection, got %v", err)
	}
}

func TestDiffV2ColumnReorderRejected(t *testing.T) {
	table := func(order []string) V2Table {
		cols := make([]V2Column, 0, len(order))
		for _, n := range order {
			cols = append(cols, intColumn(n, n == "id"))
		}
		return V2Table{
			Identity: V2Identity{Schema: "public", Name: "t"}, Managed: true,
			Columns: cols,
			Constraints: []V2Constraint{
				{Name: "t_pkey", Type: "primary-key", Columns: []string{"id"}},
			},
		}
	}
	desired := docFromModelForTest(t, V2DocumentModel{
		Version: 2, Dialect: "postgresql", Capabilities: []string{},
		Schemas: []V2SchemaDecl{{Name: "public"}},
		Tables:  []V2Table{table([]string{"id", "b", "a"})},
		Enums:   []V2EnumDecl{}, Views: []V2View{}, Opaque: []V2Opaque{},
	})
	actual := docFromModelForTest(t, V2DocumentModel{
		Version: 2, Dialect: "postgresql", Capabilities: []string{},
		Schemas: []V2SchemaDecl{{Name: "public"}},
		Tables:  []V2Table{table([]string{"id", "a", "b"})},
		Enums:   []V2EnumDecl{}, Views: []V2View{}, Opaque: []V2Opaque{},
	})
	_, err := DiffV2Document(context.Background(), desired, actual, DiffV2Options{})
	if err == nil || !strings.Contains(err.Error(), "cannot reorder columns") {
		t.Fatalf("expected column-order rejection, got %v", err)
	}
}

func TestDiffV2ExtensionAndUnsupportedBlockers(t *testing.T) {
	desiredTable := V2Table{
		Identity: V2Identity{Schema: "public", Name: "ext_t"}, Managed: true,
		Columns: []V2Column{intColumn("id", true)},
		Constraints: []V2Constraint{
			{Name: "ext_t_pkey", Type: "primary-key", Columns: []string{"id"}},
		},
	}
	desired := docFromModelForTest(t, V2DocumentModel{
		Version: 2, Dialect: "postgresql", Capabilities: []string{},
		Schemas: []V2SchemaDecl{{Name: "public"}},
		Tables:  []V2Table{desiredTable},
		Enums:   []V2EnumDecl{}, Views: []V2View{}, Opaque: []V2Opaque{},
	})

	extActual := docFromModelForTest(t, V2DocumentModel{
		Version: 2, Dialect: "postgresql", Capabilities: []string{},
		Schemas: []V2SchemaDecl{{Name: "public"}},
		Tables:  []V2Table{},
		Enums:   []V2EnumDecl{}, Views: []V2View{},
		Opaque: []V2Opaque{{
			Kind: "extension-table", Identity: V2Identity{Schema: "public", Name: "ext_t"},
			Owner: "some_ext", Reason: "owned by extension some_ext",
		}},
	})
	_, err := DiffV2Document(context.Background(), desired, extActual, DiffV2Options{AllowDestructive: true})
	if err == nil || !strings.Contains(err.Error(), "extension-owned") {
		t.Fatalf("expected extension-owned rejection, got %v", err)
	}

	unsupportedActual := docFromModelForTest(t, V2DocumentModel{
		Version: 2, Dialect: "postgresql", Capabilities: []string{},
		Schemas: []V2SchemaDecl{{Name: "public"}},
		Tables:  []V2Table{},
		Enums:   []V2EnumDecl{}, Views: []V2View{},
		Opaque: []V2Opaque{{
			Kind: "unsupported-table", Identity: V2Identity{Schema: "public", Name: "ext_t"},
			Reason: "column \"g\" is a generated column (not representable in schema document v2)",
		}},
	})
	_, err = DiffV2Document(context.Background(), desired, unsupportedActual, DiffV2Options{AllowDestructive: true})
	if err == nil || !strings.Contains(err.Error(), "generated column") {
		t.Fatalf("expected unsupported-structure rejection naming the reason, got %v", err)
	}
}

func TestDiffV2OutOfScopeObjectsUntouched(t *testing.T) {
	actual := docFromModelForTest(t, V2DocumentModel{
		Version: 2, Dialect: "postgresql", Capabilities: []string{},
		Schemas: []V2SchemaDecl{{Name: "public"}, {Name: "other"}},
		Tables: []V2Table{{
			Identity: V2Identity{Schema: "other", Name: "foreign_table"}, Managed: true,
			Columns: []V2Column{intColumn("id", true)},
			Constraints: []V2Constraint{
				{Name: "foreign_table_pkey", Type: "primary-key", Columns: []string{"id"}},
			},
		}},
		Enums: []V2EnumDecl{}, Views: []V2View{}, Opaque: []V2Opaque{},
	})
	// Desired manages public only.
	result, err := DiffV2Document(context.Background(), emptyDBDoc(t), actual, DiffV2Options{AllowDestructive: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Up) != 0 {
		t.Fatalf("out-of-scope tables must never be planned: %v", result.Up)
	}
	found := false
	for _, w := range result.Warnings {
		if strings.Contains(w, "outside the schemas managed") {
			found = true
		}
	}
	if !found {
		t.Fatalf("out-of-scope table must be reported, got %v", result.Warnings)
	}
}

func TestDiffV2SameNameIndexChangeDropsAndRecreates(t *testing.T) {
	table := func(where *string) V2Table {
		return V2Table{
			Identity: V2Identity{Schema: "public", Name: "t"}, Managed: true,
			Columns: []V2Column{intColumn("id", true)},
			Constraints: []V2Constraint{
				{Name: "t_pkey", Type: "primary-key", Columns: []string{"id"}},
			},
			Indexes: []V2Index{{
				Identity: V2Identity{Schema: "public", Name: "t_idx"}, Unique: false, Method: "btree",
				Key:   []V2IndexKeyPart{{Column: strPtrV2("id")}},
				Where: where,
			}},
		}
	}
	desired := docFromModelForTest(t, V2DocumentModel{
		Version: 2, Dialect: "postgresql", Capabilities: []string{},
		Schemas: []V2SchemaDecl{{Name: "public"}},
		Tables:  []V2Table{table(strPtrV2("id > 1"))},
		Enums:   []V2EnumDecl{}, Views: []V2View{}, Opaque: []V2Opaque{},
	})
	actual := docFromModelForTest(t, V2DocumentModel{
		Version: 2, Dialect: "postgresql", Capabilities: []string{},
		Schemas: []V2SchemaDecl{{Name: "public"}},
		Tables:  []V2Table{table(nil)},
		Enums:   []V2EnumDecl{}, Views: []V2View{}, Opaque: []V2Opaque{},
	})
	result, err := DiffV2Document(context.Background(), desired, actual, DiffV2Options{})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(result.Up, "\n")
	if !strings.Contains(joined, `drop index if exists "public"."t_idx"`) {
		t.Fatalf("changed same-name index must be dropped:\n%s", joined)
	}
	if !strings.Contains(joined, `create index "t_idx" on "public"."t" using btree ("id") where id > 1`) {
		t.Fatalf("changed same-name index must be recreated with the new predicate:\n%s", joined)
	}
}

func TestDiffV2RenameValidation(t *testing.T) {
	dt := V2Table{
		Identity: V2Identity{Schema: "public", Name: "t"}, Managed: true,
		Columns: []V2Column{intColumn("new_name", true), intColumn("kept", false)},
		Constraints: []V2Constraint{
			{Name: "t_pkey", Type: "primary-key", Columns: []string{"new_name"}},
		},
	}
	at := V2Table{
		Identity: V2Identity{Schema: "public", Name: "t"}, Managed: true,
		Columns: []V2Column{intColumn("old_name", true), intColumn("kept", false)},
		Constraints: []V2Constraint{
			{Name: "t_pkey", Type: "primary-key", Columns: []string{"old_name"}},
		},
	}
	desired := docFromModelForTest(t, V2DocumentModel{
		Version: 2, Dialect: "postgresql", Capabilities: []string{},
		Schemas: []V2SchemaDecl{{Name: "public"}},
		Tables:  []V2Table{dt}, Enums: []V2EnumDecl{}, Views: []V2View{}, Opaque: []V2Opaque{},
	})
	actual := docFromModelForTest(t, V2DocumentModel{
		Version: 2, Dialect: "postgresql", Capabilities: []string{},
		Schemas: []V2SchemaDecl{{Name: "public"}},
		Tables:  []V2Table{at}, Enums: []V2EnumDecl{}, Views: []V2View{}, Opaque: []V2Opaque{},
	})

	// Valid rename plans RENAME and nothing destructive.
	res, err := DiffV2Document(context.Background(), desired, actual, DiffV2Options{
		Renames: map[string]string{"public.t.new_name": "old_name"},
	})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(res.Up, "\n")
	if !strings.Contains(joined, `alter table "public"."t" rename column "old_name" to "new_name"`) {
		t.Fatalf("rename missing:\n%s", joined)
	}
	if strings.Contains(joined, "drop column") {
		t.Fatalf("rename must not plan a drop:\n%s", joined)
	}

	// Ambiguity: source still declared in desired.
	_, err = DiffV2Document(context.Background(), desired, actual, DiffV2Options{
		Renames: map[string]string{"public.t.kept": "old_name"},
	})
	if err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("expected ambiguity rejection, got %v", err)
	}

	// Forgery: source does not exist in the database.
	_, err = DiffV2Document(context.Background(), desired, actual, DiffV2Options{
		Renames: map[string]string{"public.t.new_name": "nope"},
	})
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("expected forged-source rejection, got %v", err)
	}
}

func TestDiffV2ProtectedTablesNeverPlanned(t *testing.T) {
	actual := docFromModelForTest(t, V2DocumentModel{
		Version: 2, Dialect: "postgresql", Capabilities: []string{},
		Schemas: []V2SchemaDecl{{Name: "public"}},
		Tables: []V2Table{{
			Identity: V2Identity{Schema: "public", Name: "_neutron_migrations"}, Managed: true,
			Columns: []V2Column{
				{Name: "id", Type: V2ColumnType{Name: "int4", Codec: "number"}, NotNull: true},
			},
			Constraints: []V2Constraint{
				{Name: "_neutron_migrations_pkey", Type: "primary-key", Columns: []string{"id"}},
			},
		}},
		Enums: []V2EnumDecl{}, Views: []V2View{}, Opaque: []V2Opaque{},
	})
	result, err := DiffV2Document(context.Background(), emptyDBDoc(t), actual, DiffV2Options{AllowDestructive: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Up) != 0 {
		t.Fatalf("internal metadata must never be dropped: %v", result.Up)
	}
	found := false
	for _, w := range result.Warnings {
		if strings.Contains(w, "neutron-internal") {
			found = true
		}
	}
	if !found {
		t.Fatalf("internal table must be reported, got %v", result.Warnings)
	}

	// Declaring it managed in desired is rejected outright.
	desired := docFromModelForTest(t, V2DocumentModel{
		Version: 2, Dialect: "postgresql", Capabilities: []string{},
		Schemas: []V2SchemaDecl{{Name: "public"}},
		Tables: []V2Table{{
			Identity: V2Identity{Schema: "public", Name: "_neutron_outcomes"}, Managed: true,
			Columns: []V2Column{intColumn("id", true)},
			Constraints: []V2Constraint{
				{Name: "_neutron_outcomes_pkey", Type: "primary-key", Columns: []string{"id"}},
			},
		}},
		Enums: []V2EnumDecl{}, Views: []V2View{}, Opaque: []V2Opaque{},
	})
	_, err = DiffV2Document(context.Background(), desired, emptyDBDoc(t), DiffV2Options{})
	if err == nil || !strings.Contains(err.Error(), "neutron-internal") {
		t.Fatalf("expected internal-name rejection, got %v", err)
	}
}

func TestDiffV2DefaultAndNullabilityTransitions(t *testing.T) {
	mk := func(def *V2ColumnDefault, notNull bool) *V2Document {
		return docFromModelForTest(t, V2DocumentModel{
			Version: 2, Dialect: "postgresql", Capabilities: []string{},
			Schemas: []V2SchemaDecl{{Name: "public"}},
			Tables: []V2Table{{
				Identity: V2Identity{Schema: "public", Name: "t"}, Managed: true,
				Columns: []V2Column{{
					Name: "c", NotNull: notNull,
					Type:    V2ColumnType{Name: "int4", Codec: "number"},
					Default: def,
				}},
				Constraints: []V2Constraint{},
			}},
			Enums: []V2EnumDecl{}, Views: []V2View{}, Opaque: []V2Opaque{},
		})
	}
	actual := mk(&V2ColumnDefault{Kind: "literal", SQL: strPtrV2("1")}, true)
	desired := mk(&V2ColumnDefault{Kind: "identity", Generated: strPtrV2("always")}, false)

	res, err := DiffV2Document(context.Background(), desired, actual, DiffV2Options{})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(res.Up, "\n")
	if !strings.Contains(joined, `alter table "public"."t" alter column "c" drop default`) {
		t.Fatalf("literal->identity must drop the old default first:\n%s", joined)
	}
	if !strings.Contains(joined, `alter column "c" add generated always as identity`) {
		t.Fatalf("identity must be added:\n%s", joined)
	}
	if !strings.Contains(joined, `alter column "c" drop not null`) {
		t.Fatalf("nullability change missing:\n%s", joined)
	}
}

func TestDiffV2ChangedFKIsDropAndAdd(t *testing.T) {
	table := func(action string) V2Table {
		return V2Table{
			Identity: V2Identity{Schema: "public", Name: "t"}, Managed: true,
			Columns: []V2Column{intColumn("id", true), intColumn("u", false)},
			Constraints: []V2Constraint{
				{Name: "t_pkey", Type: "primary-key", Columns: []string{"id"}},
				{Name: "t_u_fkey", Type: "foreign-key", Columns: []string{"u"},
					References: &V2FKReference{
						Table:    V2Identity{Schema: "public", Name: "u"},
						Columns:  []string{"id"},
						OnDelete: strPtrV2(action),
					}},
			},
		}
	}
	users := V2Table{
		Identity: V2Identity{Schema: "public", Name: "u"}, Managed: true,
		Columns: []V2Column{intColumn("id", true)},
		Constraints: []V2Constraint{
			{Name: "u_pkey", Type: "primary-key", Columns: []string{"id"}},
		},
	}
	doc := func(action string) *V2Document {
		return docFromModelForTest(t, V2DocumentModel{
			Version: 2, Dialect: "postgresql", Capabilities: []string{},
			Schemas: []V2SchemaDecl{{Name: "public"}},
			Tables:  []V2Table{table(action), users},
			Enums:   []V2EnumDecl{}, Views: []V2View{}, Opaque: []V2Opaque{},
		})
	}
	// desired: cascade; actual (live): restrict.
	res, err := DiffV2Document(context.Background(), doc("cascade"), doc("restrict"), DiffV2Options{})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(res.Up, "\n")
	if !strings.Contains(joined, `alter table "public"."t" drop constraint if exists "t_u_fkey"`) {
		t.Fatalf("changed FK must drop the old constraint:\n%s", joined)
	}
	if !strings.Contains(joined, `add constraint "t_u_fkey" foreign key ("u") references "public"."u" ("id") on delete cascade`) {
		t.Fatalf("changed FK must re-add with the new action:\n%s", joined)
	}
}

func TestDiffV2ViewPlanning(t *testing.T) {
	view := func(def string, check *string) V2View {
		return V2View{
			Identity: V2Identity{Schema: "public", Name: "v"}, Managed: true,
			Definition: def, CheckOption: check,
		}
	}
	base := V2Table{
		Identity: V2Identity{Schema: "public", Name: "t"}, Managed: true,
		Columns: []V2Column{intColumn("id", true)},
		Constraints: []V2Constraint{
			{Name: "t_pkey", Type: "primary-key", Columns: []string{"id"}},
		},
	}
	doc := func(v V2View, withView bool) *V2Document {
		m := V2DocumentModel{
			Version: 2, Dialect: "postgresql", Capabilities: []string{},
			Schemas: []V2SchemaDecl{{Name: "public"}},
			Tables:  []V2Table{base}, Enums: []V2EnumDecl{},
			Views: []V2View{}, Opaque: []V2Opaque{},
		}
		if withView {
			m.Views = append(m.Views, v)
		}
		return docFromModelForTest(t, m)
	}

	res, err := DiffV2Document(context.Background(), doc(view("select id from public.t", strPtrV2("cascaded")), true), doc(view("select id from public.t", nil), true), DiffV2Options{})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(res.Up, "\n")
	if !strings.Contains(joined, `drop view if exists "public"."v"`) ||
		!strings.Contains(joined, `create view "public"."v" with (check_option = cascaded) as select id from public.t`) {
		t.Fatalf("changed view must be dropped and recreated with options:\n%s", joined)
	}
}

func TestDiffV2DesiredTableOverUnsupportedObjectBlocks(t *testing.T) {
	desired := docFromModelForTest(t, V2DocumentModel{
		Version: 2, Dialect: "postgresql", Capabilities: []string{},
		Schemas: []V2SchemaDecl{{Name: "public"}},
		Tables: []V2Table{{
			Identity: V2Identity{Schema: "public", Name: "mv"}, Managed: true,
			Columns: []V2Column{intColumn("id", true)},
			Constraints: []V2Constraint{
				{Name: "mv_pkey", Type: "primary-key", Columns: []string{"id"}},
			},
		}},
		Enums: []V2EnumDecl{}, Views: []V2View{}, Opaque: []V2Opaque{},
	})
	actual := docFromModelForTest(t, V2DocumentModel{
		Version: 2, Dialect: "postgresql", Capabilities: []string{},
		Schemas: []V2SchemaDecl{{Name: "public"}},
		Tables:  []V2Table{}, Enums: []V2EnumDecl{}, Views: []V2View{},
		Opaque: []V2Opaque{{
			Kind: "unsupported-object", Identity: V2Identity{Schema: "public", Name: "mv"},
			Reason: "materialized view (not representable in schema document v2)",
		}},
	})
	_, err := DiffV2Document(context.Background(), desired, actual, DiffV2Options{AllowDestructive: true})
	if err == nil || !strings.Contains(err.Error(), "materialized view") {
		t.Fatalf("expected unsupported-object blocker, got %v", err)
	}
}

// TestDiffV2NoActionAndSimpleMatchFKConverge pins the MAJOR-1 rework: the
// contract admits "no action" and "simple" spellings, and the catalog
// cannot distinguish them from omitted clauses, so they must compare equal
// to the introspected-absent form instead of replaying drop+add forever.
func TestDiffV2NoActionAndSimpleMatchFKConverge(t *testing.T) {
	fkDoc := func(onDelete, match *string) *V2Document {
		ref := &V2FKReference{Table: V2Identity{Schema: "public", Name: "u"}, Columns: []string{"id"}}
		if onDelete != nil {
			ref.OnDelete = onDelete
		}
		if match != nil {
			ref.Match = match
		}
		return docFromModelForTest(t, V2DocumentModel{
			Version: 2, Dialect: "postgresql", Capabilities: []string{},
			Schemas: []V2SchemaDecl{{Name: "public"}},
			Tables: []V2Table{
				{Identity: V2Identity{Schema: "public", Name: "u"}, Managed: true,
					Columns: []V2Column{intColumn("id", true)},
					Constraints: []V2Constraint{
						{Name: "u_pkey", Type: "primary-key", Columns: []string{"id"}},
					}},
				{Identity: V2Identity{Schema: "public", Name: "t"}, Managed: true,
					Columns: []V2Column{intColumn("id", true), intColumn("u", false)},
					Constraints: []V2Constraint{
						{Name: "t_pkey", Type: "primary-key", Columns: []string{"id"}},
						{Name: "t_u_fkey", Type: "foreign-key", Columns: []string{"u"}, References: ref},
					}},
			},
			Enums: []V2EnumDecl{}, Views: []V2View{}, Opaque: []V2Opaque{},
		})
	}

	cases := []struct {
		name             string
		desiredOnDelete  *string
		desiredMatch     *string
		actualOnDelete   *string
		actualMatch      *string
		wantStatements   int
		wantUnverified   bool
		descriptionOfFix string
	}{
		{"desired no action vs live absent", strPtrV2("no action"), nil, nil, nil, 0, false, "perpetual drop+add"},
		{"desired simple vs live absent", nil, strPtrV2("simple"), nil, nil, 0, false, "perpetual drop+add"},
		{"desired absent vs live no action", nil, nil, strPtrV2("no action"), nil, 0, false, "perpetual drop+add"},
		{"desired no action + simple vs live absent", strPtrV2("no action"), strPtrV2("simple"), nil, nil, 0, false, "perpetual drop+add"},
		{"real difference still planned: no action vs restrict", strPtrV2("no action"), nil, strPtrV2("restrict"), nil, 2, false, "must not mask real action changes"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := DiffV2Document(context.Background(), fkDoc(tc.desiredOnDelete, tc.desiredMatch), fkDoc(tc.actualOnDelete, tc.actualMatch), DiffV2Options{})
			if err != nil {
				t.Fatal(err)
			}
			if len(res.Up) != tc.wantStatements {
				t.Fatalf("%s: want %d statements, got %d: %v", tc.descriptionOfFix, tc.wantStatements, len(res.Up), res.Up)
			}
		})
	}
}

// TestDiffV2ColumnReorderWithAddRejected pins the MAJOR-3 rework: a column
// reorder must be rejected even when a column is added simultaneously (the
// length guard used to let it through, applying a plan the next run
// rejected). Reviewer scenario: live (a,b,c), desired (b,a,c,d).
func TestDiffV2ColumnReorderWithAddRejected(t *testing.T) {
	table := func(order []string) V2Table {
		cols := make([]V2Column, 0, len(order))
		for i, n := range order {
			cols = append(cols, intColumn(n, i == 0))
		}
		return V2Table{
			Identity: V2Identity{Schema: "public", Name: "t"}, Managed: true,
			Columns: cols,
			Constraints: []V2Constraint{
				{Name: "t_pkey", Type: "primary-key", Columns: []string{order[0]}},
			},
		}
	}
	doc := func(order []string) *V2Document {
		return docFromModelForTest(t, V2DocumentModel{
			Version: 2, Dialect: "postgresql", Capabilities: []string{},
			Schemas: []V2SchemaDecl{{Name: "public"}},
			Tables:  []V2Table{table(order)},
			Enums:   []V2EnumDecl{}, Views: []V2View{}, Opaque: []V2Opaque{},
		})
	}

	// Reorder + add: must reject at plan time.
	_, err := DiffV2Document(context.Background(), doc([]string{"b", "a", "c", "d"}), doc([]string{"a", "b", "c"}), DiffV2Options{})
	if err == nil || !strings.Contains(err.Error(), "cannot reorder columns") {
		t.Fatalf("reorder+add must be rejected with the reorder error, got: %v", err)
	}

	// Reorder + drop (equal lengths, changed set): also rejected now.
	_, err = DiffV2Document(context.Background(), doc([]string{"b", "a", "c"}), doc([]string{"a", "b", "c", "d"}), DiffV2Options{})
	if err == nil || !strings.Contains(err.Error(), "cannot reorder columns") {
		t.Fatalf("reorder+drop must be rejected with the reorder error, got: %v", err)
	}

	// Plain add at the end and add in the middle stay plannable.
	res, err := DiffV2Document(context.Background(), doc([]string{"a", "b", "c", "d"}), doc([]string{"a", "b", "c"}), DiffV2Options{})
	if err != nil {
		t.Fatalf("plain append must plan: %v", err)
	}
	if len(res.Up) != 1 || !strings.Contains(res.Up[0], `add column "d"`) {
		t.Fatalf("plain append must add the column, got %v", res.Up)
	}
	res, err = DiffV2Document(context.Background(), doc([]string{"a", "d", "b", "c"}), doc([]string{"a", "b", "c"}), DiffV2Options{})
	if err != nil {
		t.Fatalf("insert-between must plan (matched order preserved): %v", err)
	}
	if len(res.Up) != 1 || !strings.Contains(res.Up[0], `add column "d"`) {
		t.Fatalf("insert-between must add the column, got %v", res.Up)
	}
}

// TestDiffV2TwinFailureFlagsChecksDefaultsAndAllIndexFields pins the
// MAJOR-2 rework: without a catalog oracle (no normalizer — the offline
// stand-in for a failed twin), textual differences of check expressions,
// column defaults and EVERY differing index field must be flagged
// "equivalence not verified", never silently planned.
func TestDiffV2TwinFailureFlagsChecksDefaultsAndAllIndexFields(t *testing.T) {
	mk := func(check string, def *V2ColumnDefault, keyExpr, where *string) *V2Document {
		idx := V2Index{Identity: V2Identity{Schema: "public", Name: "t_idx"}, Method: "btree"}
		if keyExpr != nil {
			idx.Key = []V2IndexKeyPart{{Expression: keyExpr}}
			idx.Where = where
		}
		return docFromModelForTest(t, V2DocumentModel{
			Version: 2, Dialect: "postgresql", Capabilities: []string{},
			Schemas: []V2SchemaDecl{{Name: "public"}},
			Tables: []V2Table{{
				Identity: V2Identity{Schema: "public", Name: "t"}, Managed: true,
				Columns: []V2Column{
					{Name: "slug", Type: V2ColumnType{Name: "text", Codec: "string"}, NotNull: true},
					{Name: "ts", Type: V2ColumnType{Name: "timestamptz", Codec: "timestamptz-string"}, NotNull: true, Default: def},
				},
				Constraints: []V2Constraint{
					{Name: "t_slug_check", Type: "check", Expression: strPtrV2(check)},
				},
				Indexes: []V2Index{idx},
			}},
			Enums: []V2EnumDecl{}, Views: []V2View{}, Opaque: []V2Opaque{},
		})
	}
	desired := mk("length(slug) > 0", &V2ColumnDefault{Kind: "expression", SQL: strPtrV2("CURRENT_TIMESTAMP")},
		strPtrV2("lower(slug)"), strPtrV2("slug <> ''"))
	// Live spellings: the catalog's deparse forms of the same expressions.
	actual := mk("length((slug)::text) > 0", &V2ColumnDefault{Kind: "expression", SQL: strPtrV2("now()")},
		strPtrV2("lower((slug)::text)"), strPtrV2("((slug)::text <> ''::text)"))

	res, err := DiffV2Document(context.Background(), desired, actual, DiffV2Options{})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(res.Up, "\n")
	// Strict-text fallback still plans the changes (never silently equal)...
	for _, want := range []string{
		`drop constraint if exists "t_slug_check"`,
		`alter table "public"."t" alter column "ts" set default CURRENT_TIMESTAMP`,
		`drop index if exists "public"."t_idx"`,
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("strict fallback must still plan %q:\n%s", want, joined)
		}
	}
	// ...but every textual comparison is flagged as unverified.
	for _, want := range []string{
		"equivalence not verified for check constraint t_slug_check expression",
		"equivalence not verified for column public.t.ts default",
		"equivalence not verified for index public.t_idx key part",
		"equivalence not verified for index public.t_idx predicate",
	} {
		found := false
		for _, w := range res.Warnings {
			if strings.Contains(w, want) {
				found = true
			}
		}
		if !found {
			t.Fatalf("missing unverified warning %q; warnings: %v", want, res.Warnings)
		}
	}
}

// TestDiffV2TableAndViewOverEnumBlocked pins the MINOR-2 rework: the
// desired-object vs live-enum identity collisions fail fast at plan time,
// symmetric with the enum-over-table/view checks.
func TestDiffV2TableAndViewOverEnumBlocked(t *testing.T) {
	withEnum := docFromModelForTest(t, V2DocumentModel{
		Version: 2, Dialect: "postgresql", Capabilities: []string{},
		Schemas: []V2SchemaDecl{{Name: "public"}},
		Tables:  []V2Table{},
		Enums:   []V2EnumDecl{{Identity: V2Identity{Schema: "public", Name: "color"}, Managed: true, Values: []string{"red"}}},
		Views:   []V2View{}, Opaque: []V2Opaque{},
	})
	tableOverEnum := docFromModelForTest(t, V2DocumentModel{
		Version: 2, Dialect: "postgresql", Capabilities: []string{},
		Schemas: []V2SchemaDecl{{Name: "public"}},
		Tables: []V2Table{{
			Identity: V2Identity{Schema: "public", Name: "color"}, Managed: true,
			Columns: []V2Column{intColumn("id", true)},
			Constraints: []V2Constraint{
				{Name: "color_pkey", Type: "primary-key", Columns: []string{"id"}},
			},
		}},
		Enums: []V2EnumDecl{}, Views: []V2View{}, Opaque: []V2Opaque{},
	})
	_, err := DiffV2Document(context.Background(), tableOverEnum, withEnum, DiffV2Options{})
	if err == nil || !strings.Contains(err.Error(), "is an enum type in the database but a table") {
		t.Fatalf("table-over-enum must block at plan time, got: %v", err)
	}

	viewOverEnum := docFromModelForTest(t, V2DocumentModel{
		Version: 2, Dialect: "postgresql", Capabilities: []string{},
		Schemas: []V2SchemaDecl{{Name: "public"}},
		Tables:  []V2Table{},
		Enums:   []V2EnumDecl{},
		Views:   []V2View{{Identity: V2Identity{Schema: "public", Name: "color"}, Managed: true, Definition: "select 1"}},
		Opaque:  []V2Opaque{},
	})
	_, err = DiffV2Document(context.Background(), viewOverEnum, withEnum, DiffV2Options{})
	if err == nil || !strings.Contains(err.Error(), "is an enum type in the database but a view") {
		t.Fatalf("view-over-enum must block at plan time, got: %v", err)
	}

	// View over a live unsupported-table identity (e.g. a partitioned
	// table) also blocks at plan time.
	actualUnsupported := docFromModelForTest(t, V2DocumentModel{
		Version: 2, Dialect: "postgresql", Capabilities: []string{},
		Schemas: []V2SchemaDecl{{Name: "public"}},
		Tables:  []V2Table{}, Enums: []V2EnumDecl{}, Views: []V2View{},
		Opaque: []V2Opaque{{
			Kind: "unsupported-table", Identity: V2Identity{Schema: "public", Name: "part"},
			Reason: "partitioned table (partitioning is not representable in schema document v2)",
		}},
	})
	viewOverTable := docFromModelForTest(t, V2DocumentModel{
		Version: 2, Dialect: "postgresql", Capabilities: []string{},
		Schemas: []V2SchemaDecl{{Name: "public"}},
		Tables:  []V2Table{}, Enums: []V2EnumDecl{},
		Views:  []V2View{{Identity: V2Identity{Schema: "public", Name: "part"}, Managed: true, Definition: "select 1"}},
		Opaque: []V2Opaque{},
	})
	_, err = DiffV2Document(context.Background(), viewOverTable, actualUnsupported, DiffV2Options{})
	if err == nil || !strings.Contains(err.Error(), "unrepresentable table") {
		t.Fatalf("view-over-unsupported-table must block at plan time, got: %v", err)
	}
}

// TestDiffV2UnchangedViewRecreatedAroundAltersWarns pins the MINOR-1
// rework: when table alterations force unchanged views through the
// drop+recreate round-trip, the plan must warn that unmodeled properties
// (privileges, comments) do not survive.
func TestDiffV2UnchangedViewRecreatedAroundAltersWarns(t *testing.T) {
	mk := func(extraCol bool) *V2Document {
		cols := []V2Column{intColumn("id", true)}
		if extraCol {
			cols = append(cols, intColumn("extra", false))
		}
		return docFromModelForTest(t, V2DocumentModel{
			Version: 2, Dialect: "postgresql", Capabilities: []string{},
			Schemas: []V2SchemaDecl{{Name: "public"}},
			Tables: []V2Table{{
				Identity: V2Identity{Schema: "public", Name: "t"}, Managed: true,
				Columns: cols,
				Constraints: []V2Constraint{
					{Name: "t_pkey", Type: "primary-key", Columns: []string{"id"}},
				},
			}},
			Enums: []V2EnumDecl{},
			Views: []V2View{{Identity: V2Identity{Schema: "public", Name: "v"}, Managed: true,
				Definition: "select id from public.t"}},
			Opaque: []V2Opaque{},
		})
	}
	res, err := DiffV2Document(context.Background(), mk(true), mk(false), DiffV2Options{})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(res.Up, "\n")
	if !strings.Contains(joined, `drop view if exists "public"."v"`) || !strings.Contains(joined, `create view "public"."v" as select id from public.t`) {
		t.Fatalf("unchanged view is still recreated around alterations:\n%s", joined)
	}
	found := false
	for _, w := range res.Warnings {
		if strings.Contains(w, `view public.v is unchanged but is dropped and recreated`) {
			found = true
		}
	}
	if !found {
		t.Fatalf("recreated unchanged view must be warned about; warnings: %v", res.Warnings)
	}
}
