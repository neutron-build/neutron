package studio

import (
	"reflect"
	"strings"
	"testing"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// Offline tests for the S05 designer edit layer: every expectation below is
// stated from PostgreSQL's DROP/RENAME COLUMN semantics and the schema
// document contract, not read back from the implementation.

func int4() db.V2ColumnType { return db.V2ColumnType{Name: "int4", Codec: "number"} }
func text() db.V2ColumnType { return db.V2ColumnType{Name: "text", Codec: "string"} }

// fixtureModel: public.parent (a, b, c; PK a; UNIQUE (b, c); index on
// (c, a) including b) and public.child with a composite FK onto parent (b, c).
func fixtureModel() db.V2DocumentModel {
	col := func(name string) *string { return &name }
	return db.V2DocumentModel{
		Version:      db.SchemaDocumentVersionV2,
		Dialect:      "postgresql",
		Capabilities: []string{},
		Schemas:      []db.V2SchemaDecl{{Name: "public"}},
		Tables: []db.V2Table{
			{
				Identity: db.V2Identity{Schema: "public", Name: "parent"},
				Managed:  true,
				Columns: []db.V2Column{
					{Name: "a", Type: int4(), NotNull: true},
					{Name: "b", Type: int4()},
					{Name: "c", Type: text()},
					{Name: "d", Type: text()},
				},
				Constraints: []db.V2Constraint{
					{Name: "parent_pkey", Type: "primary-key", Columns: []string{"a"}},
					{Name: "parent_b_c_key", Type: "unique", Columns: []string{"b", "c"}},
				},
				Indexes: []db.V2Index{
					{Identity: db.V2Identity{Schema: "public", Name: "parent_c_a_idx"}, Method: "btree",
						Key: []db.V2IndexKeyPart{{Column: col("c")}, {Column: col("a")}}, Include: []string{"b"}},
					{Identity: db.V2Identity{Schema: "public", Name: "parent_d_idx"}, Method: "btree",
						Key: []db.V2IndexKeyPart{{Column: col("d")}}},
				},
			},
			{
				Identity: db.V2Identity{Schema: "public", Name: "child"},
				Managed:  true,
				Columns: []db.V2Column{
					{Name: "id", Type: int4(), NotNull: true},
					{Name: "pb", Type: int4()},
					{Name: "pc", Type: text()},
				},
				Constraints: []db.V2Constraint{
					{Name: "child_pkey", Type: "primary-key", Columns: []string{"id"}},
					{Name: "child_parent_fkey", Type: "foreign-key", Columns: []string{"pb", "pc"},
						References: &db.V2FKReference{Table: db.V2Identity{Schema: "public", Name: "parent"}, Columns: []string{"b", "c"}}},
				},
				Indexes: []db.V2Index{},
			},
		},
		Enums:  []db.V2EnumDecl{},
		Views:  []db.V2View{},
		Opaque: []db.V2Opaque{},
	}
}

func noDeps(string, string, string) ([]columnDependent, error) { return nil, nil }

func TestDesignerEditsNeverBleedIntoTheLiveModel(t *testing.T) {
	live := fixtureModel()
	before, err := documentFromModel(live)
	if err != nil {
		t.Fatalf("fixture invalid: %v", err)
	}
	desired := copyModel(live)
	renames := map[string]string{}
	if _, err := applySchemaChanges(&desired, []SchemaChange{
		{Op: "rename-column", Schema: "public", Table: "parent", From: "c", To: "c2"},
		{Op: "drop-column", Schema: "public", Table: "parent", Column: "d"},
		{Op: "alter-column-type", Schema: "public", Table: "child", Column: "id", Type: "bigint"},
	}, renames, noDeps); err != nil {
		t.Fatalf("apply: %v", err)
	}
	after, err := documentFromModel(live)
	if err != nil {
		t.Fatal(err)
	}
	if before.SHA256Hex != after.SHA256Hex {
		t.Fatalf("editing the desired copy changed the live model:\nbefore %s\nafter  %s", before.Canonical, after.Canonical)
	}
}

func TestDropColumnDropsWholeConstraintsAndIndexesLikePostgres(t *testing.T) {
	desired := copyModel(fixtureModel())
	// Remove the FK so parent.c is not referenced from outside.
	desired.Tables[1].Constraints = desired.Tables[1].Constraints[:1]
	notes, err := applySchemaChanges(&desired, []SchemaChange{
		{Op: "drop-column", Schema: "public", Table: "parent", Column: "c"},
	}, map[string]string{}, noDeps)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	parent := desired.Table(db.V2Identity{Schema: "public", Name: "parent"})
	// UNIQUE (b, c) and the (c, a) index are dropped whole: PostgreSQL never
	// narrows them to UNIQUE (b) / index (a).
	if parent.Constraint("parent_b_c_key") != nil || parent.Index("parent_c_a_idx") != nil {
		t.Fatalf("composite objects narrowed instead of dropped: %+v %+v", parent.Constraints, parent.Indexes)
	}
	if parent.Index("parent_d_idx") == nil || parent.PrimaryKey() == nil {
		t.Fatal("unrelated index or primary key dropped")
	}
	if parent.Column("c") != nil || len(parent.Columns) != 3 {
		t.Fatalf("columns = %+v", parent.Columns)
	}
	joined := strings.Join(notes, "\n")
	if !strings.Contains(joined, "parent_b_c_key") || !strings.Contains(joined, "parent_c_a_idx") {
		t.Fatalf("notes do not name the dropped dependents: %q", joined)
	}
	if _, err := documentFromModel(desired); err != nil {
		t.Fatalf("desired document invalid: %v", err)
	}

	// An INCLUDE-only participation drops the index as well.
	desired = copyModel(fixtureModel())
	desired.Tables[1].Constraints = desired.Tables[1].Constraints[:1]
	desired.Tables[0].Constraints = desired.Tables[0].Constraints[:1]
	if _, err := applySchemaChanges(&desired, []SchemaChange{{Op: "drop-column", Schema: "public", Table: "parent", Column: "b"}}, map[string]string{}, noDeps); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if desired.Table(db.V2Identity{Schema: "public", Name: "parent"}).Index("parent_c_a_idx") != nil {
		t.Fatal("index including the dropped column survived")
	}
}

func TestDropColumnRefusesWhatWouldNeedCascade(t *testing.T) {
	cases := []struct {
		name   string
		change SchemaChange
		deps   dependentsFunc
		want   string
	}{
		{"referenced by another table's FK", SchemaChange{Op: "drop-column", Schema: "public", Table: "parent", Column: "b"}, noDeps, "child_parent_fkey"},
		{"primary key column", SchemaChange{Op: "drop-column", Schema: "public", Table: "parent", Column: "a"}, noDeps, "primary key"},
		{"view depends on it", SchemaChange{Op: "drop-column", Schema: "public", Table: "parent", Column: "d"},
			func(string, string, string) ([]columnDependent, error) {
				return []columnDependent{{Kind: "view", Describe: "rule _RETURN on view public.v"}}, nil
			}, "public.v"},
		{"generated column depends on it", SchemaChange{Op: "drop-column", Schema: "public", Table: "parent", Column: "d"},
			func(string, string, string) ([]columnDependent, error) {
				return []columnDependent{{Kind: "default", SameTable: true, HasExpr: true, Describe: "default value for column e of table parent"}}, nil
			}, "column e"},
		{"table referenced by FK", SchemaChange{Op: "drop-table", Schema: "public", Table: "parent"}, noDeps, "child_parent_fkey"},
	}
	for _, tc := range cases {
		desired := copyModel(fixtureModel())
		_, err := applySchemaChanges(&desired, []SchemaChange{tc.change}, map[string]string{}, tc.deps)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want mention of %q", tc.name, err, tc.want)
		}
		var domain mutationDomainError
		if err != nil && !asDomain(err, &domain) {
			t.Errorf("%s: error is not a request (domain) error: %T", tc.name, err)
		}
	}

	// Catalog evidence of a same-table constraint the document does not
	// list by column (a CHECK expression) is dropped explicitly.
	desired := copyModel(fixtureModel())
	desired.Tables[0].Constraints = append(desired.Tables[0].Constraints, db.V2Constraint{Name: "parent_d_check", Type: "check", Expression: strPtr("(d <> '')")})
	notes, err := applySchemaChanges(&desired, []SchemaChange{{Op: "drop-column", Schema: "public", Table: "parent", Column: "d"}}, map[string]string{},
		func(string, string, string) ([]columnDependent, error) {
			return []columnDependent{
				{Kind: "constraint", Name: "parent_d_check", SameTable: true, HasExpr: true, Describe: "constraint parent_d_check on table parent"},
				{Kind: "default", SameTable: true, Own: true, HasExpr: true, Describe: "default value for column d"},
			}, nil
		})
	if err != nil {
		t.Fatalf("check-constraint drop: %v", err)
	}
	if desired.Tables[0].Constraint("parent_d_check") != nil || !strings.Contains(strings.Join(notes, "\n"), "parent_d_check") {
		t.Fatalf("check constraint not dropped explicitly: %+v notes %q", desired.Tables[0].Constraints, notes)
	}
}

func asDomain(err error, target *mutationDomainError) bool {
	d, ok := err.(mutationDomainError)
	if ok {
		*target = d
	}
	return ok
}

func TestRenameColumnFollowsEveryReferenceAndRecordsIntent(t *testing.T) {
	desired := copyModel(fixtureModel())
	renames := map[string]string{}
	if _, err := applySchemaChanges(&desired, []SchemaChange{
		{Op: "rename-column", Schema: "public", Table: "parent", From: "b", To: "bee"},
	}, renames, noDeps); err != nil {
		t.Fatalf("apply: %v", err)
	}
	parent := desired.Table(db.V2Identity{Schema: "public", Name: "parent"})
	child := desired.Table(db.V2Identity{Schema: "public", Name: "child"})
	if got := parent.Constraint("parent_b_c_key").Columns; !reflect.DeepEqual(got, []string{"bee", "c"}) {
		t.Fatalf("unique columns = %v", got)
	}
	if got := parent.Index("parent_c_a_idx").Include; !reflect.DeepEqual(got, []string{"bee"}) {
		t.Fatalf("index include = %v", got)
	}
	if got := child.Constraint("child_parent_fkey").References.Columns; !reflect.DeepEqual(got, []string{"bee", "c"}) {
		t.Fatalf("referencing FK columns = %v (another table's FK must follow the rename)", got)
	}
	if !reflect.DeepEqual(renames, map[string]string{"public.parent.bee": "b"}) {
		t.Fatalf("renames = %v (the diff's key format: schema.table.new -> old)", renames)
	}
	if got := renameFlags(renames); !reflect.DeepEqual(got, []string{"public.parent.b>public.parent.bee"}) {
		t.Fatalf("rename flags = %v (the CLI's --rename format)", got)
	}
	if _, err := documentFromModel(desired); err != nil {
		t.Fatalf("desired document invalid: %v", err)
	}

	// SQL text PostgreSQL rewrites on rename cannot be followed: refused.
	desired = copyModel(fixtureModel())
	_, err := applySchemaChanges(&desired, []SchemaChange{{Op: "rename-column", Schema: "public", Table: "parent", From: "d", To: "dd"}}, map[string]string{},
		func(string, string, string) ([]columnDependent, error) {
			return []columnDependent{{Kind: "index", Name: "parent_lower_d", SameTable: true, HasExpr: true, Describe: "index parent_lower_d"}}, nil
		})
	if err == nil || !strings.Contains(err.Error(), "parent_lower_d") {
		t.Fatalf("expression dependent not refused: %v", err)
	}
	// Renaming twice in one change set is ambiguous: refused.
	desired = copyModel(fixtureModel())
	_, err = applySchemaChanges(&desired, []SchemaChange{
		{Op: "rename-column", Schema: "public", Table: "parent", From: "d", To: "d1"},
		{Op: "rename-column", Schema: "public", Table: "parent", From: "d1", To: "d2"},
	}, map[string]string{}, noDeps)
	if err == nil || !strings.Contains(err.Error(), "already renamed") {
		t.Fatalf("double rename: %v", err)
	}
}

func TestCreateTableAndIndexNamesAreValidatedAgainstTheLiveNamespace(t *testing.T) {
	cases := []struct {
		name   string
		change SchemaChange
		want   string
	}{
		{"unknown schema", SchemaChange{Op: "create-table", Schema: "nope", Table: "t", Columns: []SchemaColumnInput{{Name: "id", Type: "integer"}}}, "does not exist"},
		{"name used by an index", SchemaChange{Op: "create-table", Schema: "public", Table: "parent_d_idx", Columns: []SchemaColumnInput{{Name: "id", Type: "integer"}}}, "an index"},
		{"index name used on another table", SchemaChange{Op: "add-index", Schema: "public", Table: "child", Index: "parent_d_idx", Column: "pc"}, "an index on public.parent"},
		{"index name used by a constraint's index", SchemaChange{Op: "add-index", Schema: "public", Table: "child", Index: "parent_pkey", Column: "pc"}, "parent_pkey"},
		{"drop a constraint-backing index", SchemaChange{Op: "drop-index", Schema: "public", Table: "parent", Index: "parent_b_c_key"}, "backs a constraint"},
		{"sequence default cannot be typed", SchemaChange{Op: "add-column", Schema: "public", Table: "child", Column: "n", Type: "bigint", Default: strPtr("nextval('s'::regclass)")}, "sequence"},
		{"serial is not representable", SchemaChange{Op: "add-column", Schema: "public", Table: "child", Column: "n", Type: "serial"}, "not representable"},
		{"duplicate column", SchemaChange{Op: "create-table", Schema: "public", Table: "t", Columns: []SchemaColumnInput{{Name: "x", Type: "text"}, {Name: "x", Type: "text"}}}, "appears twice"},
		{"unknown op", SchemaChange{Op: "truncate", Schema: "public", Table: "child"}, "unknown op"},
	}
	for _, tc := range cases {
		desired := copyModel(fixtureModel())
		_, err := applySchemaChanges(&desired, []SchemaChange{tc.change}, map[string]string{}, noDeps)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want mention of %q", tc.name, err, tc.want)
		}
	}

	desired := copyModel(fixtureModel())
	if _, err := applySchemaChanges(&desired, []SchemaChange{{Op: "create-table", Schema: "public", Table: "tags", Columns: []SchemaColumnInput{
		{Name: "id", Type: "bigint", IsPrimaryKey: true},
		{Name: "label", Type: "varchar(60)", Default: strPtr("'x'")},
	}}}, map[string]string{}, noDeps); err != nil {
		t.Fatalf("create-table: %v", err)
	}
	tags := desired.Table(db.V2Identity{Schema: "public", Name: "tags"})
	if !tags.Column("id").NotNull || tags.PrimaryKey() == nil || tags.Column("label").Type.Params["length"] != 60 {
		t.Fatalf("tags = %+v", tags)
	}
	if _, err := documentFromModel(desired); err != nil {
		t.Fatalf("desired document invalid: %v", err)
	}
}

func TestPlanIdentityAndCLIEquivalent(t *testing.T) {
	p := &StudioPlan{RenameFlags: []string{"public.t.it's>public.t.x"}, Up: []string{"a"}, Down: []string{"b"}, BaseSHA256: "1", TargetSHA256: "2"}
	id := planFingerprint(p)
	p2 := *p
	p2.BaseSHA256, p2.TargetSHA256 = "3", "4"
	if planFingerprint(&p2) != id {
		t.Fatal("plan id depends on document hashes; unrelated concurrent changes would invalidate reviews")
	}
	p2.Up = []string{"a2"}
	if planFingerprint(&p2) == id {
		t.Fatal("plan id ignores the statements")
	}
	p.explicitDrops = true
	got := cliEquivalent(p)
	want := `neutron db push --dry-run --schema target.schema.json --rename 'public.t.it'"'"'s>public.t.x' --allow-destructive`
	if got != want {
		t.Fatalf("cliEquivalent = %s\nwant           %s", got, want)
	}
}
