package db

import (
	"encoding/json"
	"strings"
	"testing"
)

func validUsersSchema() Schema {
	return Schema{
		Version: 1,
		Tables: []TableDef{
			{
				Name: "users",
				Columns: []ColumnDef{
					{Name: "id", Type: "serial", NotNull: true, PrimaryKey: true},
					{Name: "email", Type: "varchar", VarcharLength: 255, NotNull: true, Unique: true},
					{Name: "name", Type: "text"},
				},
				Indexes: []IndexDef{{Name: "users_name_idx", Columns: []string{"name"}}},
			},
		},
	}
}

func TestValidateSchemaEmptyObjectRejected(t *testing.T) {
	// {} unmarshals to Version 0 — malformed, not an empty managed schema.
	err := ValidateSchema(&Schema{})
	if err == nil || !strings.Contains(err.Error(), "version 0") {
		t.Fatalf("expected version rejection for {}, got %v", err)
	}
}

func TestValidateSchemaUnknownVersionRejected(t *testing.T) {
	s := validUsersSchema()
	s.Version = 2
	err := ValidateSchema(&s)
	if err == nil || !strings.Contains(err.Error(), "version 2") {
		t.Fatalf("expected unknown-version rejection, got %v", err)
	}
}

func TestValidateSchemaExplicitEmptyIsValid(t *testing.T) {
	// An explicitly empty managed schema is valid and distinguishable from
	// malformed input; whether it implies drops is a destructive-intent
	// question, not a validation question.
	if err := ValidateSchema(&Schema{Version: 1}); err != nil {
		t.Fatalf("explicitly empty schema must validate: %v", err)
	}
}

func TestValidateSchemaDuplicateTableRejected(t *testing.T) {
	s := validUsersSchema()
	s.Tables = append(s.Tables, s.Tables[0])
	err := ValidateSchema(&s)
	if err == nil || !strings.Contains(err.Error(), `duplicate table name "users"`) {
		t.Fatalf("expected duplicate-table rejection, got %v", err)
	}
}

func TestValidateSchemaDuplicateColumnRejected(t *testing.T) {
	s := validUsersSchema()
	s.Tables[0].Columns = append(s.Tables[0].Columns, ColumnDef{Name: "email", Type: "text"})
	err := ValidateSchema(&s)
	if err == nil || !strings.Contains(err.Error(), `duplicate column name "email"`) {
		t.Fatalf("expected duplicate-column rejection, got %v", err)
	}
}

func TestValidateSchemaDuplicateIndexRejected(t *testing.T) {
	s := validUsersSchema()
	s.Tables[0].Indexes = append(s.Tables[0].Indexes, IndexDef{Name: "users_name_idx", Columns: []string{"email"}})
	err := ValidateSchema(&s)
	if err == nil || !strings.Contains(err.Error(), `duplicate index name "users_name_idx"`) {
		t.Fatalf("expected duplicate-index rejection, got %v", err)
	}

	// Index names are schema-global in Postgres: same name on another table
	// must also fail.
	s = validUsersSchema()
	s.Tables = append(s.Tables, TableDef{
		Name:    "orders",
		Columns: []ColumnDef{{Name: "id", Type: "serial", PrimaryKey: true}, {Name: "note", Type: "text"}},
		Indexes: []IndexDef{{Name: "users_name_idx", Columns: []string{"note"}}},
	})
	err = ValidateSchema(&s)
	if err == nil || !strings.Contains(err.Error(), `duplicate index name "users_name_idx"`) {
		t.Fatalf("expected cross-table duplicate-index rejection, got %v", err)
	}
}

func TestValidateSchemaUnsupportedTypeRejected(t *testing.T) {
	s := validUsersSchema()
	s.Tables[0].Columns = append(s.Tables[0].Columns, ColumnDef{Name: "tags", Type: "not_a_real_type"})
	err := ValidateSchema(&s)
	if err == nil || !strings.Contains(err.Error(), `unsupported type "not_a_real_type"`) {
		t.Fatalf("expected unsupported-type rejection, got %v", err)
	}
}

func TestValidateSchemaVectorRequiresNucleusOnly(t *testing.T) {
	s := validUsersSchema()
	s.Tables[0].Columns = append(s.Tables[0].Columns, ColumnDef{Name: "embedding", Type: "vector", VectorDims: 3})
	err := ValidateSchema(&s)
	if err == nil || !strings.Contains(err.Error(), "nucleusOnly") {
		t.Fatalf("expected non-nucleusOnly vector rejection, got %v", err)
	}

	s.Tables[0].Columns[3].NucleusOnly = true
	s.Tables[0].Columns[3].VectorDims = 0
	err = ValidateSchema(&s)
	if err == nil || !strings.Contains(err.Error(), "vectorDimensions") {
		t.Fatalf("expected missing-dims rejection, got %v", err)
	}
}

func TestValidateSchemaAmbiguousDefaultsRejected(t *testing.T) {
	v := "x"
	cases := []struct {
		name string
		col  ColumnDef
		want string
	}{
		{"default without hasDefault", ColumnDef{Name: "a", Type: "text", Default: &v}, "hasDefault is false"},
		{"defaultNow without hasDefault", ColumnDef{Name: "a", Type: "timestamptz", DefaultNow: true}, "hasDefault is false"},
		{"both defaultNow and literal", ColumnDef{Name: "a", Type: "timestamptz", HasDefault: true, DefaultNow: true, Default: &v}, "both defaultNow"},
		{"defaultNow on non-temporal", ColumnDef{Name: "a", Type: "integer", HasDefault: true, DefaultNow: true}, "timestamp/timestamptz"},
	}
	for _, tc := range cases {
		s := validUsersSchema()
		s.Tables[0].Columns = append(s.Tables[0].Columns, tc.col)
		err := ValidateSchema(&s)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: expected rejection mentioning %q, got %v", tc.name, tc.want, err)
		}
	}
}

func TestValidateSchemaExpressionDefaultRejected(t *testing.T) {
	s := validUsersSchema()
	inj := "0; drop table users"
	s.Tables[0].Columns = append(s.Tables[0].Columns, ColumnDef{Name: "n", Type: "integer", HasDefault: true, Default: &inj})
	err := ValidateSchema(&s)
	if err == nil || !strings.Contains(err.Error(), "not a plain numeric literal") {
		t.Fatalf("expected expression-default rejection, got %v", err)
	}
}

func TestValidateSchemaFKValidation(t *testing.T) {
	// FK to an undeclared table.
	s := validUsersSchema()
	s.Tables[0].Columns = append(s.Tables[0].Columns, ColumnDef{
		Name: "org_id", Type: "integer",
		ForeignKey: &ForeignKeyDef{Table: "orgs", Column: "id"},
	})
	err := ValidateSchema(&s)
	if err == nil || !strings.Contains(err.Error(), `referencing table "orgs", which is not declared`) {
		t.Fatalf("expected unknown FK table rejection, got %v", err)
	}

	// FK to an internal table.
	s.Tables[0].Columns[3].ForeignKey.Table = "_neutron_migrations"
	err = ValidateSchema(&s)
	if err == nil || !strings.Contains(err.Error(), "neutron-internal") {
		t.Fatalf("expected internal-table FK rejection, got %v", err)
	}

	// FK to a missing column of a declared table.
	s.Tables[0].Columns[3].ForeignKey.Table = "users"
	s.Tables[0].Columns[3].ForeignKey.Column = "nope"
	err = ValidateSchema(&s)
	if err == nil || !strings.Contains(err.Error(), "references column users.nope") {
		t.Fatalf("expected unknown FK column rejection, got %v", err)
	}

	// Unsupported referential action.
	s.Tables[0].Columns[3].ForeignKey.Column = "id"
	s.Tables[0].Columns[3].ForeignKey.OnDelete = "drop database x"
	err = ValidateSchema(&s)
	if err == nil || !strings.Contains(err.Error(), "onDelete") {
		t.Fatalf("expected onDelete whitelist rejection, got %v", err)
	}
}

func TestValidateSchemaInternalTableDeclarationRejected(t *testing.T) {
	s := validUsersSchema()
	s.Tables = append(s.Tables, TableDef{
		Name:    "_neutron_migrations",
		Columns: []ColumnDef{{Name: "version", Type: "text"}},
	})
	err := ValidateSchema(&s)
	if err == nil || !strings.Contains(err.Error(), "neutron-internal") {
		t.Fatalf("expected internal-table declaration rejection, got %v", err)
	}
}

func TestValidateSchemaIndexOnUnknownColumnRejected(t *testing.T) {
	s := validUsersSchema()
	s.Tables[0].Indexes = append(s.Tables[0].Indexes, IndexDef{Name: "bad_idx", Columns: []string{"ghost"}})
	err := ValidateSchema(&s)
	if err == nil || !strings.Contains(err.Error(), `unknown column "ghost"`) {
		t.Fatalf("expected index-column rejection, got %v", err)
	}
}

func TestValidateSchemaZeroColumnTableRejected(t *testing.T) {
	s := validUsersSchema()
	s.Tables = append(s.Tables, TableDef{Name: "empty"})
	err := ValidateSchema(&s)
	if err == nil || !strings.Contains(err.Error(), `table "empty" declares no columns`) {
		t.Fatalf("expected zero-column table rejection, got %v", err)
	}

	s.Tables[1].Columns = []ColumnDef{}
	if err := ValidateSchema(&s); err == nil {
		t.Fatal("explicit columns: [] must also be rejected")
	}
}

func TestValidateSchemaPrimaryKeyImpliesNotNull(t *testing.T) {
	// Hand-written JSON with primaryKey but no notNull: validation accepts it
	// and normalizes primaryKey => notNull, matching the TS exporter
	// (neutron-sql/src/export.ts) and README §3.2 "PK implies NOT NULL".
	s := validUsersSchema()
	s.Tables[0].Columns[0].NotNull = false // id keeps PrimaryKey: true
	if err := ValidateSchema(&s); err != nil {
		t.Fatalf("PK without notNull must validate after normalization: %v", err)
	}
	if !s.Tables[0].Columns[0].NotNull {
		t.Fatal("ValidateSchema must normalize primaryKey => notNull")
	}
}

func TestSchemaJSONTablesNullRejected(t *testing.T) {
	var s Schema
	err := json.Unmarshal([]byte(`{"version":1,"tables":null}`), &s)
	if err == nil || !strings.Contains(err.Error(), `"tables" must not be null`) {
		t.Fatalf("expected explicit null-tables rejection, got %v", err)
	}

	// A missing "tables" key and an explicit empty array stay valid.
	if err := json.Unmarshal([]byte(`{"version":1}`), &s); err != nil {
		t.Fatalf("missing tables must decode as explicitly empty, got %v", err)
	}
	if s.Version != 1 || len(s.Tables) != 0 {
		t.Fatalf("unexpected decoded schema: %+v", s)
	}
	if err := json.Unmarshal([]byte(`{"version":1,"tables":[]}`), &s); err != nil {
		t.Fatalf(`"tables": [] must decode, got %v`, err)
	}

	// Unknown-field rejection is preserved through the custom unmarshaler.
	err = json.Unmarshal([]byte(`{"version":1,"tables":[],"bogus":true}`), &s)
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("expected unknown-field rejection, got %v", err)
	}
}
