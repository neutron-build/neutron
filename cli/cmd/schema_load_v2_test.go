package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Schema loading through the real command path: version 2 documents are
// contract-validated and refused for planning with a clear message; the
// version 1 path is unchanged.
const v2Doc = `{
	"version": 2,
	"dialect": "postgresql",
	"capabilities": [],
	"schemas": [{"name": "public"}],
	"tables": [{
		"identity": {"schema": "public", "name": "users"},
		"managed": true,
		"columns": [{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true}],
		"constraints": [{"name": "users_pkey", "type": "primary-key", "columns": ["id"]}],
		"indexes": []
	}],
	"enums": [],
	"views": [],
	"opaque": []
}`

func TestLoadSchemaJSONVersion2ValidatedThenRefusedForPlanning(t *testing.T) {
	path := filepath.Join(t.TempDir(), "neutron.schema.json")
	if err := os.WriteFile(path, []byte(v2Doc), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := loadSchemaJSON(path)
	if err == nil {
		t.Fatal("expected v2 planning refusal")
	}
	for _, want := range []string{"valid schema document v2", "canonical SHA-256", "not implemented yet"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error must mention %q, got: %v", want, err)
		}
	}
}

func TestLoadSchemaJSONVersion2ContractErrorsSurface(t *testing.T) {
	dup := strings.Replace(v2Doc,
		`	}],
	"enums": [],`,
		`	}, {
		"identity": {"schema": "public", "name": "users"},
		"managed": true,
		"columns": [{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true}],
		"constraints": [],
		"indexes": []
	}],
	"enums": [],`, 1)
	if dup == v2Doc {
		t.Fatal("fixture rewrite did not apply")
	}
	path := filepath.Join(t.TempDir(), "neutron.schema.json")
	if err := os.WriteFile(path, []byte(dup), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := loadSchemaJSON(path)
	if err == nil || !strings.Contains(err.Error(), "[duplicate-table]") {
		t.Fatalf("expected contract duplicate-table rejection, got %v", err)
	}
}

func TestLoadSchemaJSONVersion1PathUnchanged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "neutron.schema.json")
	v1 := `{"version":1,"tables":[{"name":"users","columns":[{"name":"id","type":"serial","notNull":true,"primaryKey":true}],"indexes":[]}]}`
	if err := os.WriteFile(path, []byte(v1), 0o644); err != nil {
		t.Fatal(err)
	}
	schema, err := loadSchemaJSON(path)
	if err != nil {
		t.Fatalf("v1 must keep loading: %v", err)
	}
	if schema.Version != 1 || len(schema.Tables) != 1 {
		t.Fatalf("unexpected schema: %+v", schema)
	}

	// The version 1 error messages are unchanged.
	empty := filepath.Join(t.TempDir(), "empty.json")
	if err := os.WriteFile(empty, []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadSchemaJSON(empty); err == nil || !strings.Contains(err.Error(), "version 0") {
		t.Fatalf("expected v1 version-0 rejection, got %v", err)
	}

	unknown := filepath.Join(t.TempDir(), "v3.json")
	if err := os.WriteFile(unknown, []byte(`{"version":3,"tables":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadSchemaJSON(unknown); err == nil || !strings.Contains(err.Error(), "version 3") {
		t.Fatalf("expected v1 unknown-version rejection, got %v", err)
	}
}
