package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Schema loading through the real command path: version 2 documents are
// contract-validated and load for planning (M02); the version 1 path is
// unchanged. loadSchemaJSON remains the version-1-only helper and refuses
// v2 documents clearly.
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

func writeTempSchema(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "neutron.schema.json")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadSchemaDocumentVersion2LoadsForPlanning(t *testing.T) {
	loaded, err := loadSchemaDocument(writeTempSchema(t, v2Doc))
	if err != nil {
		t.Fatalf("v2 must load: %v", err)
	}
	if loaded.V1 != nil || loaded.V2 == nil {
		t.Fatalf("expected v2 document, got %+v", loaded)
	}
	if len(loaded.V2.SHA256Hex) != 64 {
		t.Fatalf("expected canonical SHA-256, got %q", loaded.V2.SHA256Hex)
	}

	// The version-1-only helper refuses the same file with a clear message.
	_, err = loadSchemaJSON(writeTempSchema(t, v2Doc))
	if err == nil || !strings.Contains(err.Error(), "schema document v2") || !strings.Contains(err.Error(), "version 1") {
		t.Fatalf("expected v1-only refusal naming the document version, got %v", err)
	}
}

func TestLoadSchemaDocumentVersion2ContractErrorsSurface(t *testing.T) {
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
	_, err := loadSchemaDocument(writeTempSchema(t, dup))
	if err == nil || !strings.Contains(err.Error(), "[duplicate-table]") {
		t.Fatalf("expected contract duplicate-table rejection, got %v", err)
	}
}

func TestLoadSchemaJSONVersion1PathUnchanged(t *testing.T) {
	v1 := `{"version":1,"tables":[{"name":"users","columns":[{"name":"id","type":"serial","notNull":true,"primaryKey":true}],"indexes":[]}]}`
	schema, err := loadSchemaJSON(writeTempSchema(t, v1))
	if err != nil {
		t.Fatalf("v1 must keep loading: %v", err)
	}
	if schema.Version != 1 || len(schema.Tables) != 1 {
		t.Fatalf("unexpected schema: %+v", schema)
	}

	// The version 1 error messages are unchanged.
	if _, err := loadSchemaJSON(writeTempSchema(t, `{}`)); err == nil || !strings.Contains(err.Error(), "version 0") {
		t.Fatalf("expected v1 version-0 rejection, got %v", err)
	}

	if _, err := loadSchemaJSON(writeTempSchema(t, `{"version":3,"tables":[]}`)); err == nil || !strings.Contains(err.Error(), "version 3") {
		t.Fatalf("expected v1 unknown-version rejection, got %v", err)
	}
}
