package neutroncli

import (
	"os"
	"strings"
	"testing"
)

func TestParseSQLBasic(t *testing.T) {
	sql := `CREATE TABLE users (
    id BIGSERIAL PRIMARY KEY NOT NULL,
    email VARCHAR(255) NOT NULL,
    name TEXT,
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);`

	f := writeTempFile(t, sql)
	defer f.Close()
	f.Seek(0, 0)

	tables, err := parseSQL(f)
	if err != nil {
		t.Fatalf("parseSQL: %v", err)
	}
	if len(tables) != 1 {
		t.Fatalf("tables = %d, want 1", len(tables))
	}

	tbl := tables[0]
	if tbl.Name != "users" {
		t.Errorf("name = %q", tbl.Name)
	}
	if len(tbl.Columns) != 5 {
		t.Fatalf("columns = %d, want 5", len(tbl.Columns))
	}

	// Check id column
	if tbl.Columns[0].Name != "id" {
		t.Errorf("col[0].Name = %q", tbl.Columns[0].Name)
	}
	if tbl.Columns[0].PK != true {
		t.Error("id should be PK")
	}
	if tbl.Columns[0].Nullable {
		t.Error("id should not be nullable")
	}

	// Check name column (nullable)
	if tbl.Columns[2].Name != "name" {
		t.Errorf("col[2].Name = %q", tbl.Columns[2].Name)
	}
	if !tbl.Columns[2].Nullable {
		t.Error("name should be nullable")
	}
}

func TestParseSQLMultipleTables(t *testing.T) {
	sql := `CREATE TABLE users (
    id SERIAL PRIMARY KEY NOT NULL,
    email TEXT NOT NULL
);

CREATE TABLE posts (
    id BIGSERIAL PRIMARY KEY NOT NULL,
    user_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    body TEXT
);`

	f := writeTempFile(t, sql)
	defer f.Close()
	f.Seek(0, 0)

	tables, err := parseSQL(f)
	if err != nil {
		t.Fatalf("parseSQL: %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("tables = %d, want 2", len(tables))
	}
	if tables[0].Name != "users" {
		t.Errorf("table[0] = %q", tables[0].Name)
	}
	if tables[1].Name != "posts" {
		t.Errorf("table[1] = %q", tables[1].Name)
	}
}

func TestGenerateStructs(t *testing.T) {
	tables := []sqlTable{
		{
			Name: "users",
			Columns: []sqlColumn{
				{Name: "id", Type: "BIGSERIAL", PK: true},
				{Name: "email", Type: "TEXT"},
				{Name: "name", Type: "TEXT", Nullable: true},
				{Name: "created_at", Type: "TIMESTAMPTZ"},
			},
		},
	}

	code := generateStructs(tables)
	if !strings.Contains(code, "type Users struct") {
		t.Error("missing struct declaration")
	}
	if !strings.Contains(code, "ID int64") {
		t.Error("missing ID field")
	}
	if !strings.Contains(code, "Email string") {
		t.Error("missing Email field")
	}
	if !strings.Contains(code, "Name *string") {
		t.Error("missing nullable Name field")
	}
	if !strings.Contains(code, "CreatedAt time.Time") {
		t.Error("missing CreatedAt field")
	}
	if !strings.Contains(code, `db:"email"`) {
		t.Error("missing db tag")
	}
	if !strings.Contains(code, `json:"email"`) {
		t.Error("missing json tag")
	}
}

func TestSQLToGoType(t *testing.T) {
	tests := []struct {
		sqlType  string
		nullable bool
		want     string
	}{
		{"BIGSERIAL", false, "int64"},
		{"INTEGER", false, "int32"},
		{"TEXT", false, "string"},
		{"VARCHAR(255)", false, "string"},
		{"BOOLEAN", false, "bool"},
		{"TIMESTAMPTZ", false, "time.Time"},
		{"JSONB", false, "json.RawMessage"},
		{"BYTEA", false, "[]byte"},
		{"TEXT", true, "*string"},
		{"INTEGER", true, "*int32"},
		{"VECTOR(1536)", false, "[]float32"},
	}

	for _, tt := range tests {
		got := sqlToGoType(tt.sqlType, tt.nullable)
		if got != tt.want {
			t.Errorf("sqlToGoType(%q, %v) = %q, want %q", tt.sqlType, tt.nullable, got, tt.want)
		}
	}
}

func TestToGoName(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"user_id", "UserID"},
		{"created_at", "CreatedAt"},
		{"name", "Name"},
		{"api_key", "APIKey"},
		{"http_url", "HTTPURL"},
	}

	for _, tt := range tests {
		got := toGoName(tt.input)
		if got != tt.want {
			t.Errorf("toGoName(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestParseSQLWithConstraints(t *testing.T) {
	sql := `CREATE TABLE orders (
    id BIGSERIAL PRIMARY KEY NOT NULL,
    user_id INTEGER NOT NULL,
    total NUMERIC(10,2) NOT NULL,
    CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id)
);`

	f := writeTempFile(t, sql)
	defer f.Close()
	f.Seek(0, 0)

	tables, err := parseSQL(f)
	if err != nil {
		t.Fatalf("parseSQL: %v", err)
	}
	if len(tables) != 1 {
		t.Fatalf("tables = %d", len(tables))
	}
	// Should have 3 columns (CONSTRAINT line should be skipped)
	if len(tables[0].Columns) != 3 {
		t.Errorf("columns = %d, want 3", len(tables[0].Columns))
	}
}

func TestParseSQLIfNotExists(t *testing.T) {
	sql := `CREATE TABLE IF NOT EXISTS items (
    id SERIAL PRIMARY KEY NOT NULL,
    name TEXT NOT NULL
);`

	f := writeTempFile(t, sql)
	defer f.Close()
	f.Seek(0, 0)

	tables, err := parseSQL(f)
	if err != nil {
		t.Fatalf("parseSQL: %v", err)
	}
	if len(tables) != 1 {
		t.Fatalf("tables = %d", len(tables))
	}
	if tables[0].Name != "items" {
		t.Errorf("name = %q", tables[0].Name)
	}
}

func writeTempFile(t *testing.T, content string) *os.File {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "*.sql")
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(content)
	return f
}
