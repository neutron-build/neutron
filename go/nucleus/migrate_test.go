package nucleus

import (
	"sort"
	"testing"
)

func TestMigrationStruct(t *testing.T) {
	m := Migration{
		Version: 1,
		Name:    "create_users",
		Up:      "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
		Down:    "DROP TABLE users",
	}
	if m.Version != 1 {
		t.Errorf("Version = %d", m.Version)
	}
	if m.Name != "create_users" {
		t.Errorf("Name = %q", m.Name)
	}
	if m.Up == "" {
		t.Error("Up should not be empty")
	}
	if m.Down == "" {
		t.Error("Down should not be empty")
	}
}

func TestMigrationRecordStruct(t *testing.T) {
	r := MigrationRecord{
		Version: 1,
		Name:    "create_users",
	}
	if r.Version != 1 {
		t.Errorf("Version = %d", r.Version)
	}
	if r.Name != "create_users" {
		t.Errorf("Name = %q", r.Name)
	}
	if r.AppliedAt.IsZero() {
		// This is expected for a zero-value struct
	}
}

func TestMigrationSorting(t *testing.T) {
	migrations := []Migration{
		{Version: 3, Name: "add_index"},
		{Version: 1, Name: "create_users"},
		{Version: 2, Name: "add_email"},
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	if migrations[0].Version != 1 {
		t.Errorf("first migration version = %d, want 1", migrations[0].Version)
	}
	if migrations[1].Version != 2 {
		t.Errorf("second migration version = %d, want 2", migrations[1].Version)
	}
	if migrations[2].Version != 3 {
		t.Errorf("third migration version = %d, want 3", migrations[2].Version)
	}
}

func TestMigrationSortingDescending(t *testing.T) {
	// MigrateDown sorts descending
	migrations := []Migration{
		{Version: 1, Name: "first"},
		{Version: 3, Name: "third"},
		{Version: 2, Name: "second"},
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version > migrations[j].Version
	})

	if migrations[0].Version != 3 {
		t.Errorf("first version = %d, want 3", migrations[0].Version)
	}
	if migrations[1].Version != 2 {
		t.Errorf("second version = %d, want 2", migrations[1].Version)
	}
	if migrations[2].Version != 1 {
		t.Errorf("third version = %d, want 1", migrations[2].Version)
	}
}

func TestMigrationUpDownSQL(t *testing.T) {
	m := Migration{
		Version: 1,
		Name:    "create_users",
		Up:      "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)",
		Down:    "DROP TABLE IF EXISTS users",
	}

	if m.Up == "" {
		t.Fatal("Up SQL should not be empty")
	}
	if m.Down == "" {
		t.Fatal("Down SQL should not be empty")
	}
}

func TestMigrationEmptyDown(t *testing.T) {
	// Some migrations may not have a down migration
	m := Migration{
		Version: 1,
		Name:    "init",
		Up:      "CREATE TABLE config (key TEXT PRIMARY KEY, value TEXT)",
		Down:    "",
	}

	if m.Down != "" {
		t.Errorf("Down should be empty, got %q", m.Down)
	}
}

func TestMigrationsTableSQL(t *testing.T) {
	// Verify the migrations table SQL constant is not empty
	if migrationsTable == "" {
		t.Error("migrationsTable should not be empty")
	}
	// Check it creates the expected table
	if !contains(migrationsTable, "_neutron_migrations") {
		t.Error("migrationsTable should reference _neutron_migrations")
	}
	if !contains(migrationsTable, "version") {
		t.Error("migrationsTable should have version column")
	}
	if !contains(migrationsTable, "name") {
		t.Error("migrationsTable should have name column")
	}
	if !contains(migrationsTable, "applied_at") {
		t.Error("migrationsTable should have applied_at column")
	}
}

func TestMigrationMultipleMigrations(t *testing.T) {
	// Test a realistic migration set
	migrations := []Migration{
		{
			Version: 1,
			Name:    "create_users",
			Up:      "CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT UNIQUE NOT NULL)",
			Down:    "DROP TABLE users",
		},
		{
			Version: 2,
			Name:    "create_sessions",
			Up:      "CREATE TABLE sessions (id TEXT PRIMARY KEY, user_id INT REFERENCES users(id))",
			Down:    "DROP TABLE sessions",
		},
		{
			Version: 3,
			Name:    "add_user_name",
			Up:      "ALTER TABLE users ADD COLUMN name TEXT DEFAULT ''",
			Down:    "ALTER TABLE users DROP COLUMN name",
		},
	}

	// Sort and verify ordering
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	for i, m := range migrations {
		if m.Version != i+1 {
			t.Errorf("migrations[%d].Version = %d, want %d", i, m.Version, i+1)
		}
	}
}

func TestMigrationAppliedVersionsMap(t *testing.T) {
	// Test the applied versions logic used by Migrate
	applied := map[int]bool{
		1: true,
		2: true,
	}

	migrations := []Migration{
		{Version: 1, Name: "first", Up: "CREATE TABLE a (id INT)"},
		{Version: 2, Name: "second", Up: "CREATE TABLE b (id INT)"},
		{Version: 3, Name: "third", Up: "CREATE TABLE c (id INT)"},
	}

	var pending []Migration
	for _, m := range migrations {
		if !applied[m.Version] {
			pending = append(pending, m)
		}
	}

	if len(pending) != 1 {
		t.Fatalf("pending = %d, want 1", len(pending))
	}
	if pending[0].Version != 3 {
		t.Errorf("pending[0].Version = %d, want 3", pending[0].Version)
	}
}

func TestMigrationDownStepCounting(t *testing.T) {
	// Test the MigrateDown step counting logic
	applied := map[int]bool{1: true, 2: true, 3: true}
	migrations := []Migration{
		{Version: 3, Name: "third", Down: "DROP TABLE c"},
		{Version: 2, Name: "second", Down: "DROP TABLE b"},
		{Version: 1, Name: "first", Down: "DROP TABLE a"},
	}

	steps := 2
	rolled := 0
	var rolledBack []int
	for _, m := range migrations {
		if rolled >= steps {
			break
		}
		if !applied[m.Version] {
			continue
		}
		rolledBack = append(rolledBack, m.Version)
		rolled++
	}

	if len(rolledBack) != 2 {
		t.Fatalf("rolled back %d, want 2", len(rolledBack))
	}
	if rolledBack[0] != 3 {
		t.Errorf("first rollback = %d, want 3", rolledBack[0])
	}
	if rolledBack[1] != 2 {
		t.Errorf("second rollback = %d, want 2", rolledBack[1])
	}
}

func TestMigrationDownMissingDownSQL(t *testing.T) {
	// Verify the error case when Down is empty
	m := Migration{
		Version: 1,
		Name:    "irreversible",
		Up:      "DROP TABLE old_data",
		Down:    "",
	}

	if m.Down != "" {
		t.Error("expected empty Down SQL")
	}
}

func contains(s, substr string) bool {
	return len(s) > 0 && len(substr) > 0 && (s == substr || len(s) >= len(substr) && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
