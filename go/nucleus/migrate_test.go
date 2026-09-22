package nucleus

import (
	"sort"
	"strings"
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

// GO-30: migration inputs are copied and validated before any SQL runs —
// the caller's slice is never mutated (it may be shared configuration), and
// duplicate/nonpositive/empty versions are rejected up front instead of
// mid-run after earlier migrations already executed.
func TestPrepareMigrationsDoesNotMutateInput(t *testing.T) {
	input := []Migration{
		{Version: 3, Name: "c", Up: "SELECT 3"},
		{Version: 1, Name: "a", Up: "SELECT 1"},
		{Version: 2, Name: "b", Up: "SELECT 2"},
	}
	plan, err := prepareMigrations(input, false)
	if err != nil {
		t.Fatalf("prepareMigrations: %v", err)
	}
	if len(plan) != 3 || plan[0].Version != 1 || plan[2].Version != 3 {
		t.Errorf("plan not sorted ascending: %+v", plan)
	}
	if input[0].Version != 3 || input[1].Version != 1 {
		t.Errorf("caller's slice was mutated in place: %+v", input)
	}

	desc, err := prepareMigrations(input, true)
	if err != nil {
		t.Fatalf("prepareMigrations descending: %v", err)
	}
	if desc[0].Version != 3 || desc[2].Version != 1 {
		t.Errorf("plan not sorted descending: %+v", desc)
	}
}

func TestPrepareMigrationsRejectsBadPlans(t *testing.T) {
	cases := []struct {
		name    string
		input   []Migration
		wantErr string
	}{
		{
			name:    "duplicate versions",
			input:   []Migration{{Version: 1, Name: "a", Up: "SELECT 1"}, {Version: 1, Name: "b", Up: "SELECT 1"}},
			wantErr: "duplicate migration version 1",
		},
		{
			name:    "nonpositive version",
			input:   []Migration{{Version: 0, Name: "a", Up: "SELECT 1"}},
			wantErr: "invalid migration version 0",
		},
		{
			name:    "empty name",
			input:   []Migration{{Version: 1, Name: "  ", Up: "SELECT 1"}},
			wantErr: "empty name",
		},
		{
			name:    "empty up sql",
			input:   []Migration{{Version: 1, Name: "a", Up: "   "}},
			wantErr: "empty Up SQL",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := prepareMigrations(tc.input, false)
			if err == nil {
				t.Fatalf("expected an error containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

// GO-30: applied migrations record a sha256 over version, name, and Up SQL.
// Deterministic, and sensitive to exactly the inputs that define what ran.
func TestMigrationChecksum(t *testing.T) {
	base := Migration{Version: 1, Name: "create_users", Up: "CREATE TABLE users (id INT)"}
	want := migrationChecksum(base)

	if got := migrationChecksum(base); got != want {
		t.Errorf("checksum not deterministic: %s vs %s", got, want)
	}
	if len(want) != 64 { // sha256 hex
		t.Errorf("checksum length = %d, want 64", len(want))
	}

	changed := base
	changed.Up = "CREATE TABLE users (id BIGINT)"
	if migrationChecksum(changed) == want {
		t.Error("checksum insensitive to Up SQL changes")
	}

	// The v2 checksum covers the up SQL only (contracts/data/MIGRATIONS.md
	// §3): identity lives in the version primary key, the down SQL may
	// evolve, and the name is metadata.
	changed = base
	changed.Name = "create_users_v2"
	if migrationChecksum(changed) != want {
		t.Error("checksum must not track name changes")
	}
	changed = base
	changed.Version = 2
	if migrationChecksum(changed) != want {
		t.Error("checksum must not track version changes")
	}
}

// Golden vectors pin the canonical algorithm across the CLI and both SDKs
// (contracts/data/MIGRATIONS.md §3).
func TestMigrationChecksumGoldenVectors(t *testing.T) {
	cases := []struct{ sql, want string }{
		{"CREATE TABLE x (id INT)\n", "c4b873a900b90da54e1de8efb0da3f7294599c0e96b5c50f2ff411bd7274a65a"},
		{"CREATE TABLE users (id serial PRIMARY KEY);\nALTER TABLE users ADD COLUMN email TEXT;\n",
			"5df840dd9f1517a75a84c53d78c6caf338ecff05e211ea8a08df48f21b983f9c"},
	}
	for _, c := range cases {
		m := Migration{Version: 9, Name: "vector", Up: c.sql}
		if got := migrationChecksum(m); got != c.want {
			t.Errorf("migrationChecksum(%q) = %s, want %s", c.sql, got, c.want)
		}
	}
}

// The legacy digest golden vector — the compatibility shim verified during
// adoption.
func TestLegacyMigrationChecksumVector(t *testing.T) {
	got := legacyMigrationChecksum(1, "first", "CREATE TABLE legacy_a (id INT)")
	want := "208474566c268521846034e32490fac1e893a2d6390018f75bb915b3722d995f"
	if got != want {
		t.Errorf("legacyMigrationChecksum = %s, want %s", got, want)
	}
	// NUL separation: (1, "ab", ...) and (1, "a", "b", ...) must differ.
	if legacyMigrationChecksum(1, "ab", "c") == legacyMigrationChecksum(1, "a", "bc") {
		t.Error("legacy digest not NUL-separated: field boundary can slide")
	}
}

// The history and lock tables carry the columns the code writes.
func TestMigrationsTableSQLChecksum(t *testing.T) {
	for _, col := range []string{"checksum", "owner", "format"} {
		if !contains(migrationsTable, col) {
			t.Errorf("migrationsTable should have a %s column", col)
		}
	}
	if !contains(migrationsAddColumns, "ADD COLUMN IF NOT EXISTS checksum") {
		t.Error("migrationsAddColumns should add the checksum column if missing")
	}
	if !contains(migrationLockTable, "_neutron_migration_lock") {
		t.Error("migrationLockTable should create _neutron_migration_lock")
	}
	if !contains(migrationLockTable, "token") || !contains(migrationLockTable, "locked_at") || !contains(migrationLockTable, "owner") {
		t.Error("migrationLockTable should carry token, locked_at and owner")
	}
}
