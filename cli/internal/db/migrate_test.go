package db

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadMigrationFiles(t *testing.T) {
	dir := t.TempDir()

	// Create migration files in expected format
	files := map[string]string{
		"001_create_users.up.sql":   "CREATE TABLE users (id SERIAL);",
		"001_create_users.down.sql": "DROP TABLE users;",
		"002_add_email.up.sql":      "ALTER TABLE users ADD COLUMN email TEXT;",
		"002_add_email.down.sql":    "ALTER TABLE users DROP COLUMN email;",
		"README.md":                 "# Not a migration",
	}
	for name, content := range files {
		os.WriteFile(filepath.Join(dir, name), []byte(content), 0644)
	}

	result, err := ReadMigrationFiles(dir)
	if err != nil {
		t.Fatalf("ReadMigrationFiles() error: %v", err)
	}

	if len(result) != 2 {
		t.Fatalf("got %d migration files, want 2", len(result))
	}

	// Should be sorted by version
	if result[0].Version != "001" {
		t.Errorf("first migration version = %q, want %q", result[0].Version, "001")
	}
	if result[1].Version != "002" {
		t.Errorf("second migration version = %q, want %q", result[1].Version, "002")
	}

	// Check names
	if result[0].Name != "create_users" {
		t.Errorf("first migration name = %q, want %q", result[0].Name, "create_users")
	}
	if result[1].Name != "add_email" {
		t.Errorf("second migration name = %q, want %q", result[1].Name, "add_email")
	}

	// Check SQL content
	if result[0].SQL != "CREATE TABLE users (id SERIAL);" {
		t.Errorf("first migration SQL = %q", result[0].SQL)
	}
}

func TestReadMigrationFilesEmpty(t *testing.T) {
	dir := t.TempDir()

	result, err := ReadMigrationFiles(dir)
	if err != nil {
		t.Fatalf("ReadMigrationFiles() error: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("got %d files, want 0", len(result))
	}
}

func TestReadMigrationFilesNonexistentDir(t *testing.T) {
	_, err := ReadMigrationFiles("/nonexistent/path/that/does/not/exist")
	if err == nil {
		t.Fatal("expected error for nonexistent directory")
	}
}

func TestReadMigrationFilesSorted(t *testing.T) {
	dir := t.TempDir()

	// Create files out of order
	os.WriteFile(filepath.Join(dir, "003_third.up.sql"), []byte("third"), 0644)
	os.WriteFile(filepath.Join(dir, "001_first.up.sql"), []byte("first"), 0644)
	os.WriteFile(filepath.Join(dir, "002_second.up.sql"), []byte("second"), 0644)

	result, err := ReadMigrationFiles(dir)
	if err != nil {
		t.Fatalf("ReadMigrationFiles() error: %v", err)
	}

	if len(result) != 3 {
		t.Fatalf("got %d files, want 3", len(result))
	}

	for i, expected := range []string{"001", "002", "003"} {
		if result[i].Version != expected {
			t.Errorf("result[%d].Version = %q, want %q", i, result[i].Version, expected)
		}
	}
}

func TestReadMigrationFilesSkipsDownFiles(t *testing.T) {
	dir := t.TempDir()

	os.WriteFile(filepath.Join(dir, "001_init.up.sql"), []byte("up"), 0644)
	os.WriteFile(filepath.Join(dir, "001_init.down.sql"), []byte("down"), 0644)

	result, err := ReadMigrationFiles(dir)
	if err != nil {
		t.Fatalf("ReadMigrationFiles() error: %v", err)
	}

	// Only .up.sql files should be returned
	if len(result) != 1 {
		t.Errorf("got %d files, want 1 (only .up.sql)", len(result))
	}
}

func TestReadMigrationFilesSkipsDirectories(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "001_subdir.up.sql"), 0755) // directory named like migration
	os.WriteFile(filepath.Join(dir, "002_real.up.sql"), []byte("real"), 0644)

	result, err := ReadMigrationFiles(dir)
	if err != nil {
		t.Fatalf("ReadMigrationFiles() error: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("got %d files, want 1", len(result))
	}
}

func TestReadMigrationFilesSkipsBadFormat(t *testing.T) {
	dir := t.TempDir()
	// File without underscore separator
	os.WriteFile(filepath.Join(dir, "nounderscore.up.sql"), []byte("bad"), 0644)
	os.WriteFile(filepath.Join(dir, "001_good.up.sql"), []byte("good"), 0644)

	result, err := ReadMigrationFiles(dir)
	if err != nil {
		t.Fatalf("ReadMigrationFiles() error: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("got %d files, want 1", len(result))
	}
}

func TestCreateMigrationFiles(t *testing.T) {
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")

	upPath, downPath, err := CreateMigrationFiles(migrationsDir, "create_users")
	if err != nil {
		t.Fatalf("CreateMigrationFiles() error: %v", err)
	}

	// Verify paths
	if !strings.HasSuffix(upPath, ".up.sql") {
		t.Errorf("upPath %q should end with .up.sql", upPath)
	}
	if !strings.HasSuffix(downPath, ".down.sql") {
		t.Errorf("downPath %q should end with .down.sql", downPath)
	}

	// Verify files exist
	if _, err := os.Stat(upPath); err != nil {
		t.Errorf("up file not created: %v", err)
	}
	if _, err := os.Stat(downPath); err != nil {
		t.Errorf("down file not created: %v", err)
	}

	// Verify content
	upContent, _ := os.ReadFile(upPath)
	if !strings.Contains(string(upContent), "create_users") {
		t.Errorf("up file should mention migration name, got: %s", upContent)
	}

	downContent, _ := os.ReadFile(downPath)
	if !strings.Contains(string(downContent), "create_users") {
		t.Errorf("down file should mention migration name, got: %s", downContent)
	}
}

func TestCreateMigrationFilesIncrementsVersion(t *testing.T) {
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")

	// Create first migration
	up1, _, err := CreateMigrationFiles(migrationsDir, "first")
	if err != nil {
		t.Fatalf("CreateMigrationFiles(first) error: %v", err)
	}

	// Create second migration
	up2, _, err := CreateMigrationFiles(migrationsDir, "second")
	if err != nil {
		t.Fatalf("CreateMigrationFiles(second) error: %v", err)
	}

	// First should be 001, second should be 002
	if !strings.Contains(up1, "001_") {
		t.Errorf("first migration should be version 001, got: %s", up1)
	}
	if !strings.Contains(up2, "002_") {
		t.Errorf("second migration should be version 002, got: %s", up2)
	}
}

func TestCreateMigrationFilesNormalizesName(t *testing.T) {
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")

	upPath, _, err := CreateMigrationFiles(migrationsDir, "Add User Email")
	if err != nil {
		t.Fatalf("CreateMigrationFiles() error: %v", err)
	}

	// Name should be lowercased and spaces replaced with underscores
	if !strings.Contains(upPath, "add_user_email") {
		t.Errorf("migration name not normalized, got: %s", upPath)
	}
}

func TestCreateMigrationFilesCreatesDir(t *testing.T) {
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "deep", "nested", "migrations")

	_, _, err := CreateMigrationFiles(migrationsDir, "init")
	if err != nil {
		t.Fatalf("CreateMigrationFiles() error: %v", err)
	}

	info, err := os.Stat(migrationsDir)
	if err != nil {
		t.Fatalf("migrations directory not created: %v", err)
	}
	if !info.IsDir() {
		t.Error("migrations path is not a directory")
	}
}

// After a gap in the sequence (001, 003), the next version must be 004, not
// 003 again — the old len(files)+1 allocation re-used the gap and silently
// overwrote the existing pair.
func TestCreateMigrationFilesSkipsGaps(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "001_first.up.sql"), []byte("up1"), 0644)
	os.WriteFile(filepath.Join(dir, "001_first.down.sql"), []byte("down1"), 0644)
	os.WriteFile(filepath.Join(dir, "003_third.up.sql"), []byte("up3"), 0644)
	os.WriteFile(filepath.Join(dir, "003_third.down.sql"), []byte("down3"), 0644)

	upPath, downPath, err := CreateMigrationFiles(dir, "fourth")
	if err != nil {
		t.Fatalf("CreateMigrationFiles() error: %v", err)
	}

	if !strings.Contains(upPath, "004_fourth.up.sql") {
		t.Errorf("upPath = %q, want version 004 (max existing + 1)", upPath)
	}
	if !strings.Contains(downPath, "004_fourth.down.sql") {
		t.Errorf("downPath = %q, want version 004 (max existing + 1)", downPath)
	}

	// The existing 003 pair must be untouched
	third, err := os.ReadFile(filepath.Join(dir, "003_third.up.sql"))
	if err != nil || string(third) != "up3" {
		t.Errorf("003 up file was modified: content = %q, err = %v", third, err)
	}
}

func TestCreateMigrationFilesIgnoresNonNumericVersions(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "abc_manual.up.sql"), []byte("up"), 0644)

	upPath, _, err := CreateMigrationFiles(dir, "init")
	if err != nil {
		t.Fatalf("CreateMigrationFiles() error: %v", err)
	}
	if !strings.Contains(upPath, "001_init") {
		t.Errorf("upPath = %q, want version 001 when no numeric versions exist", upPath)
	}
}

// A collision must fail loudly instead of overwriting. The down file is not
// scanned for version allocation, so an orphaned down file with no up file
// recreates the collision the O_EXCL flag exists to catch.
func TestCreateMigrationFilesFailsOnCollision(t *testing.T) {
	dir := t.TempDir()
	orphan := filepath.Join(dir, "001_orphan.down.sql")
	os.WriteFile(orphan, []byte("precious down"), 0644)

	_, _, err := CreateMigrationFiles(dir, "orphan")
	if err == nil {
		t.Fatal("expected error when target down file already exists")
	}

	orphanContent, readErr := os.ReadFile(orphan)
	if readErr != nil || string(orphanContent) != "precious down" {
		t.Errorf("existing down file was modified: content = %q, err = %v", orphanContent, readErr)
	}

	// The up half of the pair must not be left behind
	if _, statErr := os.Stat(filepath.Join(dir, "001_orphan.up.sql")); statErr == nil {
		t.Error("orphaned up file left behind after failed create")
	}
}

func TestMigrationFileStruct(t *testing.T) {
	mf := MigrationFile{
		Version: "001",
		Name:    "create_users",
		Path:    "migrations/001_create_users.up.sql",
		SQL:     "CREATE TABLE users (id INT);",
		IsDown:  false,
	}
	if mf.Version != "001" {
		t.Errorf("Version = %q", mf.Version)
	}
	if mf.IsDown {
		t.Error("IsDown should be false")
	}
}

func TestMigrationRecordStruct(t *testing.T) {
	r := MigrationRecord{
		Version: "001",
		Name:    "init",
	}
	if r.Version != "001" {
		t.Errorf("Version = %q", r.Version)
	}
}

func TestMigrationStatusStruct(t *testing.T) {
	s := MigrationStatus{
		Version: "001",
		Name:    "init",
		Applied: false,
	}
	if s.Applied {
		t.Error("Applied should be false")
	}
}

func TestReadDownMigrationFiles(t *testing.T) {
	dir := t.TempDir()

	// Create down migration files
	files := map[string]string{
		"001_create_users.down.sql": "DROP TABLE users;",
		"002_add_email.down.sql":    "ALTER TABLE users DROP COLUMN email;",
		"003_add_role.down.sql":     "ALTER TABLE users DROP COLUMN role;",
	}
	for name, content := range files {
		os.WriteFile(filepath.Join(dir, name), []byte(content), 0644)
	}

	result, err := ReadDownMigrationFiles(dir)
	if err != nil {
		t.Fatalf("ReadDownMigrationFiles() error: %v", err)
	}

	if len(result) != 3 {
		t.Fatalf("got %d migration files, want 3", len(result))
	}

	// Should be sorted newest-first (reverse)
	if result[0].Version != "003" {
		t.Errorf("first migration version = %q, want %q", result[0].Version, "003")
	}
	if result[1].Version != "002" {
		t.Errorf("second migration version = %q, want %q", result[1].Version, "002")
	}
	if result[2].Version != "001" {
		t.Errorf("third migration version = %q, want %q", result[2].Version, "001")
	}

	// Check IsDown flag
	for i, mf := range result {
		if !mf.IsDown {
			t.Errorf("result[%d].IsDown should be true", i)
		}
	}
}

func TestReadDownMigrationFilesReverseSorted(t *testing.T) {
	dir := t.TempDir()

	// Create files out of order
	os.WriteFile(filepath.Join(dir, "001_first.down.sql"), []byte("down1"), 0644)
	os.WriteFile(filepath.Join(dir, "003_third.down.sql"), []byte("down3"), 0644)
	os.WriteFile(filepath.Join(dir, "002_second.down.sql"), []byte("down2"), 0644)

	result, err := ReadDownMigrationFiles(dir)
	if err != nil {
		t.Fatalf("ReadDownMigrationFiles() error: %v", err)
	}

	if len(result) != 3 {
		t.Fatalf("got %d files, want 3", len(result))
	}

	// Should be sorted newest-first
	for i, expected := range []string{"003", "002", "001"} {
		if result[i].Version != expected {
			t.Errorf("result[%d].Version = %q, want %q", i, result[i].Version, expected)
		}
	}
}

// Ordering must survive versions beyond the three-digit padding width:
// lexicographic order put "1000" before "999" (audit neutron-06).
func TestReadMigrationFilesNumericOrderAcrossPaddingWidth(t *testing.T) {
	dir := t.TempDir()

	for _, v := range []string{"998", "999", "1000", "1001"} {
		os.WriteFile(filepath.Join(dir, v+"_step.up.sql"), []byte("up "+v), 0644)
		os.WriteFile(filepath.Join(dir, v+"_step.down.sql"), []byte("down "+v), 0644)
	}

	result, err := ReadMigrationFiles(dir)
	if err != nil {
		t.Fatalf("ReadMigrationFiles() error: %v", err)
	}
	if len(result) != 4 {
		t.Fatalf("got %d files, want 4", len(result))
	}
	for i, expected := range []string{"998", "999", "1000", "1001"} {
		if result[i].Version != expected {
			t.Errorf("result[%d].Version = %q, want %q", i, result[i].Version, expected)
		}
	}

	down, err := ReadDownMigrationFiles(dir)
	if err != nil {
		t.Fatalf("ReadDownMigrationFiles() error: %v", err)
	}
	for i, expected := range []string{"1001", "1000", "999", "998"} {
		if down[i].Version != expected {
			t.Errorf("down[%d].Version = %q, want %q (reverse order broken past padding width)", i, down[i].Version, expected)
		}
	}
}

// Mixed-width numeric spellings order by value; duplicates stay adjacent
// and deterministic (audit neutron-06).
func TestReadMigrationFilesMixedWidthAndDuplicateVersions(t *testing.T) {
	dir := t.TempDir()

	for _, name := range []string{
		"7_short.up.sql",
		"003_padded.up.sql",
		"007_duplicate.up.sql",
		"1_tiny.up.sql",
	} {
		os.WriteFile(filepath.Join(dir, name), []byte("x"), 0644)
	}

	result, err := ReadMigrationFiles(dir)
	if err != nil {
		t.Fatalf("ReadMigrationFiles() error: %v", err)
	}

	got := make([]string, len(result))
	for i, f := range result {
		got[i] = f.Version
	}
	want := []string{"1", "003", "007", "7"}
	if len(got) != len(want) {
		t.Fatalf("versions = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("versions[%d] = %q, want %q (all: %v)", i, got[i], want[i], got)
		}
	}
}

// A path-like migration name must not write outside the migrations
// directory (audit neutron-05).
func TestCreateMigrationFilesRejectsPathLikeNames(t *testing.T) {
	outer := t.TempDir()
	dir := filepath.Join(outer, "migrations")

	for _, name := range []string{
		"part/../../escape",
		`part\..\escape`,
		"../traversal",
		"..",
		".",
		"",
		"naïve",
		"semi;colon",
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := CreateMigrationFiles(dir, name)
			if err == nil {
				t.Fatalf("CreateMigrationFiles(%q) accepted a path-like or illegal name", name)
			}
		})
	}

	// Nothing may have been created anywhere under the outer directory.
	entries, err := os.ReadDir(outer)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("rejected names left files behind under %s: %v", outer, entries)
	}
}

func TestCreateMigrationFilesAcceptsDescriptiveNames(t *testing.T) {
	dir := t.TempDir()

	for _, name := range []string{"create_users", "Add User Email", "add-index-constraint"} {
		t.Run(name, func(t *testing.T) {
			upPath, _, err := CreateMigrationFiles(dir, name)
			if err != nil {
				t.Fatalf("CreateMigrationFiles(%q) error: %v", name, err)
			}
			if filepath.Dir(upPath) != dir {
				t.Errorf("file created outside the migrations directory: %s", upPath)
			}
		})
	}

	for _, name := range []string{"semi;colon", "shell $(x)"} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := CreateMigrationFiles(dir, name); err == nil {
				t.Errorf("CreateMigrationFiles(%q) accepted an illegal character", name)
			}
		})
	}
}

// Applied versions with no local file must surface in the union with
// Missing set, not vanish from the status output (audit neutron-07).
func TestMergeMigrationStatusesIncludesAppliedButMissing(t *testing.T) {
	files := []MigrationFile{
		{Version: "001", Name: "init"},
		{Version: "003", Name: "later"},
	}
	applied := []MigrationRecord{
		{Version: "001", Name: "init"},
		{Version: "002", Name: "vanished"},
	}

	statuses := mergeMigrationStatuses(files, applied)
	if len(statuses) != 3 {
		t.Fatalf("got %d statuses, want 3 (union of files and records)", len(statuses))
	}

	byVersion := map[string]MigrationStatus{}
	for _, s := range statuses {
		byVersion[s.Version] = s
	}

	if s := byVersion["001"]; !s.Applied || s.Missing {
		t.Errorf("001 = %+v, want applied with file present", s)
	}
	s2, ok := byVersion["002"]
	if !ok {
		t.Fatal("applied-but-missing 002 was omitted from the status union")
	}
	if !s2.Applied || !s2.Missing || s2.Name != "vanished" {
		t.Errorf("002 = %+v, want applied, Missing, name from the database record", s2)
	}
	if s := byVersion["003"]; s.Applied || s.Missing {
		t.Errorf("003 = %+v, want pending", s)
	}
}

func TestMergeMigrationStatusesOrderedNumerically(t *testing.T) {
	files := []MigrationFile{
		{Version: "1000", Name: "wide"},
		{Version: "999", Name: "narrow"},
	}
	statuses := mergeMigrationStatuses(files, nil)
	if len(statuses) != 2 || statuses[0].Version != "999" || statuses[1].Version != "1000" {
		t.Errorf("statuses = %+v, want 999 before 1000", statuses)
	}
}
