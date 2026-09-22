package db

import (
	"strings"
	"testing"
	"time"
)

// Checksum golden vectors pin the canonical algorithm
// (contracts/data/MIGRATIONS.md §3) — identical vectors are pinned by the Go
// and TS Nucleus SDKs, so all three runners must keep agreeing byte-for-byte.
func TestMigrationChecksumGoldenVectors(t *testing.T) {
	cases := []struct{ sql, want string }{
		{"CREATE TABLE x (id INT)\n", "c4b873a900b90da54e1de8efb0da3f7294599c0e96b5c50f2ff411bd7274a65a"},
		{"CREATE TABLE users (id serial PRIMARY KEY);\nALTER TABLE users ADD COLUMN email TEXT;\n",
			"5df840dd9f1517a75a84c53d78c6caf338ecff05e211ea8a08df48f21b983f9c"},
	}
	for _, c := range cases {
		if got := MigrationChecksum(c.sql); got != c.want {
			t.Errorf("MigrationChecksum(%q) = %s, want %s", c.sql, got, c.want)
		}
	}
}

// The checksum is verbatim content: whitespace differences are different
// content (schema contract v2's verbatim-text rule — no normalization).
func TestMigrationChecksumVerbatim(t *testing.T) {
	a := MigrationChecksum("CREATE TABLE x (id INT)")
	b := MigrationChecksum("CREATE TABLE x (id INT)\n")
	c := MigrationChecksum("CREATE TABLE x  (id INT)")
	if a == b || a == c || b == c {
		t.Error("formatting differences must produce different checksums")
	}
	if len(a) != 64 || strings.ToLower(a) != a {
		t.Errorf("checksum must be lowercase 64-hex, got %q", a)
	}
}

// The legacy Go SDK digest golden vector — the compatibility shim verified
// during adoption.
func TestLegacyGoSDKChecksumVector(t *testing.T) {
	got := LegacyGoSDKChecksum(1, "first", "CREATE TABLE legacy_a (id INT)")
	want := "208474566c268521846034e32490fac1e893a2d6390018f75bb915b3722d995f"
	if got != want {
		t.Errorf("LegacyGoSDKChecksum = %s, want %s", got, want)
	}
}

func rec(version string, checksum *string) MigrationRecord {
	return MigrationRecord{Version: version, Name: "n", AppliedAt: time.Now(), Checksum: checksum}
}

func file(version string) MigrationFile {
	return MigrationFile{Version: version, Name: "n", SQL: "SELECT 1"}
}

func TestDetectMigrationCollisions(t *testing.T) {
	s := func(v string) *string { return &v }

	tests := []struct {
		name    string
		files   []MigrationFile
		applied []MigrationRecord
		wantErr string // empty means no error
	}{
		{
			name:  "distinct numeric ids pass",
			files: []MigrationFile{file("001"), file("002")},
		},
		{
			name:    "history and file same id pass",
			files:   []MigrationFile{file("001")},
			applied: []MigrationRecord{rec("001", s("x"))},
		},
		{
			name:  "non-numeric and numeric never collide",
			files: []MigrationFile{file("001"), file("init")},
		},
		{
			name:  "two non-numeric distinct ids pass",
			files: []MigrationFile{file("alpha"), file("beta")},
		},
		{
			name:    "history 1 vs file 001 collide",
			files:   []MigrationFile{file("001")},
			applied: []MigrationRecord{rec("1", nil)},
			wantErr: "numerically equal but textually distinct",
		},
		{
			name:    "file 1 vs file 001 collide",
			files:   []MigrationFile{file("1"), file("001")},
			wantErr: "numerically equal but textually distinct",
		},
		{
			name:    "same id twice across files is duplicate",
			files:   []MigrationFile{file("001"), file("001")},
			wantErr: "claimed by more than one file",
		},
		{
			name:    "padded spelling in history vs file",
			files:   []MigrationFile{file("0007")},
			applied: []MigrationRecord{rec("7", nil)},
			wantErr: "numerically equal but textually distinct",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := DetectMigrationCollisions(tc.files, tc.applied)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestVerifyAppliedChecksums(t *testing.T) {
	match := "SELECT 1"
	other := "SELECT 2"
	matchSum := MigrationChecksum(match)

	t.Run("matching checksum passes", func(t *testing.T) {
		unverified, err := VerifyAppliedChecksums(
			[]MigrationFile{{Version: "001", SQL: match}},
			[]MigrationRecord{rec("001", &matchSum)},
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(unverified) != 0 {
			t.Errorf("unverified = %v, want none", unverified)
		}
	})

	t.Run("nil checksum reports unverified without failing", func(t *testing.T) {
		unverified, err := VerifyAppliedChecksums(
			[]MigrationFile{{Version: "001", SQL: match}},
			[]MigrationRecord{rec("001", nil)},
		)
		if err != nil {
			t.Fatalf("unverified history must not fail the run: %v", err)
		}
		if len(unverified) != 1 || unverified[0] != "001" {
			t.Errorf("unverified = %v, want [001]", unverified)
		}
	})

	t.Run("applied without file is ignored", func(t *testing.T) {
		if _, err := VerifyAppliedChecksums(nil, []MigrationRecord{rec("001", &matchSum)}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("modified applied SQL fails with both digests named", func(t *testing.T) {
		_, err := VerifyAppliedChecksums(
			[]MigrationFile{{Version: "001", Name: "init", SQL: other}},
			[]MigrationRecord{rec("001", &matchSum)},
		)
		if err == nil {
			t.Fatal("modified applied migration accepted")
		}
		for _, want := range []string{"modified", matchSum, MigrationChecksum(other)} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q missing %q", err, want)
			}
		}
	})
}

// R1 (review-1): a v2-text history row with NULL format must be refused
// until adopted — never silently treated as unverified (NULL-checksum)
// history. Mirrors the SDK runners' verifyHistory rule (§4/§7).
func TestVerifyHistoryFormats(t *testing.T) {
	stamped := rec("001", nil)
	stamped.Format = MigrationHistoryFormat

	t.Run("all rows stamped v2 pass", func(t *testing.T) {
		if err := VerifyHistoryFormats([]MigrationRecord{stamped}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("NULL format row refuses with adopt directive", func(t *testing.T) {
		err := VerifyHistoryFormats([]MigrationRecord{stamped, rec("002", nil)})
		if err == nil {
			t.Fatal("NULL-format row accepted as runnable history")
		}
		for _, want := range []string{"002", "adopt"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q missing %q", err, want)
			}
		}
	})
	t.Run("foreign format marker refuses", func(t *testing.T) {
		foreign := rec("003", nil)
		foreign.Format = "v3"
		if err := VerifyHistoryFormats([]MigrationRecord{foreign}); err == nil {
			t.Fatal("foreign format marker accepted")
		}
	})
}

// V12: every migration/internal metadata table is protected from diff/push
// by the _neutron_ prefix rule — including the SDK claim table this card
// makes shared vocabulary.
func TestMigrationMetadataTablesAreProtected(t *testing.T) {
	for _, name := range []string{"_neutron_migrations", "_neutron_migration_lock"} {
		if !isProtectedTableName(name) {
			t.Errorf("%s must be a protected table", name)
		}
	}
	if isProtectedTableName("users") {
		t.Error("users must not be protected")
	}
}
