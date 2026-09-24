package db

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

// TestBeginTxAtomicity pins the S02 transaction helper against a real
// disposable Postgres: statements through one pgx.Tx share a single
// connection and commit or roll back as a unit, and a failed statement
// leaves nothing applied. Skipped unless NEUTRON_E2E_DATABASE_URL is set
// (NEUTRON_LIVE_REQUIRED=1 makes a missing URL a failure). Uses one
// uniquely-named s02_* database, dropped afterwards.
func TestBeginTxAtomicity(t *testing.T) {
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; BeginTx live test skipped")
	}
	dbName := fmt.Sprintf("s02_tx_%d_%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse NEUTRON_E2E_DATABASE_URL: %v", err)
	}
	u.Path = "/" + dbName
	dbURL := u.String()

	admin, err := Connect(context.Background(), base)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	t.Cleanup(admin.Close)
	if err := admin.Exec(context.Background(), fmt.Sprintf(`CREATE DATABASE %q`, dbName)); err != nil {
		t.Fatalf("create database %s: %v", dbName, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := admin.Exec(ctx, fmt.Sprintf(
			`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '%s' AND pid <> pg_backend_pid()`, dbName,
		)); err != nil {
			t.Errorf("terminate backends: %v", err)
		}
		if !strings.HasPrefix(dbName, "s02_") {
			t.Errorf("refusing to drop unexpected database %q", dbName)
			return
		}
		if err := admin.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q`, dbName)); err != nil {
			t.Errorf("drop database %s: %v", dbName, err)
		}
	})

	client, err := Connect(context.Background(), dbURL)
	if err != nil {
		t.Fatalf("connect fixture: %v", err)
	}
	defer client.Close()
	ctx := context.Background()
	if err := client.Exec(ctx, `CREATE TABLE tx_probe (id int PRIMARY KEY, v text NOT NULL)`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	count := func() int {
		var n int
		if err := client.QueryRow(ctx, `SELECT count(*) FROM tx_probe`).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		return n
	}

	// Commit persists every statement of the unit.
	tx, err := client.BeginTx(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO tx_probe VALUES (1, 'a')`); err != nil {
		t.Fatalf("insert in tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO tx_probe VALUES (2, 'b')`); err != nil {
		t.Fatalf("insert in tx: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if n := count(); n != 2 {
		t.Fatalf("after commit: %d rows, want 2", n)
	}

	// A late failure inside the transaction rolls the whole unit back,
	// including statements that succeeded before it.
	tx, err = client.BeginTx(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `INSERT INTO tx_probe VALUES (3, 'c')`); err != nil {
		t.Fatalf("insert in tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO tx_probe VALUES (3, 'duplicate')`); err == nil {
		t.Fatal("duplicate key insert must fail")
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if n := count(); n != 2 {
		t.Fatalf("after rollback: %d rows, want 2 (nothing from the failed unit)", n)
	}
}

func TestParseNucleusVersion(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"PostgreSQL 16.0 (Nucleus 0.1.0 — The Definitive Database)", "0.1.0"},
		{"PostgreSQL 16.0 (Nucleus 1.2.3)", "1.2.3"},
		{"Nucleus 0.5.0", "0.5.0"},
		{"PostgreSQL 16.0", ""},
		{"", ""},
		{"Nucleus 2.0.0-beta", "2.0.0-beta"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseNucleusVersion(tt.input)
			if got != tt.want {
				t.Errorf("parseNucleusVersion(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestStatusInfoStruct(t *testing.T) {
	info := StatusInfo{
		URL:            "postgres://localhost:5432/neutron",
		Version:        "PostgreSQL 16.0 (Nucleus 0.1.0)",
		IsNucleus:      true,
		NucleusVersion: "0.1.0",
	}

	if info.URL != "postgres://localhost:5432/neutron" {
		t.Errorf("URL = %q", info.URL)
	}
	if !info.IsNucleus {
		t.Error("IsNucleus should be true")
	}
	if info.NucleusVersion != "0.1.0" {
		t.Errorf("NucleusVersion = %q", info.NucleusVersion)
	}
}

// The unlocked status path used to CREATE the empty v2 history table
// (pre-M04 behavior, removed in M05: read-only means read-only — the
// history table is born only on a locked run or via adoption). The live
// contract is pinned by the M05 E2E status case; there is no unlocked DDL
// left here to unit-test.
