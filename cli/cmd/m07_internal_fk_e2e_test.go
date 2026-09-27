package cmd

// M07 review-1 F3 through the REAL CLI binary: a user table with a foreign
// key into the job queues' _neutron_jobs table. pull and baseline refuse
// with the table, the key, the internal target and the options, and write
// nothing; once the key is dropped both succeed and leave _neutron_jobs out.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// m07JobsTableSQL is the Go job queue's table (go/neutronjobs EnsureSchema).
const m07JobsTableSQL = `CREATE TABLE IF NOT EXISTS _neutron_jobs (
		id TEXT PRIMARY KEY,
		job_type TEXT NOT NULL,
		payload JSONB NOT NULL DEFAULT '{}',
		status TEXT NOT NULL DEFAULT 'pending',
		attempts INT NOT NULL DEFAULT 0,
		max_retry INT NOT NULL DEFAULT 0,
		backoff_ms BIGINT NOT NULL DEFAULT 1000,
		run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		deadline TIMESTAMPTZ,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		lease_expires_at TIMESTAMPTZ,
		worker_id TEXT,
		error TEXT
	)`

func TestM07ForeignKeyIntoInternalTable(t *testing.T) {
	dbURL, fx := newM02CommandDB(t, "m07jobfk")
	bin := buildCLIBinary(t)
	ctx := context.Background()
	for _, sql := range []string{
		m07JobsTableSQL,
		`CREATE TABLE job_results (id integer PRIMARY KEY, job_id text REFERENCES _neutron_jobs(id))`,
	} {
		if err := fx.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	work := t.TempDir()
	pulled := filepath.Join(work, "pulled.json")
	mig := filepath.Join(work, "migrations")
	if err := os.MkdirAll(mig, 0o755); err != nil {
		t.Fatal(err)
	}
	wants := []string{
		"table public.job_results has foreign key job_results_job_id_fkey referencing public._neutron_jobs",
		"_neutron_* tables are neutron-managed",
		"cannot be declared in a schema document",
		"drop the foreign key or replace it with one to a table you own",
		"leave public.job_results unmanaged",
	}
	refused := func(t *testing.T, args ...string) {
		t.Helper()
		code, out := runCLIProcess(t, bin, dbURL, args...)
		if code == 0 {
			t.Fatalf("neutron %s must be refused:\n%s", strings.Join(args, " "), out)
		}
		for _, w := range wants {
			if !strings.Contains(out, w) {
				t.Errorf("neutron %s refusal must say %q:\n%s", strings.Join(args, " "), w, out)
			}
		}
		if n := strings.Count(out, "job_results_job_id_fkey"); n != 1 {
			t.Errorf("the refusal must be printed once (key named %d times):\n%s", n, out)
		}
	}

	refused(t, "schema", "pull", "--out", pulled)
	if _, err := os.Stat(pulled); !os.IsNotExist(err) {
		t.Fatalf("a refused pull must write nothing (%v)", err)
	}
	refused(t, "schema", "baseline", "--dir", mig)
	if _, err := os.Stat(filepath.Join(mig, "snapshots")); !os.IsNotExist(err) {
		t.Fatalf("a refused baseline must write nothing (%v)", err)
	}

	// The documented way out: drop the key.
	if err := fx.Exec(ctx, `ALTER TABLE job_results DROP CONSTRAINT job_results_job_id_fkey`); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"schema", "pull", "--out", pulled},
		{"schema", "baseline", "--dir", mig},
	} {
		code, out := runCLIProcess(t, bin, dbURL, args...)
		if code != 0 || !strings.Contains(out, "public._neutron_jobs") {
			t.Fatalf("neutron %s after dropping the key (%d) must succeed and leave _neutron_jobs out:\n%s", strings.Join(args, " "), code, out)
		}
	}
	if got := strings.Join(m07DocTables(t, pulled), ","); got != "public.job_results" {
		t.Fatalf("pulled document lists %s, want public.job_results", got)
	}
}
