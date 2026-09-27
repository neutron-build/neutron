package cmd

// M07 review-1 F1: the baseline's applied-files check must hold against a
// concurrent `neutron migrate`. The runner applies 002 while the baseline
// sits between its introspection and its history read (the test seam
// schemaBaselineAfterIntrospect widens that window, as the reviewer's fault
// injection did). A baseline that then covers 002 with a document read
// before 002 ran is the R03 attempt-3 artifact.

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

// runBaselineInProcess runs `schema baseline` in-process (a private command
// with the production flags) against dbURL, so the test seam applies.
func runBaselineInProcess(t *testing.T, dbURL, mig string) error {
	t.Helper()
	urlFlag := rootCmd.PersistentFlags().Lookup("url")
	oldURL, oldChanged := urlFlag.Value.String(), urlFlag.Changed
	if err := urlFlag.Value.Set(dbURL); err != nil {
		t.Fatal(err)
	}
	urlFlag.Changed = true
	defer func() {
		_ = urlFlag.Value.Set(oldURL)
		urlFlag.Changed = oldChanged
	}()

	c := &cobra.Command{Use: "baseline"}
	c.Flags().String("dir", "", "")
	c.Flags().Duration("timeout", 30*time.Second, "")
	if err := c.ParseFlags([]string{"--dir", mig}); err != nil {
		t.Fatal(err)
	}
	c.SetContext(context.Background())
	return runSchemaBaseline(c, nil)
}

func TestM07BaselineHoldsAgainstConcurrentMigrate(t *testing.T) {
	dbURL, fx := newM02CommandDB(t, "m07race")
	bin := buildCLIBinary(t)
	mig := filepath.Join(t.TempDir(), "migrations")
	if err := os.MkdirAll(mig, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(mig, "001_init.up.sql"), m07InitSQL+";\n")
	if code, out := runCLIProcess(t, bin, dbURL, "migrate", "--dir", mig); code != 0 {
		t.Fatalf("migrate 001 exited %d:\n%s", code, out)
	}
	writeFile(t, filepath.Join(mig, "002_add_bio.up.sql"), m07BioSQL+";\n")
	snapshots := filepath.Join(mig, "snapshots")
	waiters := `SELECT count(*)::text FROM pg_locks WHERE ` + advisoryLockPredicate +
		` AND NOT granted AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`

	t.Run("RunnerDuringBaseline", func(t *testing.T) {
		var runnerOut bytes.Buffer
		runner := exec.Command(bin, "--url", dbURL, "migrate", "--dir", mig)
		runner.Stdout, runner.Stderr = &runnerOut, &runnerOut
		done := make(chan error, 1)
		var runnerErr error
		finished, queued := false, false

		// In the window, start the runner and wait until it has either
		// finished (nothing held it off) or is queued on the migration lock.
		schemaBaselineAfterIntrospect = func() {
			if err := runner.Start(); err != nil {
				t.Errorf("start migrate: %v", err)
				return
			}
			go func() { done <- runner.Wait() }()
			deadline := time.Now().Add(30 * time.Second)
			for time.Now().Before(deadline) {
				select {
				case runnerErr = <-done:
					finished = true
					return
				default:
				}
				if q09Query(t, fx, waiters) != "0" {
					queued = true
					return
				}
				time.Sleep(20 * time.Millisecond)
			}
			t.Errorf("migrate neither finished nor queued on the migration lock within 30s")
		}
		defer func() { schemaBaselineAfterIntrospect = nil }()

		baselineErr := runBaselineInProcess(t, dbURL, mig)
		schemaBaselineAfterIntrospect = nil
		if !finished && runner.Process != nil {
			runnerErr = <-done
		}
		if runnerErr != nil {
			t.Fatalf("concurrent migrate failed: %v\n%s", runnerErr, runnerOut.String())
		}
		if got := q09Query(t, fx, `SELECT string_agg(version, ',' ORDER BY version) FROM _neutron_migrations`); got != "001,002" {
			t.Fatalf("history after the concurrent migrate = %s, want 001,002", got)
		}

		if baselineErr == nil {
			raw := mustReadFile(t, filepath.Join(snapshots, "000_baseline.snapshot.json"))
			var snap struct{ Covers []string }
			if err := json.Unmarshal(raw, &snap); err != nil {
				t.Fatal(err)
			}
			t.Fatalf("baseline succeeded with covers %v and a document that has bio: %v, while migrate applied 002 between its introspection and its history read (runner finished inside the window: %v) — a baseline covering a file its document does not reflect",
				snap.Covers, strings.Contains(string(raw), `"bio"`), finished)
		}
		if !queued {
			t.Fatalf("the runner was not queued behind the baseline's migration lock (finished inside the window: %v); baseline error: %v", finished, baselineErr)
		}
		if msg := baselineErr.Error(); !strings.Contains(msg, "002") || !strings.Contains(msg, "not applied") {
			t.Fatalf("the baseline read the history before the runner ran, so it must refuse the unapplied 002: %v", baselineErr)
		}
		if _, err := os.Stat(snapshots); !os.IsNotExist(err) {
			t.Fatalf("a refused baseline must write nothing (snapshots dir: %v)", err)
		}

		// Once the runner has finished, the baseline covers 002 and reflects it.
		if code, out := runCLIProcess(t, bin, dbURL, "schema", "baseline", "--dir", mig); code != 0 {
			t.Fatalf("baseline after migrate exited %d:\n%s", code, out)
		}
		doc := string(mustReadFile(t, filepath.Join(snapshots, "000_baseline.snapshot.json")))
		if !strings.Contains(doc, `"bio"`) {
			t.Fatalf("baseline covering 002 must list the bio column:\n%s", doc)
		}
		if err := os.RemoveAll(snapshots); err != nil {
			t.Fatal(err)
		}
	})

	// A runner holding the lock: the baseline waits for it like the other
	// runners do, within --timeout, and writes nothing if it runs out.
	t.Run("BaselineWaitsForRunner", func(t *testing.T) {
		if code, out := runCLIProcess(t, bin, dbURL, "migrate", "--dir", mig); code != 0 {
			t.Fatalf("migrate exited %d:\n%s", code, out)
		}
		sess, err := fx.LockMigrations(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer sess.Release()
		code, out := runCLIProcess(t, bin, dbURL, "schema", "baseline", "--dir", mig, "--timeout", "2s")
		if code == 0 || !strings.Contains(out, "migration lock") {
			t.Fatalf("baseline must wait for the held migration lock and give up at --timeout (%d):\n%s", code, out)
		}
		if _, err := os.Stat(snapshots); !os.IsNotExist(err) {
			t.Fatalf("a baseline that timed out must write nothing (snapshots dir: %v)", err)
		}
		sess.Release()
		if code, out := runCLIProcess(t, bin, dbURL, "schema", "baseline", "--dir", mig); code != 0 {
			t.Fatalf("baseline after the lock is released exited %d:\n%s", code, out)
		}
	})
}
