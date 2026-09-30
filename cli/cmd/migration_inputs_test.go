package cmd

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMigrationInputsStayImmutableDuringLockWait(t *testing.T) {
	dir := t.TempDir()
	up := filepath.Join(dir, "001_first.up.sql")
	down := filepath.Join(dir, "001_first.down.sql")
	if err := os.WriteFile(up, []byte("SELECT 1;"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(down, []byte("SELECT 2;"), 0600); err != nil {
		t.Fatal(err)
	}
	inputs, cleanup, err := captureMigrationInputs(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if err := os.WriteFile(down, []byte("DROP TABLE changed_during_wait;"), 0600); err != nil {
		t.Fatal(err)
	}
	p, err := analyzeMigrations(inputs.dir, inputs.files)
	if err != nil {
		t.Fatal(err)
	}
	if p[0].DownFile.SQL != "SELECT 2;" {
		t.Fatal("companion changed after immutable capture")
	}
}

func TestMigrationInputCaptureAcrossActualAdvisoryLockWait(t *testing.T) {
	url, observer := newM02CommandDB(t, "input_bundle")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	holder, err := observer.LockMigrations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Release()
	flag := rootCmd.PersistentFlags().Lookup("url")
	old, changed := flag.Value.String(), flag.Changed
	if err := flag.Value.Set(url); err != nil {
		t.Fatal(err)
	}
	flag.Changed = true
	defer func() { _ = flag.Value.Set(old); flag.Changed = changed }()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "001_first.up.sql"), "SELECT 1;")
	writeFile(t, filepath.Join(dir, "001_first.down.sql"), "SELECT 2;")
	type result struct {
		sql string
		err error
	}
	done := make(chan result, 1)
	go func() {
		_, inputs, _, release, err := migrateSessionGuard(ctx, dir)
		if release != nil {
			defer release()
		}
		if err != nil {
			done <- result{err: err}
			return
		}
		p, err := analyzeMigrations(inputs.dir, inputs.files)
		if err != nil {
			done <- result{err: err}
			return
		}
		done <- result{sql: p[0].DownFile.SQL}
	}()
	queued := false
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		var count int
		if err := observer.QueryRow(ctx, "SELECT count(*) FROM pg_locks WHERE "+advisoryLockPredicate+" AND NOT granted AND database = (SELECT oid FROM pg_database WHERE datname = current_database())").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count > 0 {
			queued = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !queued {
		cancel()
		holder.Release()
		<-done
		t.Fatal("runner never queued for held migration lock")
	}
	writeFile(t, filepath.Join(dir, "001_first.down.sql"), "DROP TABLE changed_during_wait;")
	holder.Release()
	r := <-done
	if r.err != nil {
		t.Fatal(r.err)
	}
	if r.sql != "SELECT 2;" {
		t.Fatalf("captured companion changed while queued: %q", r.sql)
	}
}

func TestMigrationInputsCaptureAllCompanionsAndRejectNonregularFiles(t *testing.T) {
	dir := t.TempDir()
	names := []string{"001_first.up.sql", "001_first.down.sql", "001_first.plan.json", "snapshots/000_baseline.snapshot.json"}
	for _, name := range names {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		writeFile(t, path, name)
	}
	inputs, cleanup, err := captureMigrationInputs(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		writeFile(t, filepath.Join(dir, name), "edited")
		data, err := os.ReadFile(filepath.Join(inputs.dir, name))
		if err != nil || string(data) != name {
			t.Fatalf("companion %s changed: %q %v", name, data, err)
		}
	}
	captured := inputs.dir
	cleanup()
	if _, err := os.Stat(captured); !os.IsNotExist(err) {
		t.Fatalf("private bundle not removed: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, names[0])); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(dir, names[1]), filepath.Join(dir, names[0])); err != nil {
		t.Fatal(err)
	}
	if _, cleanup, err := captureMigrationInputs(dir); err == nil {
		cleanup()
		t.Fatal("symlinked input accepted")
	}
}

func TestMigrationInputComparisonDetectsEditsAdditionsAndRemovals(t *testing.T) {
	a := map[string][]byte{"a": []byte("one")}
	for _, b := range []map[string][]byte{{"a": []byte("two")}, {"a": []byte("one"), "b": nil}, {}} {
		if sameMigrationInputBytes(a, b) {
			t.Fatal("changed input set accepted")
		}
	}
}
