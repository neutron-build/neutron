package cmd

// Actual compiled CLI operators against a separately owned disposable PG
// database. No production endpoint or operator-supplied arbitrary SQL is used.
import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestBackfillOperatorNativeCLI(t *testing.T) {
	endpoint, native := newM02CommandDB(t, "backfill_operator")
	ctx := context.Background()
	if err := native.Exec(ctx, `CREATE TABLE public.source(id bigint PRIMARY KEY,src text,dst text);CREATE TABLE public.progress(job_id text PRIMARY KEY,job_digest text NOT NULL,format text NOT NULL,chunks bigint NOT NULL,updated_rows bigint NOT NULL);INSERT INTO public.source VALUES(1,'a',NULL),(2,'b',NULL),(3,NULL,'wrong')`); err != nil {
		t.Fatal("native fixture setup failed")
	}
	bin := filepath.Join(t.TempDir(), "neutron-cli")
	compileCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	build := exec.CommandContext(compileCtx, "go", "build", "-p=1", "-o", bin, ".")
	build.Dir = ".."
	if err := build.Run(); err != nil {
		t.Fatal("operator CLI build failed")
	}
	spec := operatorTestSpec()
	path := operatorTestFile(t, "spec.json", spec)
	run := func(action string, approval string) (map[string]any, int) {
		t.Helper()
		bounded, done := context.WithTimeout(ctx, 40*time.Second)
		defer done()
		arguments := []string{"migrate", "backfill", action, "--spec", path, "--profile", "postgres-direct"}
		if approval != "" {
			arguments = append(arguments, "--approve-job", approval)
		}
		command := exec.CommandContext(bounded, bin, arguments...)
		command.Dir = t.TempDir()
		command.Env = append(os.Environ(), "DATABASE_URL="+endpoint, "NO_COLOR=1")
		raw, err := command.Output()
		exit := 0
		if err != nil {
			nativeExit, ok := err.(*exec.ExitError)
			if !ok {
				t.Fatal("operator process did not exit normally")
			}
			exit = nativeExit.ExitCode()
		}
		var result map[string]any
		if json.Unmarshal(raw, &result) != nil {
			t.Fatal("operator emitted malformed JSON (raw diagnostics suppressed)")
		}
		return result, exit
	}
	scalar := func(sql string) string {
		t.Helper()
		var value string
		if native.QueryRow(ctx, sql).Scan(&value) != nil {
			t.Fatal("independent native query failed")
		}
		return value
	}
	first, exit := run("inspect", "")
	digest, _ := first["jobDigest"].(string)
	if exit != 0 || !operatorDigestPattern.MatchString(digest) || first["status"] != "admitted" || scalar(`SELECT count(*)::text FROM public.progress`) != "0" {
		t.Fatal("inspect effects or identity mismatch")
	}
	refused, exit := run("chunk", strings.Repeat("f", 64))
	if exit == 0 || refused["code"] != "approved_job_differs" || scalar(`SELECT count(*)::text FROM public.progress`) != "0" || scalar(`SELECT count(*)::text FROM public.source WHERE dst IS NOT NULL`) != "1" {
		t.Fatal("wrong approval caused effects")
	}
	// A different immutable writer policy must not reuse the approval digest.
	changed := spec
	changed.WriterPolicySHA256 = strings.Repeat("c", 64)
	path = operatorTestFile(t, "changed.json", changed)
	refused, exit = run("chunk", digest)
	if exit == 0 || refused["code"] != "approved_job_differs" || scalar(`SELECT count(*)::text FROM public.progress`) != "0" {
		t.Fatal("changed immutable spec adopted approval")
	}
	path = operatorTestFile(t, "original.json", spec)
	if err := native.Exec(ctx, `ALTER TABLE public.source ADD COLUMN extra text`); err != nil {
		t.Fatal("fixture shape change failed")
	}
	refused, exit = run("chunk", digest)
	if exit == 0 || refused["code"] != "approved_job_differs" || scalar(`SELECT count(*)::text FROM public.progress`) != "0" {
		t.Fatal("changed relation adopted old approval")
	}
	second, exit := run("inspect", "")
	digest, _ = second["jobDigest"].(string)
	if exit != 0 || !operatorDigestPattern.MatchString(digest) {
		t.Fatal("new native admission failed")
	}
	committed, exit := run("chunk", digest)
	if exit != 0 || committed["status"] != "committed" || committed["complete"] != false || scalar(`SELECT updated_rows::text FROM public.progress`) != "2" || scalar(`SELECT count(*)::text FROM public.source WHERE dst IS DISTINCT FROM src`) != "1" {
		t.Fatal("one chunk did not match independent progress/data")
	}
	validation, exit := run("validate", "")
	if exit == 0 || validation["status"] != "snapshot_mismatches" || validation["complete"] != false {
		t.Fatal("partial data falsely validated")
	}
	// A genuinely competing worker holding the checkpoint fences the CLI.
	blocker, err := pgx.Connect(ctx, endpoint)
	if err != nil {
		t.Fatal("native blocker connect failed")
	}
	defer blocker.Close(ctx)
	transaction, err := blocker.Begin(ctx)
	if err != nil {
		t.Fatal("native blocker begin failed")
	}
	if _, err = transaction.Exec(ctx, `SELECT job_id FROM public.progress FOR UPDATE`); err != nil {
		t.Fatal("native blocker lock failed")
	}
	busy, exit := run("chunk", digest)
	if err := transaction.Rollback(ctx); err != nil {
		t.Fatal("native blocker rollback failed")
	}
	if exit == 0 || busy["status"] != "busy" || scalar(`SELECT updated_rows::text FROM public.progress`) != "2" {
		t.Fatal("busy worker reported success or caused effects")
	}
	committed, exit = run("chunk", digest)
	if exit != 0 || committed["status"] != "committed" || scalar(`SELECT updated_rows::text FROM public.progress`) != "3" || scalar(`SELECT count(*)::text FROM public.source WHERE dst IS DISTINCT FROM src`) != "0" {
		t.Fatal("resume skipped nullable mismatch")
	}
	validation, exit = run("validate", "")
	if exit != 0 || validation["status"] != "snapshot_validated" || validation["complete"] != false {
		t.Fatal("snapshot status invalid")
	}
	idle, exit := run("chunk", digest)
	if exit != 0 || idle["status"] != "idle" || idle["complete"] != false {
		t.Fatal("idle falsely implied completion")
	}
	t.Logf("actual CLI inspect/chunk/validate; digest=%s; native shape/spec/approval refusals, busy fencing, progress and NULL mismatch verified", digest)
}
