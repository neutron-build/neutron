package db

import (
	"context"
	"testing"
)

func newBackfillHarness(t *testing.T) (*q07Harness, BackfillJob) {
	h := newQ07Harness(t, "backfill")
	h.exec(`CREATE TABLE public.source(id bigint PRIMARY KEY,src text,dst text CHECK(dst IS NULL OR dst <> 'fail'))`)
	h.exec(`CREATE TABLE public.progress(job_id text PRIMARY KEY,job_digest text NOT NULL,format text NOT NULL,chunks bigint NOT NULL,updated_rows bigint NOT NULL)`)
	job, err := InspectBackfillJob(context.Background(), h.client, backfillFixtureSpec())
	if err != nil {
		t.Fatal(err)
	}
	return h, job
}
func TestBackfillNativeAtomicResumeAndConcurrentWrites(t *testing.T) {
	h, job := newBackfillHarness(t)
	ctx := context.Background()
	h.exec(`INSERT INTO public.source VALUES(1,'a',NULL),(2,'b',NULL),(3,NULL,NULL)`)
	result, err := RunBackfillChunk(ctx, h.client, job)
	if err != nil || result.Status != "committed" || result.UpdatedRows != 2 {
		t.Fatalf("initial %+v %v", result, err)
	}
	if h.queryOne(`SELECT dst FROM public.source WHERE id=1`) != "a" || h.queryOne(`SELECT updated_rows::text FROM public.progress`) != "2" {
		t.Fatal("independent native copy/checkpoint mismatch")
	}
	// Keyset/highwater implementations would miss this lower key and the
	// already-copied source change. Full mismatch re-evaluation catches both.
	h.exec(`INSERT INTO public.source VALUES(-9,'late',NULL);UPDATE public.source SET src='changed' WHERE id=1`)
	result, err = RunBackfillChunk(ctx, h.client, job)
	if err != nil || result.UpdatedRows != 2 {
		t.Fatalf("reconciliation %+v %v", result, err)
	}
	if h.queryOne(`SELECT dst FROM public.source WHERE id=-9`) != "late" || h.queryOne(`SELECT dst FROM public.source WHERE id=1`) != "changed" {
		t.Fatal("low-key/update rows skipped")
	}
	v, err := ValidateBackfill(ctx, h.client, job)
	if err != nil || !v.SnapshotValidated || v.Mismatches != 0 {
		t.Fatalf("validation %+v %v", v, err)
	}
	h.exec(`UPDATE public.source SET src='later' WHERE id=2`)
	v, err = ValidateBackfill(ctx, h.client, job)
	if err != nil || v.Mismatches != 1 {
		t.Fatalf("snapshot incorrectly implied future completion %+v %v", v, err)
	}
}
func TestBackfillNativeRollbackAndLockedRows(t *testing.T) {
	h, job := newBackfillHarness(t)
	ctx := context.Background()
	h.exec(`INSERT INTO public.source VALUES(1,'safe',NULL),(2,'fail',NULL)`)
	if _, err := RunBackfillChunk(ctx, h.client, job); err == nil {
		t.Fatal("CHECK failure not surfaced")
	}
	if h.queryOne(`SELECT count(*)::text FROM public.source WHERE dst IS NOT NULL`) != "0" || h.queryOne(`SELECT count(*)::text FROM public.progress`) != "0" {
		t.Fatal("chunk/checkpoint failure not atomic")
	}
	h.exec(`UPDATE public.source SET src='ok' WHERE id=2`)
	lock, err := h.client.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer backfillRollback(lock)
	if _, err := lock.Exec(ctx, `SELECT id FROM public.source FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	idle, err := RunBackfillChunk(ctx, h.client, job)
	if err != nil || idle.Status != "idle" || idle.UpdatedRows != 0 {
		t.Fatalf("locked candidates %+v %v", idle, err)
	}
	v, err := ValidateBackfill(ctx, h.client, job)
	if err != nil || v.Mismatches != 2 {
		t.Fatalf("idle treated as completion %+v %v", v, err)
	}
	if err := lock.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	// A different process owning the checkpoint row fences another worker.
	hold, err := h.client.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer backfillRollback(hold)
	if _, err := hold.Exec(ctx, `SELECT job_id FROM public.progress FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	busy, err := RunBackfillChunk(ctx, h.client, job)
	if err != nil || busy.Status != "busy" {
		t.Fatalf("competing worker %+v %v", busy, err)
	}
	if err := hold.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	done, err := RunBackfillChunk(ctx, h.client, job)
	if err != nil || done.UpdatedRows != 2 {
		t.Fatalf("resume %+v %v", done, err)
	}
}
func TestBackfillNativeRefusesChangedIdentityAndUnknownCheckpoint(t *testing.T) {
	h, job := newBackfillHarness(t)
	ctx := context.Background()
	h.exec(`INSERT INTO public.source VALUES(1,'data',NULL)`)
	h.exec(`INSERT INTO public.progress VALUES('job','unknown','legacy',0,0)`)
	if _, err := RunBackfillChunk(ctx, h.client, job); err == nil {
		t.Fatal("unknown checkpoint adopted")
	}
	if h.queryOne(`SELECT count(*)::text FROM public.source WHERE dst IS NOT NULL`) != "0" {
		t.Fatal("admission failure changed rows")
	}
	h.exec(`DELETE FROM public.progress;ALTER TABLE public.source ADD COLUMN changed int`)
	if _, err := RunBackfillChunk(ctx, h.client, job); err == nil {
		t.Fatal("changed relation definition admitted")
	}
	h.exec(`CREATE DOMAIN public.int8 AS bigint; CREATE TABLE public.domain_source(id public.int8 PRIMARY KEY,src text,dst text)`)
	s := backfillFixtureSpec()
	s.Source.Name = "domain_source"
	if _, err := InspectBackfillJob(ctx, h.client, s); err == nil {
		t.Fatal("builtin-looking domain key admitted")
	}
}
