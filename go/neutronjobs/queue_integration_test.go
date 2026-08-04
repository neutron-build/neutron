package neutronjobs

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/neutron-dev/neutron-go/nucleus"
)

// These exercise the lease, reaper, and shutdown paths against a real database,
// because none of them can be verified any other way: every defect they cover is
// a disagreement between what the SDK believes it wrote and what the database
// actually stored. A mock would be written from the same belief.
//
// Run with a live Nucleus or PostgreSQL:
//
//	NEUTRON_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:55432/postgres \
//	    go test ./neutronjobs/ -run Integration -v

func testQueue(t *testing.T, opts ...QueueOption) (*Queue, context.Context) {
	t.Helper()

	url := os.Getenv("NEUTRON_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("NEUTRON_TEST_DATABASE_URL not set; skipping database integration test")
	}

	ctx := context.Background()
	client, err := nucleus.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(client.Close)

	q := NewQueue(client, opts...)
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	return q, ctx
}

// uniqueType keeps concurrent tests from claiming each other's jobs.
func uniqueType(t *testing.T) string {
	t.Helper()
	return "test_" + t.Name() + "_" + generateJobID()[:8]
}

func statusOf(t *testing.T, q *Queue, ctx context.Context, id string) string {
	t.Helper()
	rows, err := q.client.Pool().Query(ctx, "SELECT status FROM _neutron_jobs WHERE id = $1", id)
	if err != nil {
		t.Fatalf("query status: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("job %s not found", id)
	}
	var status string
	if err := rows.Scan(&status); err != nil {
		t.Fatalf("scan status: %v", err)
	}
	return status
}

// A claim must record who holds the job and until when. Without both, a dead
// worker is indistinguishable from a slow one and nothing can recover the job.
func TestIntegrationClaimRecordsLease(t *testing.T) {
	q, ctx := testQueue(t)
	jobType := uniqueType(t)

	id, err := Enqueue(ctx, q, jobType, map[string]string{"x": "1"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		defer close(done)
		_ = q.Process(runCtx, jobType, func(ctx context.Context, payload []byte) error {
			close(started)
			<-release
			return nil
		}, 1)
	}()

	select {
	case <-started:
	case <-time.After(15 * time.Second):
		t.Fatal("handler never started")
	}

	rows, err := q.client.Pool().Query(ctx,
		"SELECT worker_id, lease_expires_at FROM _neutron_jobs WHERE id = $1", id)
	if err != nil {
		t.Fatalf("query lease: %v", err)
	}
	var workerID *string
	var leaseAt *time.Time
	if rows.Next() {
		if err := rows.Scan(&workerID, &leaseAt); err != nil {
			rows.Close()
			t.Fatalf("scan lease: %v", err)
		}
	}
	rows.Close()

	if workerID == nil || *workerID == "" {
		t.Error("claim did not record a worker_id; a stranded job cannot be traced to its holder")
	}
	if leaseAt == nil {
		t.Fatal("claim did not record lease_expires_at; nothing can ever reclaim this job")
	}
	if !leaseAt.After(time.Now()) {
		t.Errorf("lease already expired at claim time: %v", *leaseAt)
	}

	close(release)
	cancel()

	// Wait for the worker to finish before the deferred client close, or the
	// terminal write races the pool shutdown and the suite goes flaky.
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Error("Process did not return after cancellation")
	}
}

// The core recovery property: a job whose worker vanished goes back to pending.
// Simulated by writing the row a dead worker would have left behind.
func TestIntegrationReaperRequeuesExpiredLease(t *testing.T) {
	q, ctx := testQueue(t)
	jobType := uniqueType(t)

	id, err := Enqueue(ctx, q, jobType, map[string]string{"x": "1"}, WithRetry(3, time.Second))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// The state a killed worker leaves: running, attempts consumed, lease long past.
	if _, err := q.client.SQL().Exec(ctx,
		`UPDATE _neutron_jobs SET status = 'running', attempts = 1,
		 lease_expires_at = $1, worker_id = 'dead-worker' WHERE id = $2`,
		time.Now().Add(-time.Hour), id); err != nil {
		t.Fatalf("simulate dead worker: %v", err)
	}

	requeued, dead, err := q.Reap(ctx, jobType)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if requeued != 1 {
		t.Errorf("requeued = %d, want 1", requeued)
	}
	if dead != 0 {
		t.Errorf("dead_lettered = %d, want 0", dead)
	}
	if got := statusOf(t, q, ctx, id); got != string(JobPending) {
		t.Errorf("status = %q, want pending — a dead worker stranded the job", got)
	}
}

// A job that keeps killing its worker must stop being handed out. Otherwise the
// reaper turns one bad payload into an unbounded crash loop.
func TestIntegrationReaperDeadLettersExhausted(t *testing.T) {
	q, ctx := testQueue(t)
	jobType := uniqueType(t)

	id, err := Enqueue(ctx, q, jobType, map[string]string{"x": "1"}, WithRetry(2, time.Second))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	if _, err := q.client.SQL().Exec(ctx,
		`UPDATE _neutron_jobs SET status = 'running', attempts = 2,
		 lease_expires_at = $1, worker_id = 'dead-worker' WHERE id = $2`,
		time.Now().Add(-time.Hour), id); err != nil {
		t.Fatalf("simulate dead worker: %v", err)
	}

	requeued, dead, err := q.Reap(ctx, jobType)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if dead != 1 {
		t.Errorf("dead_lettered = %d, want 1", dead)
	}
	if requeued != 0 {
		t.Errorf("requeued = %d, want 0 — an exhausted job must not go round again", requeued)
	}
	if got := statusOf(t, q, ctx, id); got != string(JobDeadLetter) {
		t.Errorf("status = %q, want dead_letter", got)
	}
}

// A lease that is merely stale must not be reaped. Reaping a live worker's job
// runs it twice, which is worse than the stranding this whole mechanism exists
// to fix, so the grace margin is load-bearing.
func TestIntegrationReaperRespectsGrace(t *testing.T) {
	q, ctx := testQueue(t)
	jobType := uniqueType(t)

	id, err := Enqueue(ctx, q, jobType, map[string]string{"x": "1"}, WithRetry(3, time.Second))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Expired one second ago — inside the default ten-second grace.
	if _, err := q.client.SQL().Exec(ctx,
		`UPDATE _neutron_jobs SET status = 'running', attempts = 1,
		 lease_expires_at = $1, worker_id = 'slow-worker' WHERE id = $2`,
		time.Now().Add(-time.Second), id); err != nil {
		t.Fatalf("simulate slow worker: %v", err)
	}

	requeued, dead, err := q.Reap(ctx, jobType)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if requeued != 0 || dead != 0 {
		t.Errorf("reaped inside the grace window (requeued=%d dead=%d); clock skew would double-run jobs", requeued, dead)
	}
	if got := statusOf(t, q, ctx, id); got != string(JobRunning) {
		t.Errorf("status = %q, want running", got)
	}
}

// A panicking handler must not take the worker process down, and must consume
// its retries like any other failure.
func TestIntegrationPanicIsContainedAndFails(t *testing.T) {
	q, ctx := testQueue(t)
	jobType := uniqueType(t)

	id, err := Enqueue(ctx, q, jobType, map[string]string{"x": "1"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	runCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	var calls atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = q.Process(runCtx, jobType, func(ctx context.Context, payload []byte) error {
			calls.Add(1)
			panic("poison payload")
		}, 1)
	}()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if statusOf(t, q, ctx, id) == string(JobFailed) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	if got := statusOf(t, q, ctx, id); got != string(JobFailed) {
		t.Errorf("status = %q, want failed — a panic must be recorded, not crash the worker", got)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("handler called %d times, want 1", n)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Error("Process did not return after cancellation")
	}
}

// The double-delivery regression. A handler that finishes its work while the
// process is shutting down must still be recorded as completed. Writing the
// terminal update on the cancelled context loses that record, the lease lapses,
// and the reaper hands the same job to another worker — so every deploy silently
// re-runs whatever was in flight.
func TestIntegrationCompletionSurvivesShutdown(t *testing.T) {
	q, ctx := testQueue(t)
	jobType := uniqueType(t)

	id, err := Enqueue(ctx, q, jobType, map[string]string{"x": "1"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	started := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)
		_ = q.Process(runCtx, jobType, func(hctx context.Context, payload []byte) error {
			close(started)
			// Shutdown arrives mid-job; the work itself still finishes.
			<-hctx.Done()
			return nil
		}, 1)
	}()

	select {
	case <-started:
	case <-time.After(15 * time.Second):
		t.Fatal("handler never started")
	}

	cancel()

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("Process did not drain in-flight jobs before returning")
	}

	if got := statusOf(t, q, ctx, id); got != string(JobCompleted) {
		t.Errorf("status = %q, want completed — the completion write did not survive shutdown, "+
			"so the reaper would hand this job to another worker", got)
	}
}

// A handler interrupted by shutdown has not failed, so it must go back to the
// queue without burning a retry.
func TestIntegrationInterruptedJobIsReturnedUnpenalised(t *testing.T) {
	q, ctx := testQueue(t)
	jobType := uniqueType(t)

	id, err := Enqueue(ctx, q, jobType, map[string]string{"x": "1"}, WithRetry(3, time.Second))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	started := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)
		_ = q.Process(runCtx, jobType, func(hctx context.Context, payload []byte) error {
			close(started)
			<-hctx.Done()
			return errors.New("interrupted")
		}, 1)
	}()

	select {
	case <-started:
	case <-time.After(15 * time.Second):
		t.Fatal("handler never started")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("Process did not drain")
	}

	if got := statusOf(t, q, ctx, id); got != string(JobPending) {
		t.Errorf("status = %q, want pending", got)
	}

	rows, err := q.client.Pool().Query(ctx, "SELECT attempts FROM _neutron_jobs WHERE id = $1", id)
	if err != nil {
		t.Fatalf("query attempts: %v", err)
	}
	var attempts int
	if rows.Next() {
		_ = rows.Scan(&attempts)
	}
	rows.Close()

	if attempts != 0 {
		t.Errorf("attempts = %d, want 0 — a deploy must not consume a job's retry budget", attempts)
	}
}

// EnsureSchema must upgrade a table that predates leases. CREATE TABLE IF NOT
// EXISTS silently does nothing when the table is already there, so without the
// ALTERs an upgraded application fails on every claim.
func TestIntegrationSchemaMigratesLegacyTable(t *testing.T) {
	url := os.Getenv("NEUTRON_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("NEUTRON_TEST_DATABASE_URL not set; skipping database integration test")
	}

	ctx := context.Background()
	client, err := nucleus.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	if _, err := client.SQL().Exec(ctx, "DROP TABLE IF EXISTS _neutron_jobs"); err != nil {
		t.Fatalf("drop: %v", err)
	}

	// The pre-lease shape, verbatim.
	legacy := `CREATE TABLE _neutron_jobs (
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
		error TEXT
	)`
	if _, err := client.SQL().Exec(ctx, legacy); err != nil {
		t.Fatalf("create legacy table: %v", err)
	}

	q := NewQueue(client)
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema over a legacy table: %v", err)
	}

	// The claim must now work end to end against the migrated table.
	jobType := uniqueType(t)
	if _, err := Enqueue(ctx, q, jobType, map[string]string{"x": "1"}); err != nil {
		t.Fatalf("enqueue after migration: %v", err)
	}
	rows, err := client.Pool().Query(ctx, claimJobSQL, jobType, time.Now().Add(time.Minute), "test-worker")
	if err != nil {
		t.Fatalf("claim against migrated table: %v", err)
	}
	rows.Close()
}
