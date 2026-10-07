package neutronjobs

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/neutron-build/neutron/go/nucleus"
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
	rows, err := client.Pool().Query(ctx, claimJobSQL, jobType, time.Now().Add(time.Minute), "test-worker", "test-token")
	if err != nil {
		t.Fatalf("claim against migrated table: %v", err)
	}
	rows.Close()

	// A claim through the queue must now also mint and record a fencing token.
	if _, err := Enqueue(ctx, q, jobType, map[string]string{"x": "2"}); err != nil {
		t.Fatalf("enqueue for token check: %v", err)
	}
	q2 := NewQueue(client)
	c, err := q2.claimOne(ctx, jobType)
	if err != nil {
		t.Fatalf("claimOne after migration: %v", err)
	}
	if c == nil {
		t.Fatal("claimOne found nothing after migration")
	}
	if c.token == "" {
		t.Error("claim did not mint a claim token; terminal writes cannot be fenced")
	}
}

// rowState is what the fencing tests assert against, all in one read.
type rowState struct {
	status   string
	attempts int
	errorMsg string
	token    string
}

func stateOf(t *testing.T, q *Queue, ctx context.Context, id string) rowState {
	t.Helper()
	rows, err := q.client.Pool().Query(ctx,
		`SELECT status, attempts, COALESCE(error, ''), COALESCE(claim_token, '') FROM _neutron_jobs WHERE id = $1`, id)
	if err != nil {
		t.Fatalf("query state: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("job %s not found", id)
	}
	var s rowState
	if err := rows.Scan(&s.status, &s.attempts, &s.errorMsg, &s.token); err != nil {
		t.Fatalf("scan state: %v", err)
	}
	return s
}

// reclaim simulates the recovery path end to end: the reaper returns the row
// to pending, then another worker claims it, minting a fresh token. Both
// queues deliberately share one worker ID — worker_id is a process identity
// and the whole point of NA-01 is that it must not be an ownership identity.
func reclaim(t *testing.T, q *Queue, ctx context.Context, jobType string) *claimed {
	t.Helper()
	if _, _, err := q.Reap(ctx, jobType); err != nil {
		t.Fatalf("reap: %v", err)
	}
	c, err := q.claimOne(ctx, jobType)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if c == nil {
		t.Fatal("reclaim found nothing")
	}
	return c
}

// The NA-01 core invariant: only the current claim may mutate the queue
// record for that claim. Worker A claims, loses the job (expired lease →
// reaper → worker B claims), and then A's handler finishes anyway. A's
// terminal write must affect zero rows: B's status, token, attempt count and
// error must all be exactly what B left them. With the old ID-only terminal
// writes, A marked B's attempt completed (or requeued it, or burned its
// retries) — and because both claims carry the same worker_id here, the old
// heartbeat predicate would have been no protection either.
func TestIntegrationStaleAttemptCannotMutateNewerClaim(t *testing.T) {
	q, ctx := testQueue(t, WithWorkerID("same-process"), WithReaperInterval(50*time.Millisecond))
	jobType := uniqueType(t)

	id, err := Enqueue(ctx, q, jobType, map[string]string{"x": "1"}, WithRetry(3, time.Second))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	a, err := q.claimOne(ctx, jobType)
	if err != nil || a == nil {
		t.Fatalf("first claim: %v %v", a, err)
	}

	// Expire A's lease so the reaper will hand the row back, then let B win it.
	if _, err := q.client.SQL().Exec(ctx,
		`UPDATE _neutron_jobs SET lease_expires_at = $1 WHERE id = $2`,
		time.Now().Add(-time.Hour), id); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	b := reclaim(t, q, ctx, jobType)
	if b.token == a.token {
		t.Fatal("reclaim minted the same token; the test cannot distinguish attempts")
	}

	before := stateOf(t, q, ctx, id)

	handler := func(context.Context, []byte) error { return nil }
	// All four terminal paths, attempted by the stale claim A.
	q.executeClaim(ctx, *a, handler)                       // success → completion write
	q.executeClaim(ctx, *a, func(context.Context, []byte) error {
		return errors.New("stale failure")
	}) // failure → retry write

	// Shutdown-release and final-failure need attempts to differ, so point the
	// stale claim at maxRetry boundaries through direct fenced writes.
	shutdownCtx, cancel := context.WithCancel(ctx)
	cancel()
	if ok := q.fencedExec(shutdownCtx, releaseJobSQL, "shutdown release", *a, "id", a.id, "token", a.token); ok {
		t.Error("stale shutdown release matched rows; a newer attempt was mutated")
	}
	if ok := q.fencedExec(ctx, failJobSQL, "failure", *a, "id", a.id, "token", a.token, "error", "stale"); ok {
		t.Error("stale final-failure write matched rows; a newer attempt was mutated")
	}

	after := stateOf(t, q, ctx, id)
	if after != before {
		t.Errorf("stale attempt mutated the newer claim:\n  before: %+v\n  after:  %+v", before, after)
	}
	if after.status != string(JobRunning) || after.token != b.token {
		t.Errorf("job no longer owned by the newer claim: %+v", after)
	}

	// The current claim's writes still land, which is the other half: fencing
	// must not lock the rightful owner out.
	q.executeClaim(ctx, *b, handler)
	if got := statusOf(t, q, ctx, id); got != string(JobCompleted) {
		t.Errorf("current claim completion: status = %q, want completed", got)
	}
}

// Renewal that affects zero rows must cancel the handler (this is also the
// path a lost lease takes): another worker owns the job now, so running the
// handler to completion would execute the job twice.
func TestIntegrationLostLeaseCancelsHandler(t *testing.T) {
	q, ctx := testQueue(t, WithLease(2*time.Second))
	jobType := uniqueType(t)

	id, err := Enqueue(ctx, q, jobType, map[string]string{"x": "1"}, WithRetry(3, time.Second))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	a, err := q.claimOne(ctx, jobType)
	if err != nil || a == nil {
		t.Fatalf("claim: %v %v", a, err)
	}

	handlerDone := make(chan error, 1)
	go func() {
		q.executeClaim(ctx, *a, func(hctx context.Context, _ []byte) error {
			<-hctx.Done()
			return hctx.Err()
		})
		handlerDone <- nil
	}()

	// Steal the row the way the reaper + another claim would: token cleared,
	// then re-claimed by someone else.
	if _, err := q.client.SQL().Exec(ctx,
		`UPDATE _neutron_jobs SET status = 'pending', lease_expires_at = NULL,
		 worker_id = NULL, claim_token = NULL WHERE id = $1`, id); err != nil {
		t.Fatalf("steal: %v", err)
	}
	if b := reclaim(t, q, ctx, jobType); b == nil {
		t.Fatal("reclaim after steal found nothing")
	}

	select {
	case <-handlerDone:
	case <-time.After(10 * time.Second):
		t.Fatal("handler kept running after its lease was lost; it would run the job twice")
	}

	// The stale attempt's failure write must not have touched the new owner.
	s := stateOf(t, q, ctx, id)
	if s.status != string(JobRunning) {
		t.Errorf("status = %q, want running under the new claim", s.status)
	}
}

// A handler whose renewals keep failing (a partitioned database) must stop at
// the last lease boundary the database confirmed — not run forever. This test
// closes the client after the claim, so every renewal errors; the watchdog is
// the only thing that can end the handler.
func TestIntegrationRenewalErrorsBoundHandlerLifetime(t *testing.T) {
	q, ctx := testQueue(t, WithLease(2*time.Second))
	jobType := uniqueType(t)

	if _, err := Enqueue(ctx, q, jobType, map[string]string{"x": "1"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	a, err := q.claimOne(ctx, jobType)
	if err != nil || a == nil {
		t.Fatalf("claim: %v %v", a, err)
	}

	// Every database call from here on fails. Before NA-01 the heartbeat
	// treated a renewal error as "keep working", so this handler would still
	// be running when the test gave up on it.
	q.client.Close()

	handlerDone := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(handlerDone)
		q.executeClaim(ctx, *a, func(hctx context.Context, _ []byte) error {
			<-hctx.Done()
			return hctx.Err()
		})
	}()

	select {
	case <-handlerDone:
	case <-time.After(15 * time.Second):
		t.Fatal("handler outlived its confirmed lease boundary; renewal failures did not bound its lifetime")
	}
	if elapsed := time.Since(start); elapsed > 6*time.Second {
		t.Errorf("handler stopped after %v; the watchdog should fire near the 2s lease boundary", elapsed)
	}
}

// NA-02: a claim may only be taken by a worker with capacity to start it.
// With concurrency 1 and handler A blocked, a second job must stay pending —
// not be leased into 'running' with no heartbeat, where another worker's
// reaper would reclaim and run it while this process still held its payload.
func TestIntegrationClaimRequiresCapacity(t *testing.T) {
	q, ctx := testQueue(t)
	jobType := uniqueType(t)

	idA, err := Enqueue(ctx, q, jobType, map[string]string{"x": "a"})
	if err != nil {
		t.Fatalf("enqueue A: %v", err)
	}
	idB, err := Enqueue(ctx, q, jobType, map[string]string{"x": "b"})
	if err != nil {
		t.Fatalf("enqueue B: %v", err)
	}

	aStarted := make(chan struct{})
	var startedOnce sync.Once
	release := make(chan struct{})
	done := make(chan struct{})
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// The first handler invocation is job A; it holds the only worker slot
	// until released. Job B cannot even be claimed until that happens, which
	// is the invariant under test. (Payload bytes are not compared: JSONB
	// re-renders them on read-back.)
	go func() {
		defer close(done)
		_ = q.Process(runCtx, jobType, func(ctx context.Context, payload []byte) error {
			startedOnce.Do(func() { close(aStarted) })
			<-release
			return nil
		}, 1)
	}()

	select {
	case <-aStarted:
	case <-time.After(15 * time.Second):
		t.Fatal("handler A never started")
	}

	// A holds the only slot. B must remain pending and untouched: the old
	// claim-then-wait loop leased it 'running' here, where nothing heartbeat
	// it and another worker's reaper would run it under this worker's feet.
	s := stateOf(t, q, ctx, idB)
	if s.status != string(JobPending) {
		t.Errorf("job B status = %q while all workers are busy; want pending (no claim without capacity)", s.status)
	}
	if s.attempts != 0 {
		t.Errorf("job B attempts = %d while parked; want 0 — parked claims burn the retry budget", s.attempts)
	}

	close(release)

	// The slot frees, so B may now be claimed and run to completion.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if statusOf(t, q, ctx, idB) == string(JobCompleted) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if got := statusOf(t, q, ctx, idB); got != string(JobCompleted) {
		t.Errorf("job B status = %q, want completed", got)
	}
	if got := statusOf(t, q, ctx, idA); got != string(JobCompleted) {
		t.Errorf("job A status = %q, want completed", got)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("Process did not drain")
	}
}

// NA-03: a running row whose lease is NULL — the state a deployment that
// predates leases leaves behind — is invisible to the reaper forever. The
// explicit cutover migration must hand it back to the normal recovery path
// without touching rows that have a live lease.
func TestIntegrationLegacyNullLeaseRecovery(t *testing.T) {
	q, ctx := testQueue(t)
	jobType := uniqueType(t)

	legacyID, err := Enqueue(ctx, q, jobType, map[string]string{"x": "legacy"}, WithRetry(3, time.Second))
	if err != nil {
		t.Fatalf("enqueue legacy: %v", err)
	}
	deadID, err := Enqueue(ctx, q, jobType, map[string]string{"x": "dead"}, WithRetry(1, time.Second))
	if err != nil {
		t.Fatalf("enqueue dead: %v", err)
	}
	liveID, err := Enqueue(ctx, q, jobType, map[string]string{"x": "live"}, WithRetry(3, time.Second))
	if err != nil {
		t.Fatalf("enqueue live: %v", err)
	}

	// The state a pre-lease worker leaves behind: running, no lease at all.
	// The dead row has already spent its single attempt, the way a worker that
	// died mid-run would have left it.
	if _, err := q.client.SQL().Exec(ctx,
		`UPDATE _neutron_jobs SET status = 'running', attempts = 0, lease_expires_at = NULL,
		 worker_id = 'legacy-worker', claim_token = NULL WHERE id = $1`, legacyID); err != nil {
		t.Fatalf("simulate legacy row: %v", err)
	}
	if _, err := q.client.SQL().Exec(ctx,
		`UPDATE _neutron_jobs SET status = 'running', attempts = 1, lease_expires_at = NULL,
		 worker_id = 'legacy-worker', claim_token = NULL WHERE id = $1`, deadID); err != nil {
		t.Fatalf("simulate exhausted legacy row: %v", err)
	}
	// A current, healthy claim: running with a live lease and a token.
	if _, err := q.client.SQL().Exec(ctx,
		`UPDATE _neutron_jobs SET status = 'running', attempts = 1,
		 lease_expires_at = $1, worker_id = 'live-worker', claim_token = 'tok-live' WHERE id = $2`,
		time.Now().Add(time.Hour), liveID); err != nil {
		t.Fatalf("simulate live claim: %v", err)
	}

	// The unmodified reaper leaves the NULL-lease rows exactly where they are.
	requeued, dead, err := q.Reap(ctx, jobType)
	if err != nil {
		t.Fatalf("reap before migration: %v", err)
	}
	if requeued != 0 || dead != 0 {
		t.Errorf("reap before migration moved %d/%d rows; NULL-lease rows should be untouched", requeued, dead)
	}
	if got := statusOf(t, q, ctx, legacyID); got != string(JobRunning) {
		t.Errorf("legacy row status = %q before migration, want running", got)
	}

	// The explicit cutover step marks exactly the NULL-lease running rows.
	n, err := q.RecoverLegacyRunning(ctx)
	if err != nil {
		t.Fatalf("recover legacy running: %v", err)
	}
	if n != 2 {
		t.Errorf("RecoverLegacyRunning marked %d rows, want 2", n)
	}

	// Normal recovery policy then applies: retry budget intact → requeued;
	// budget exhausted → dead-letter. The live claim is untouched.
	requeued, dead, err = q.Reap(ctx, jobType)
	if err != nil {
		t.Fatalf("reap after migration: %v", err)
	}
	if requeued != 1 || dead != 1 {
		t.Errorf("reap after migration: requeued=%d dead=%d, want 1/1", requeued, dead)
	}
	if got := statusOf(t, q, ctx, legacyID); got != string(JobPending) {
		t.Errorf("legacy row status = %q after recovery, want pending", got)
	}
	if got := stateOf(t, q, ctx, legacyID).attempts; got != 0 {
		t.Errorf("legacy row attempts = %d after recovery, want unchanged 0", got)
	}
	if got := statusOf(t, q, ctx, deadID); got != string(JobDeadLetter) {
		t.Errorf("exhausted legacy row status = %q after recovery, want dead_letter", got)
	}
	if got := statusOf(t, q, ctx, liveID); got != string(JobRunning) {
		t.Errorf("live claim status = %q after recovery, want running (untouched)", got)
	}
}
