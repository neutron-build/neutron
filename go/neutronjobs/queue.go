package neutronjobs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/neutron-dev/neutron-go/nucleus"
)

// Defaults for the lease machinery. A claim is only valid for `defaultLease`;
// past that the reaper assumes the worker died and puts the job back. Running
// jobs extend their own lease, so the lease bounds how long a *dead* worker can
// strand a job — not how long a job may legitimately take.
const (
	defaultLease          = 30 * time.Second
	defaultReaperInterval = 15 * time.Second

	// A lease is only considered expired once it is this far past due. Lease
	// deadlines are written by one worker and read by another, so the margin
	// absorbs ordinary clock skew between application servers. Reaping a job
	// whose worker is alive runs it twice, so this errs late deliberately.
	defaultReapGrace = 10 * time.Second

	// How long a terminal write gets after the job itself is done. This runs on
	// a context detached from shutdown, so it needs its own bound or a hung
	// database would block shutdown forever.
	terminalWriteTimeout = 10 * time.Second
)

// claimJobSQL claims one pending job and takes a lease on it.
//
// Correctness comes from the `status = 'pending'` predicate on the UPDATE
// itself, not from row locking: two workers may select the same id, only one
// UPDATE matches, and the loser affects zero rows and polls again. That holds
// on any backend.
//
// It deliberately omits `FOR UPDATE SKIP LOCKED`. The clause reduces contention
// but cannot carry correctness here, because it is not universally implemented
// — Nucleus, this SDK's own database, parsed and silently discarded it, so a
// claim depending on it would have handed one job to every worker polling at
// that moment while looking entirely correct.
//
// The cost is contention: with many workers, losers burn a round trip. The
// answer to that is batching or a backend-specific fast path, never borrowing a
// correctness guarantee from an optional clause.
//
// `lease_expires_at` is passed as a timestamp the caller computed, never as an
// interval string. Formatting a Go duration into SQL looks obvious and is a
// trap: `time.Duration.String()` renders sub-millisecond values with the micro
// sign (`500µs`), and PostgreSQL's interval parser only accepts `us`, so any
// lease under a millisecond would error on every single claim. Passing an
// instant removes the whole class rather than escaping one instance of it.
const claimJobSQL = `UPDATE _neutron_jobs
			SET status = 'running', attempts = attempts + 1, updated_at = NOW(),
			    lease_expires_at = $2, worker_id = $3
			WHERE id = (
				SELECT id FROM _neutron_jobs
				WHERE job_type = $1 AND status = 'pending' AND run_at <= NOW()
				AND (deadline IS NULL OR deadline > NOW())
				ORDER BY run_at
				LIMIT 1
			)
			AND status = 'pending'
			RETURNING id, payload, attempts, max_retry, backoff_ms`

// Queue provides a persistent job queue backed by Nucleus/PostgreSQL.
type Queue struct {
	client         *nucleus.Client
	logger         *slog.Logger
	lease          time.Duration
	reaperInterval time.Duration
	reapGrace      time.Duration
	disableReaper  bool
	workerID       string
}

// NewQueue creates a new job queue.
func NewQueue(client *nucleus.Client, opts ...QueueOption) *Queue {
	q := &Queue{
		client:         client,
		logger:         slog.Default(),
		lease:          defaultLease,
		reaperInterval: defaultReaperInterval,
		reapGrace:      defaultReapGrace,
		workerID:       defaultWorkerID(),
	}
	for _, o := range opts {
		o(q)
	}
	return q
}

// QueueOption configures the queue.
type QueueOption func(*Queue)

// WithQueueLogger sets the logger for the queue.
func WithQueueLogger(l *slog.Logger) QueueOption {
	return func(q *Queue) { q.logger = l }
}

// WithLease sets how long a claim stays valid before the reaper treats the
// worker as dead. Running jobs renew their own lease, so this does not cap job
// duration — it caps how long a crashed worker can strand a job.
func WithLease(d time.Duration) QueueOption {
	return func(q *Queue) {
		if d > 0 {
			q.lease = d
		}
	}
}

// WithReaperInterval sets how often expired leases are swept.
func WithReaperInterval(d time.Duration) QueueOption {
	return func(q *Queue) {
		if d > 0 {
			q.reaperInterval = d
		}
	}
}

// WithoutReaper disables the automatic reaper started by Process. Jobs held by
// a worker that dies then stay 'running' until something else calls Reap.
func WithoutReaper() QueueOption {
	return func(q *Queue) { q.disableReaper = true }
}

// WithWorkerID overrides the identifier recorded against claimed jobs. Defaults
// to hostname plus a random suffix, which is what makes a stuck job traceable
// back to the process that was holding it.
func WithWorkerID(id string) QueueOption {
	return func(q *Queue) {
		if id != "" {
			q.workerID = id
		}
	}
}

// JobOption configures individual jobs.
type JobOption func(*jobOpts)

type jobOpts struct {
	delay    time.Duration
	maxRetry int
	backoff  time.Duration
	deadline time.Time
}

// WithDelay delays job execution.
func WithDelay(d time.Duration) JobOption {
	return func(o *jobOpts) { o.delay = d }
}

// WithRetry sets retry count and backoff duration.
func WithRetry(max int, backoff time.Duration) JobOption {
	return func(o *jobOpts) { o.maxRetry = max; o.backoff = backoff }
}

// WithDeadline sets a deadline for the job.
func WithDeadline(t time.Time) JobOption {
	return func(o *jobOpts) { o.deadline = t }
}

// JobStatus represents the current state of a job.
type JobStatus string

const (
	JobPending JobStatus = "pending"
	JobRunning JobStatus = "running"

	JobCompleted JobStatus = "completed"
	JobFailed    JobStatus = "failed"

	// JobDeadLetter is terminal and means the job stopped being retried without
	// ever reporting a failure of its own: its worker kept dying, or it kept
	// panicking, until the attempt budget ran out. Without it a job that kills
	// whatever picks it up is requeued forever, and the reaper turns one bad
	// payload into an unbounded crash loop.
	JobDeadLetter JobStatus = "dead_letter"
)

// EnsureSchema creates the jobs table if it doesn't exist, and brings an
// existing table up to the current shape.
//
// The ALTERs are what make this safe to deploy over a queue that predates
// leases. `CREATE TABLE IF NOT EXISTS` alone silently does nothing when the
// table is already there, so an upgraded application would claim jobs writing
// to columns that do not exist and fail on every claim.
func (q *Queue) EnsureSchema(ctx context.Context) error {
	sql := `CREATE TABLE IF NOT EXISTS _neutron_jobs (
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
	if _, err := q.client.SQL().Exec(ctx, sql); err != nil {
		return err
	}

	for _, alter := range []string{
		`ALTER TABLE _neutron_jobs ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ`,
		`ALTER TABLE _neutron_jobs ADD COLUMN IF NOT EXISTS worker_id TEXT`,
	} {
		if _, err := q.client.SQL().Exec(ctx, alter); err != nil {
			return fmt.Errorf("neutronjobs: migrate schema: %w", err)
		}
	}
	return nil
}

// Enqueue adds a job to the queue.
func Enqueue[T any](ctx context.Context, q *Queue, jobType string, payload T, opts ...JobOption) (string, error) {
	var o jobOpts
	for _, fn := range opts {
		fn(&o)
	}

	id := generateJobID()
	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("neutronjobs: marshal payload: %w", err)
	}

	runAt := time.Now().Add(o.delay)

	sql := `INSERT INTO _neutron_jobs (id, job_type, payload, max_retry, backoff_ms, run_at, deadline)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`

	var deadline *time.Time
	if !o.deadline.IsZero() {
		deadline = &o.deadline
	}

	_, err = q.client.SQL().Exec(ctx, sql,
		id, jobType, string(data), o.maxRetry,
		o.backoff.Milliseconds(), runAt, deadline)
	if err != nil {
		return "", fmt.Errorf("neutronjobs: enqueue: %w", err)
	}

	return id, nil
}

// Process starts processing jobs of the given type. It blocks until the
// context is cancelled, then waits for in-flight jobs to finish before
// returning.
//
// Draining is not politeness. A job whose handler has already done its work is
// only recorded as complete by the terminal write that follows it; returning
// the moment the context is cancelled abandons that write, leaves the row
// 'running', and the reaper then hands the same job to another worker. The
// effect is that every deploy silently re-runs whatever was in flight.
func (q *Queue) Process(ctx context.Context, jobType string, handler func(ctx context.Context, payload []byte) error, concurrency int) error {
	if concurrency < 1 {
		concurrency = 1
	}

	if !q.disableReaper {
		reaperDone := make(chan struct{})
		go func() {
			defer close(reaperDone)
			q.runReaper(ctx, jobType)
		}()
		defer func() { <-reaperDone }()
	}

	sem := make(chan struct{}, concurrency)
	var inflight sync.WaitGroup
	defer inflight.Wait()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rows, err := q.client.Pool().Query(ctx, claimJobSQL, jobType, time.Now().Add(q.lease), q.workerID)
		if err != nil {
			// Log it. Swallowed, a schema that was never migrated or a
			// permissions problem stops the worker processing anything at all
			// while emitting nothing — the queue looks idle rather than broken,
			// which is the hardest failure to notice.
			//
			// Cancellation is the ordinary way to stop a worker, though, and
			// logging that at error level fires on every clean shutdown. An
			// error that appears in normal operation is one people learn to
			// scroll past, which costs more than it reports.
			if ctx.Err() == nil {
				q.logger.Error("job claim query failed", "job_type", jobType, "err", err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
			continue
		}

		// Drain the claim into memory before doing anything that can block.
		// Acquiring the concurrency semaphore inside the row loop holds an open
		// result set — and so a pooled connection — for as long as every worker
		// slot is busy, which starves the pool the queue itself needs to make
		// progress.
		type claimed struct {
			id                 string
			payload            []byte
			attempts, maxRetry int
			backoffMs          int64
		}
		var batch []claimed
		for rows.Next() {
			var c claimed
			if err := rows.Scan(&c.id, &c.payload, &c.attempts, &c.maxRetry, &c.backoffMs); err != nil {
				q.logger.Error("scan job", "error", err)
				continue
			}
			batch = append(batch, c)
		}
		rowsErr := rows.Err()
		rows.Close()
		if rowsErr != nil && ctx.Err() == nil {
			q.logger.Error("job claim read failed", "job_type", jobType, "err", rowsErr)
		}

		for _, c := range batch {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				// The claim already happened, so the lease is what recovers
				// this one: leave it running and let the reaper requeue it.
				inflight.Wait()
				return ctx.Err()
			}
			inflight.Add(1)
			go func(c claimed) {
				defer inflight.Done()
				defer func() { <-sem }()
				q.executeJob(ctx, c.id, c.payload, c.attempts, c.maxRetry, c.backoffMs, handler)
			}(c)
		}

		if len(batch) == 0 {
			// Nothing claimed — poll interval. This also covers a row that
			// failed to scan, which keeps an unreadable row from spinning the
			// loop at full speed.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
		}
	}
}

// executeJob runs one claimed job, holding its lease for the duration.
func (q *Queue) executeJob(ctx context.Context, id string, payload []byte, attempts, maxRetry int, backoffMs int64, handler func(context.Context, []byte) error) {
	// The job's own context. It is cancelled when the parent is cancelled, and
	// also the moment this worker loses the lease — at that point another
	// worker is entitled to the job, and continuing would run it twice.
	jobCtx, cancelJob := context.WithCancel(ctx)
	defer cancelJob()

	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		q.heartbeat(jobCtx, id, cancelJob)
	}()

	err := q.runHandler(jobCtx, payload, handler)

	cancelJob()
	<-heartbeatDone

	// Terminal writes must outlive the shutdown that may have just cancelled
	// ctx. Writing them on the cancelled context is what converts "we lost the
	// completion record" into "the job runs a second time": the work already
	// happened, so the only thing that stops a duplicate is recording it.
	writeCtx, cancelWrite := context.WithTimeout(context.WithoutCancel(ctx), terminalWriteTimeout)
	defer cancelWrite()

	if err == nil {
		if _, uerr := q.client.SQL().Exec(writeCtx,
			"UPDATE _neutron_jobs SET status = 'completed', updated_at = NOW(), lease_expires_at = NULL, worker_id = NULL WHERE id = $1", id); uerr != nil {
			// The job did its work and we could not record it. The reaper will
			// requeue it once the lease lapses, so this is an at-least-once
			// delivery event and worth shouting about.
			q.logger.Error("job completed but status write failed; job may run again",
				"id", id, "error", uerr)
		}
		return
	}

	// A handler that returned because the process is shutting down has not
	// failed. Counting it as a failure burns a retry and can dead-letter a
	// perfectly good job across a few deploys, so hand it straight back.
	if ctx.Err() != nil {
		if _, uerr := q.client.SQL().Exec(writeCtx,
			`UPDATE _neutron_jobs SET status = 'pending', attempts = attempts - 1, run_at = NOW(),
			 updated_at = NOW(), lease_expires_at = NULL, worker_id = NULL WHERE id = $1`, id); uerr != nil {
			q.logger.Error("job interrupted and release failed; lease will recover it",
				"id", id, "error", uerr)
		} else {
			q.logger.Info("job interrupted by shutdown, returned to queue", "id", id)
		}
		return
	}

	q.logger.Error("job failed", "id", id, "attempt", attempts, "error", err)

	if attempts < maxRetry {
		retryAt := time.Now().Add(time.Duration(backoffMs*int64(attempts)) * time.Millisecond)
		if _, uerr := q.client.SQL().Exec(writeCtx,
			`UPDATE _neutron_jobs SET status = 'pending', run_at = $1, error = $2, updated_at = NOW(),
			 lease_expires_at = NULL, worker_id = NULL WHERE id = $3`,
			retryAt, err.Error(), id); uerr != nil {
			q.logger.Error("job status update failed", "id", id, "target", "retry", "error", uerr)
		}
		return
	}

	if _, uerr := q.client.SQL().Exec(writeCtx,
		`UPDATE _neutron_jobs SET status = 'failed', error = $1, updated_at = NOW(),
		 lease_expires_at = NULL, worker_id = NULL WHERE id = $2`,
		err.Error(), id); uerr != nil {
		q.logger.Error("job status update failed", "id", id, "target", "failed", "error", uerr)
	}
}

// runHandler invokes the handler and converts a panic into an ordinary error.
//
// Without this a single malformed payload takes the whole worker process down
// with it. That job is still 'running', so the reaper hands it to the next
// worker, which also dies — one bad row becomes a fleet-wide crash loop that
// looks like an outage rather than a bad job. Converted to an error it consumes
// its retries like any other failure and ends up dead-lettered.
func (q *Queue) runHandler(ctx context.Context, payload []byte, handler func(context.Context, []byte) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("neutronjobs: handler panicked: %v", r)
		}
	}()
	return handler(ctx, payload)
}

// heartbeat extends the job's lease while it runs, and cancels the job if the
// lease is lost.
//
// A fixed lease alone forces an impossible choice: short enough to recover a
// dead worker quickly, but longer than the slowest job or a healthy worker gets
// its job stolen mid-flight. Renewing removes the second half of that — the
// lease only has to outlive a renewal interval, not the job.
func (q *Queue) heartbeat(ctx context.Context, id string, cancelJob context.CancelFunc) {
	interval := max(q.lease/3, time.Second)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		// Renew only while we still own it. `worker_id` in the predicate is the
		// whole point: if the reaper decided we were dead and requeued the job,
		// this matches zero rows and we must stop working on it.
		n, err := q.client.SQL().Exec(ctx,
			`UPDATE _neutron_jobs SET lease_expires_at = $1, updated_at = NOW()
			 WHERE id = $2 AND worker_id = $3 AND status = 'running'`,
			time.Now().Add(q.lease), id, q.workerID)
		if err != nil {
			// A failed renewal is not proof the lease was lost, so keep working
			// and try again; the lease itself is the backstop.
			if ctx.Err() == nil {
				q.logger.Warn("lease renewal failed", "id", id, "error", err)
			}
			continue
		}
		if n == 0 {
			q.logger.Warn("lease lost, abandoning job to avoid running it twice",
				"id", id, "worker_id", q.workerID)
			cancelJob()
			return
		}
	}
}

// runReaper sweeps expired leases until the context is cancelled.
func (q *Queue) runReaper(ctx context.Context, jobType string) {
	ticker := time.NewTicker(q.reaperInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if requeued, dead, err := q.Reap(ctx, jobType); err != nil {
			if ctx.Err() == nil {
				q.logger.Error("reaper sweep failed", "job_type", jobType, "error", err)
			}
		} else if requeued > 0 || dead > 0 {
			q.logger.Info("reaped expired job leases",
				"job_type", jobType, "requeued", requeued, "dead_lettered", dead)
		}
	}
}

// Reap returns jobs whose lease has expired to the queue, and dead-letters the
// ones that have exhausted their attempts. It reports how many of each.
//
// This is exported so an application can sweep on its own schedule, or sweep a
// job type that currently has no workers running.
func (q *Queue) Reap(ctx context.Context, jobType string) (requeued, deadLettered int64, err error) {
	// Both halves compare against a cutoff computed here rather than the
	// database's NOW(). Lease deadlines are written by workers, so keeping both
	// sides of the comparison on application clocks avoids introducing a second
	// source of skew between the application and the database.
	cutoff := time.Now().Add(-q.reapGrace)

	// Exhausted first. Doing it the other way round would requeue a job and
	// then immediately dead-letter it in the same sweep.
	deadLettered, err = q.client.SQL().Exec(ctx,
		`UPDATE _neutron_jobs
		 SET status = 'dead_letter', updated_at = NOW(), lease_expires_at = NULL, worker_id = NULL,
		     error = 'lease expired without completion; attempts exhausted'
		 WHERE job_type = $1 AND status = 'running'
		   AND lease_expires_at IS NOT NULL AND lease_expires_at < $2
		   AND attempts >= max_retry`,
		jobType, cutoff)
	if err != nil {
		return 0, 0, fmt.Errorf("neutronjobs: dead-letter expired jobs: %w", err)
	}

	requeued, err = q.client.SQL().Exec(ctx,
		`UPDATE _neutron_jobs
		 SET status = 'pending', run_at = NOW(), updated_at = NOW(),
		     lease_expires_at = NULL, worker_id = NULL,
		     error = 'lease expired without completion; requeued'
		 WHERE job_type = $1 AND status = 'running'
		   AND lease_expires_at IS NOT NULL AND lease_expires_at < $2
		   AND attempts < max_retry`,
		jobType, cutoff)
	if err != nil {
		return 0, deadLettered, fmt.Errorf("neutronjobs: requeue expired jobs: %w", err)
	}

	return requeued, deadLettered, nil
}

func generateJobID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// defaultWorkerID identifies this process well enough to trace a stuck job back
// to it. The random suffix distinguishes several workers on one host.
func defaultWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return host + "-" + hex.EncodeToString(b)
}
