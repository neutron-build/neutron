package neutronjobs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/neutron-dev/neutron-go/nucleus"
)

// Queue provides a persistent job queue backed by Nucleus/PostgreSQL.
type Queue struct {
	client *nucleus.Client
	logger *slog.Logger
}

// NewQueue creates a new job queue.
func NewQueue(client *nucleus.Client, opts ...QueueOption) *Queue {
	q := &Queue{
		client: client,
		logger: slog.Default(),
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
	JobPending   JobStatus = "pending"
	JobRunning   JobStatus = "running"
	JobCompleted JobStatus = "completed"
	JobFailed    JobStatus = "failed"
)

// EnsureSchema creates the jobs table if it doesn't exist.
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
		error TEXT
	)`
	_, err := q.client.SQL().Exec(ctx, sql)
	return err
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
// context is cancelled.
func (q *Queue) Process(ctx context.Context, jobType string, handler func(ctx context.Context, payload []byte) error, concurrency int) error {
	if concurrency < 1 {
		concurrency = 1
	}

	sem := make(chan struct{}, concurrency)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Fetch next job using advisory lock
		sql := `UPDATE _neutron_jobs
			SET status = 'running', attempts = attempts + 1, updated_at = NOW()
			WHERE id = (
				SELECT id FROM _neutron_jobs
				WHERE job_type = $1 AND status = 'pending' AND run_at <= NOW()
				AND (deadline IS NULL OR deadline > NOW())
				ORDER BY run_at
				LIMIT 1
				FOR UPDATE SKIP LOCKED
			)
			RETURNING id, payload, attempts, max_retry, backoff_ms`

		rows, err := q.client.Pool().Query(ctx, sql, jobType)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}

		var found bool
		for rows.Next() {
			found = true
			var id string
			var payload []byte
			var attempts, maxRetry int
			var backoffMs int64
			if err := rows.Scan(&id, &payload, &attempts, &maxRetry, &backoffMs); err != nil {
				q.logger.Error("scan job", "error", err)
				continue
			}

			sem <- struct{}{}
			go func(id string, payload []byte, attempts, maxRetry int, backoffMs int64) {
				defer func() { <-sem }()
				q.executeJob(ctx, id, payload, attempts, maxRetry, backoffMs, handler)
			}(id, payload, attempts, maxRetry, backoffMs)
		}
		rows.Close()

		if !found {
			// No jobs available, poll interval
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
		}
	}
}

func (q *Queue) executeJob(ctx context.Context, id string, payload []byte, attempts, maxRetry int, backoffMs int64, handler func(context.Context, []byte) error) {
	err := handler(ctx, payload)
	if err == nil {
		q.client.SQL().Exec(ctx,
			"UPDATE _neutron_jobs SET status = 'completed', updated_at = NOW() WHERE id = $1", id)
		return
	}

	q.logger.Error("job failed", "id", id, "attempt", attempts, "error", err)

	if attempts < maxRetry {
		// Schedule retry with backoff
		retryAt := time.Now().Add(time.Duration(backoffMs*int64(attempts)) * time.Millisecond)
		q.client.SQL().Exec(ctx,
			"UPDATE _neutron_jobs SET status = 'pending', run_at = $1, error = $2, updated_at = NOW() WHERE id = $3",
			retryAt, err.Error(), id)
	} else {
		q.client.SQL().Exec(ctx,
			"UPDATE _neutron_jobs SET status = 'failed', error = $1, updated_at = NOW() WHERE id = $2",
			err.Error(), id)
	}
}

func generateJobID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
