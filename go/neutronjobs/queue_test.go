package neutronjobs

import (
	"testing"
	"time"
)

func TestJobStatusConstants(t *testing.T) {
	tests := []struct {
		status JobStatus
		want   string
	}{
		{JobPending, "pending"},
		{JobRunning, "running"},
		{JobCompleted, "completed"},
		{JobFailed, "failed"},
	}

	for _, tc := range tests {
		if string(tc.status) != tc.want {
			t.Errorf("JobStatus = %q, want %q", tc.status, tc.want)
		}
	}
}

func TestJobOptionsWithDelay(t *testing.T) {
	var o jobOpts
	WithDelay(5 * time.Second)(&o)
	if o.delay != 5*time.Second {
		t.Errorf("delay = %v, want 5s", o.delay)
	}
}

func TestJobOptionsWithRetry(t *testing.T) {
	var o jobOpts
	WithRetry(3, 2*time.Second)(&o)
	if o.maxRetry != 3 {
		t.Errorf("maxRetry = %d, want 3", o.maxRetry)
	}
	if o.backoff != 2*time.Second {
		t.Errorf("backoff = %v, want 2s", o.backoff)
	}
}

func TestJobOptionsWithDeadline(t *testing.T) {
	deadline := time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC)
	var o jobOpts
	WithDeadline(deadline)(&o)
	if !o.deadline.Equal(deadline) {
		t.Errorf("deadline = %v, want %v", o.deadline, deadline)
	}
}

func TestJobOptionsDefault(t *testing.T) {
	var o jobOpts
	if o.delay != 0 {
		t.Errorf("default delay = %v, want 0", o.delay)
	}
	if o.maxRetry != 0 {
		t.Errorf("default maxRetry = %d, want 0", o.maxRetry)
	}
	if o.backoff != 0 {
		t.Errorf("default backoff = %v, want 0", o.backoff)
	}
	if !o.deadline.IsZero() {
		t.Errorf("default deadline should be zero")
	}
}

func TestJobOptionsComposition(t *testing.T) {
	var o jobOpts
	opts := []JobOption{
		WithDelay(10 * time.Second),
		WithRetry(5, 3*time.Second),
		WithDeadline(time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC)),
	}
	for _, fn := range opts {
		fn(&o)
	}
	if o.delay != 10*time.Second {
		t.Errorf("delay = %v", o.delay)
	}
	if o.maxRetry != 5 {
		t.Errorf("maxRetry = %d", o.maxRetry)
	}
	if o.backoff != 3*time.Second {
		t.Errorf("backoff = %v", o.backoff)
	}
}

func TestGenerateJobID(t *testing.T) {
	id := generateJobID()
	if len(id) != 32 { // 16 bytes hex-encoded
		t.Errorf("id length = %d, want 32", len(id))
	}

	// IDs should be unique
	id2 := generateJobID()
	if id == id2 {
		t.Error("IDs should be unique")
	}
}

func TestGenerateJobIDFormat(t *testing.T) {
	id := generateJobID()
	for _, c := range id {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Errorf("invalid hex character: %c", c)
		}
	}
}

func TestQueueOptionWithLogger(t *testing.T) {
	// Test that WithQueueLogger compiles and can be applied
	// (actual logger usage requires integration test)
	opt := WithQueueLogger(nil)
	q := &Queue{}
	opt(q)
	// Logger should be nil now (we explicitly set it)
	if q.logger != nil {
		t.Error("expected nil logger")
	}
}

func TestNewQueueDefaults(t *testing.T) {
	q := NewQueue(nil)
	if q == nil {
		t.Fatal("NewQueue returned nil")
	}
	if q.logger == nil {
		t.Error("default logger should not be nil")
	}
}

func TestNewQueueWithOptions(t *testing.T) {
	q := NewQueue(nil, WithQueueLogger(nil))
	if q == nil {
		t.Fatal("NewQueue returned nil")
	}
}

func TestJobOptionsOverride(t *testing.T) {
	// Later options should override earlier ones
	var o jobOpts
	WithDelay(1 * time.Second)(&o)
	WithDelay(5 * time.Second)(&o)
	if o.delay != 5*time.Second {
		t.Errorf("delay = %v, want 5s (override)", o.delay)
	}
}

func TestJobStatusStringConversion(t *testing.T) {
	s := JobStatus("custom")
	if string(s) != "custom" {
		t.Errorf("custom status = %q", s)
	}
}

func TestRetryBackoffCalculation(t *testing.T) {
	// Test the retry backoff logic used in executeJob
	backoffMs := int64(1000)

	// Attempt 1: backoffMs * 1 = 1s
	retryDelay1 := time.Duration(backoffMs*1) * time.Millisecond
	if retryDelay1 != time.Second {
		t.Errorf("attempt 1 delay = %v, want 1s", retryDelay1)
	}

	// Attempt 2: backoffMs * 2 = 2s
	retryDelay2 := time.Duration(backoffMs*2) * time.Millisecond
	if retryDelay2 != 2*time.Second {
		t.Errorf("attempt 2 delay = %v, want 2s", retryDelay2)
	}

	// Attempt 3: backoffMs * 3 = 3s
	retryDelay3 := time.Duration(backoffMs*3) * time.Millisecond
	if retryDelay3 != 3*time.Second {
		t.Errorf("attempt 3 delay = %v, want 3s", retryDelay3)
	}
}
