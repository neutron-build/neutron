package gmail

import (
	"context"
	"errors"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/neutron-build/neutron/mail"
	"google.golang.org/api/googleapi"
)

const (
	// A conservative local ceiling, not a reservation of provider quota.
	// All read methods share it, including optional body and attachment fetches.
	readSpacing     = 250 * time.Millisecond
	readAttempts    = 3
	maxReadBackoff  = 30 * time.Second
	initialPageSize = 100
)

// ReadLimiter spaces reads without a burst allowance. It can be shared by
// adapters for the same account; waiting and lock acquisition are cancellable.
// A zero value is not usable: construct it with NewReadLimiter.
type ReadLimiter struct {
	gate     chan struct{}
	mu       sync.Mutex
	next     time.Time
	cooldown time.Time
	now      func() time.Time
	wait     func(context.Context, time.Duration) error
}

func NewReadLimiter() *ReadLimiter {
	return &ReadLimiter{gate: make(chan struct{}, 1), now: time.Now, wait: waitRead}
}

func waitRead(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

// Wait admits one provider read. A cancelled waiter consumes no future slot.
func (l *ReadLimiter) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case l.gate <- struct{}{}:
	}
	defer func() { <-l.gate }()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		l.mu.Lock()
		now := l.now()
		// Preserve a long provider cooldown without holding a sync worker for
		// minutes or hours. Later attempts are rejected locally until it is near.
		if l.cooldown.Sub(now) > maxReadBackoff {
			l.mu.Unlock()
			return mail.ErrRateLimited
		}
		next := l.next
		if l.cooldown.After(next) {
			next = l.cooldown
		}
		delay := next.Sub(now)
		if delay <= 0 {
			l.next = now.Add(readSpacing)
			l.mu.Unlock()
			return nil
		}
		l.mu.Unlock()
		if err := l.wait(ctx, delay); err != nil {
			return err
		}
		// A different adapter may have observed a new cooldown while we waited.
		// Recheck it before consuming a slot or touching the provider.
	}
}

func (l *ReadLimiter) pause(delay time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	until := l.now().Add(delay)
	if until.After(l.cooldown) {
		l.cooldown = until
	}
}

// readCall retries only explicitly throttled reads. Retrying a mutation or
// restarting a whole page here could duplicate effects or discard progress.
// A page remains all-or-error; only its failing individual read is retried.
func readCall[T any](ctx context.Context, a *Adapter, call func() (*T, error)) (*T, error) {
	var err error
	for attempt := 0; attempt < readAttempts; attempt++ {
		if waitErr := a.reads.Wait(ctx); waitErr != nil {
			return nil, waitErr
		}
		result, readErr := call()
		if readErr == nil || !errors.Is(classify(readErr), mail.ErrRateLimited) {
			return result, readErr
		}
		err = readErr
		// Publish every throttle, including the last permitted attempt, so another
		// adapter or later scheduler run cannot ignore the provider's cooldown.
		delay := (time.Second << uint(attempt)) + a.retryJitter()
		var apiErr *googleapi.Error
		if errors.As(err, &apiErr) {
			if retryAfter, ok := readRetryAfter(apiErr.Header.Get("Retry-After"), a.reads.now()); ok && retryAfter > delay {
				delay = retryAfter
			}
		}
		a.reads.pause(delay)
		if delay > maxReadBackoff {
			return nil, err
		}
	}
	return nil, err
}

func readRetryAfter(raw string, now time.Time) (time.Duration, bool) {
	if raw == "" {
		return 0, false
	}
	if seconds, err := strconv.ParseUint(raw, 10, 64); err == nil {
		if seconds > uint64((time.Duration(1<<63-1))/time.Second) {
			return time.Duration(1<<63 - 1), true
		}
		return time.Duration(seconds) * time.Second, true
	} else if errors.Is(err, strconv.ErrRange) {
		return time.Duration(1<<63 - 1), true
	}
	date, err := http.ParseTime(raw)
	if err != nil {
		return 0, false
	}
	return date.Sub(now), true
}

func readJitter() time.Duration { return time.Duration(rand.IntN(250)) * time.Millisecond }
