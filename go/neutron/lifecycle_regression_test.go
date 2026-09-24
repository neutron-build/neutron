package neutron

import (
	"context"
	"io"
	"log/slog"
	"testing"
)

// Regression for the rollback-on-success defect: a fully successful start
// must leave every hook RUNNING (found live by teploy-observe's O11 e2e —
// the booted binary served healthz 503 with a closed pool because the
// deferred rollback ran on the success path).
func TestLifecycleSuccessfulStartRunsNoRollback(t *testing.T) {
	var running int
	hooks := []LifecycleHook{
		{Name: "a", OnStart: func(context.Context) error { running++; return nil }, OnStop: func(context.Context) error { running--; return nil }},
		{Name: "b", OnStart: func(context.Context) error { running++; return nil }, OnStop: func(context.Context) error { running--; return nil }},
	}
	lc := newLifecycle(slog.New(slog.NewTextHandler(io.Discard, nil)))
	lc.add(hooks...)
	if err := lc.start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	if running != 2 {
		t.Fatalf("after a successful start, %d/2 hooks running — the success-path rollback fired", running)
	}
	// A FAILED start still rolls back the started prefix (GO-11, unchanged).
	fail := []LifecycleHook{
		{Name: "a", OnStart: func(context.Context) error { running++; return nil }, OnStop: func(context.Context) error { running--; return nil }},
		{Name: "b", OnStart: func(context.Context) error { return errBoom }, OnStop: func(context.Context) error { return nil }},
	}
	lc2 := newLifecycle(slog.New(slog.NewTextHandler(io.Discard, nil)))
	lc2.add(fail...)
	running = 0
	_ = lc2.start(context.Background())
	if running != 0 {
		t.Fatalf("failed start must roll back its prefix, %d still running", running)
	}
}

var errBoom = boomError{}

type boomError struct{}

func (boomError) Error() string { return "boom" }
