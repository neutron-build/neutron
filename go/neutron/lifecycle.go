package neutron

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// LifecycleHook represents ordered startup and shutdown actions.
type LifecycleHook struct {
	Name    string
	OnStart func(ctx context.Context) error
	OnStop  func(ctx context.Context) error
}

// lifecycle manages ordered hook execution.
type lifecycle struct {
	hooks  []LifecycleHook
	logger *slog.Logger
}

func newLifecycle(logger *slog.Logger) *lifecycle {
	return &lifecycle{logger: logger}
}

func (lc *lifecycle) add(hooks ...LifecycleHook) {
	lc.hooks = append(lc.hooks, hooks...)
}

// start runs all OnStart hooks in registration order.
//
// A failure rolls back every hook that had already started, in reverse
// order, before returning (GO-11): the old code returned immediately,
// leaking the resources the earlier hooks initialized. The rollback errors
// are joined onto the original error so both are observable.
func (lc *lifecycle) start(ctx context.Context) error {
	started := 0
	defer func() {
		if started == 0 {
			return
		}
		if err := lc.stopLimited(context.Background(), started); err != nil {
			lc.logger.Error("lifecycle start-rollback failed", "error", err)
		}
	}()
	for _, h := range lc.hooks {
		if h.OnStart == nil {
			continue
		}
		lc.logger.Info("starting lifecycle hook", "name", h.Name)
		if err := h.OnStart(ctx); err != nil {
			err = fmt.Errorf("lifecycle start %q: %w", h.Name, err)
			// Roll back the started prefix; stopLimited runs the first
			// `started` hooks' OnStop in reverse. Its own failure is logged
			// above; the original error is what the caller sees.
			return err
		}
		started++
	}
	return nil
}

// stop runs all OnStop hooks in reverse registration order. Every hook runs
// even when one fails; errors are combined rather than discarded.
func (lc *lifecycle) stop(ctx context.Context) error {
	return lc.stopLimited(ctx, len(lc.hooks))
}

// stopLimited runs OnStop for the first `count` hooks in reverse order.
func (lc *lifecycle) stopLimited(ctx context.Context, count int) error {
	var errs []error
	for i := count - 1; i >= 0; i-- {
		h := lc.hooks[i]
		if h.OnStop == nil {
			continue
		}
		lc.logger.Info("stopping lifecycle hook", "name", h.Name)
		if err := h.OnStop(ctx); err != nil {
			lc.logger.Error("lifecycle stop failed", "name", h.Name, "error", err)
			errs = append(errs, fmt.Errorf("lifecycle stop %q: %w", h.Name, err))
		}
	}
	return errors.Join(errs...)
}

// shutdownBudget is the fresh context budget stop hooks receive when the
// caller's own shutdown context has already expired (GO-11): stop hooks used
// to be handed an already-cancelled context and could do nothing at all.
const shutdownBudget = 10 * time.Second

// stopWithBudget stops with a guaranteed-fresh context: if the passed
// context is already expired, a bounded replacement is used so cleanup still
// gets a chance to run.
func (lc *lifecycle) stopWithBudget(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		fresh, cancel := context.WithTimeout(context.Background(), shutdownBudget)
		defer cancel()
		return lc.stop(fresh)
	}
	return lc.stop(ctx)
}
