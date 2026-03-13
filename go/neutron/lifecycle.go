package neutron

import (
	"context"
	"fmt"
	"log/slog"
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
func (lc *lifecycle) start(ctx context.Context) error {
	for _, h := range lc.hooks {
		if h.OnStart == nil {
			continue
		}
		lc.logger.Info("starting lifecycle hook", "name", h.Name)
		if err := h.OnStart(ctx); err != nil {
			return fmt.Errorf("lifecycle start %q: %w", h.Name, err)
		}
	}
	return nil
}

// stop runs all OnStop hooks in reverse registration order.
func (lc *lifecycle) stop(ctx context.Context) error {
	var firstErr error
	for i := len(lc.hooks) - 1; i >= 0; i-- {
		h := lc.hooks[i]
		if h.OnStop == nil {
			continue
		}
		lc.logger.Info("stopping lifecycle hook", "name", h.Name)
		if err := h.OnStop(ctx); err != nil {
			lc.logger.Error("lifecycle stop failed", "name", h.Name, "error", err)
			if firstErr == nil {
				firstErr = fmt.Errorf("lifecycle stop %q: %w", h.Name, err)
			}
		}
	}
	return firstErr
}
