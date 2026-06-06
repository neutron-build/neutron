package neutronjobs

import (
	"context"
	"fmt"
	"time"

	cron "github.com/robfig/cron/v3"
)

// Schedule registers a recurring job. The expression is a standard cron spec
// parsed by robfig/cron, supporting:
//
//	"*/5 * * * *"  - every 5 minutes
//	"30 9 * * 1"   - 09:30 every Monday (real field semantics, not an interval)
//	"@every 5m"    - interval-based
//	"@daily", "@hourly", "@weekly", "@monthly" - descriptors
//
// Unlike the previous implementation (which mapped every expression to a fixed
// interval and could only handle a few patterns), this fires at the correct
// next activation time computed from the schedule.
func (q *Queue) Schedule(ctx context.Context, spec string, jobType string, payload any) error {
	schedule, err := cron.ParseStandard(spec)
	if err != nil {
		return fmt.Errorf("neutronjobs: parse cron %q: %w", spec, err)
	}

	go func() {
		for {
			now := time.Now()
			next := schedule.Next(now)
			if next.IsZero() {
				// No future activation (shouldn't happen for valid specs).
				return
			}
			timer := time.NewTimer(next.Sub(now))
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
				if _, err := Enqueue(ctx, q, jobType, payload); err != nil {
					q.logger.Error("schedule enqueue failed", "job_type", jobType, "error", err)
				}
			}
		}
	}()

	return nil
}
