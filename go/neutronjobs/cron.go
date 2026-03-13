package neutronjobs

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Schedule registers a recurring job. The cron expression supports:
//
//	"@every 5m"   - interval-based
//	"0 * * * *"   - standard 5-field cron (min hour dom month dow)
func (q *Queue) Schedule(ctx context.Context, cron string, jobType string, payload any) error {
	interval, err := parseCron(cron)
	if err != nil {
		return fmt.Errorf("neutronjobs: parse cron %q: %w", cron, err)
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, err := Enqueue(ctx, q, jobType, payload)
				if err != nil {
					q.logger.Error("schedule enqueue failed", "job_type", jobType, "error", err)
				}
			}
		}
	}()

	return nil
}

// parseCron handles simple cron-like expressions.
func parseCron(expr string) (time.Duration, error) {
	expr = strings.TrimSpace(expr)

	// @every syntax
	if strings.HasPrefix(expr, "@every ") {
		durStr := strings.TrimPrefix(expr, "@every ")
		return time.ParseDuration(durStr)
	}

	// Simple 5-field cron — only support "*/N" for minutes
	fields := strings.Fields(expr)
	if len(fields) == 5 {
		// Check for "*/N * * * *" (every N minutes)
		if strings.HasPrefix(fields[0], "*/") {
			n, err := strconv.Atoi(strings.TrimPrefix(fields[0], "*/"))
			if err == nil {
				return time.Duration(n) * time.Minute, nil
			}
		}
		// "0 * * * *" = every hour
		if fields[0] == "0" && fields[1] == "*" {
			return time.Hour, nil
		}
		// "0 0 * * *" = every day
		if fields[0] == "0" && fields[1] == "0" && fields[2] == "*" {
			return 24 * time.Hour, nil
		}
	}

	return 0, fmt.Errorf("unsupported cron expression: %s", expr)
}
