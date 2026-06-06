package neutronjobs

import (
	"context"
	"log/slog"
	"testing"
	"time"

	cron "github.com/robfig/cron/v3"
)

// Schedule accepts standard cron specs (including real field semantics that the
// old interval-only parser could not express) and rejects invalid ones.
func TestScheduleAcceptsStandardCron(t *testing.T) {
	q := &Queue{logger: slog.Default()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // cancels immediately so the scheduler goroutine exits before firing

	valid := []string{
		"*/5 * * * *", // every 5 minutes
		"30 9 * * 1",  // 09:30 every Monday — impossible with the old parser
		"0 0 1 * *",   // midnight on the 1st of each month
		"@every 1h",
		"@daily",
	}
	for _, spec := range valid {
		if err := q.Schedule(ctx, spec, "job", nil); err != nil {
			t.Errorf("Schedule(%q) returned error: %v", spec, err)
		}
	}

	if err := q.Schedule(ctx, "not a cron", "job", nil); err == nil {
		t.Error("expected an error for an invalid cron expression")
	}
}

// The standard 5-field semantics fire at the correct wall-clock time, not on a
// fixed interval — the core defect the old parser had.
func TestCronStandardFieldSemantics(t *testing.T) {
	schedule, err := cron.ParseStandard("30 9 * * 1") // 09:30 on Mondays
	if err != nil {
		t.Fatalf("ParseStandard: %v", err)
	}
	// From a Wednesday, the next activation must be the following Monday at 09:30.
	from := time.Date(2026, time.June, 3, 12, 0, 0, 0, time.UTC) // Wed
	next := schedule.Next(from)
	if next.Weekday() != time.Monday {
		t.Errorf("next.Weekday() = %v, want Monday", next.Weekday())
	}
	if next.Hour() != 9 || next.Minute() != 30 {
		t.Errorf("next time = %02d:%02d, want 09:30", next.Hour(), next.Minute())
	}
}
