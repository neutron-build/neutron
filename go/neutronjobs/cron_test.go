package neutronjobs

import (
	"testing"
	"time"
)

func TestParseCronEvery(t *testing.T) {
	d, err := parseCron("@every 5m")
	if err != nil {
		t.Fatalf("parseCron: %v", err)
	}
	if d != 5*time.Minute {
		t.Errorf("duration = %v, want 5m", d)
	}
}

func TestParseCronEveryHour(t *testing.T) {
	d, err := parseCron("@every 1h")
	if err != nil {
		t.Fatalf("parseCron: %v", err)
	}
	if d != time.Hour {
		t.Errorf("duration = %v, want 1h", d)
	}
}

func TestParseCronStarSlash(t *testing.T) {
	d, err := parseCron("*/15 * * * *")
	if err != nil {
		t.Fatalf("parseCron: %v", err)
	}
	if d != 15*time.Minute {
		t.Errorf("duration = %v, want 15m", d)
	}
}

func TestParseCronHourly(t *testing.T) {
	d, err := parseCron("0 * * * *")
	if err != nil {
		t.Fatalf("parseCron: %v", err)
	}
	if d != time.Hour {
		t.Errorf("duration = %v, want 1h", d)
	}
}

func TestParseCronDaily(t *testing.T) {
	d, err := parseCron("0 0 * * *")
	if err != nil {
		t.Fatalf("parseCron: %v", err)
	}
	if d != 24*time.Hour {
		t.Errorf("duration = %v, want 24h", d)
	}
}

func TestParseCronInvalid(t *testing.T) {
	_, err := parseCron("not-a-cron")
	if err == nil {
		t.Fatal("expected error for invalid cron")
	}
}

func TestJobOptions(t *testing.T) {
	var o jobOpts
	WithDelay(5 * time.Second)(&o)
	WithRetry(3, time.Second)(&o)
	deadline := time.Now().Add(time.Hour)
	WithDeadline(deadline)(&o)

	if o.delay != 5*time.Second {
		t.Errorf("delay = %v", o.delay)
	}
	if o.maxRetry != 3 {
		t.Errorf("maxRetry = %d", o.maxRetry)
	}
	if o.backoff != time.Second {
		t.Errorf("backoff = %v", o.backoff)
	}
	if !o.deadline.Equal(deadline) {
		t.Errorf("deadline mismatch")
	}
}
