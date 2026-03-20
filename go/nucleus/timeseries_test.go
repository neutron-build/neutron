package nucleus

import (
	"context"
	"testing"
	"time"
)

func TestAggFuncString(t *testing.T) {
	tests := []struct {
		f    AggFunc
		want string
	}{
		{Sum, "sum"},
		{Avg, "avg"},
		{Min, "min"},
		{Max, "max"},
		{Count, "count"},
		{First, "first"},
		{Last, "last"},
	}
	for _, tt := range tests {
		if got := tt.f.String(); got != tt.want {
			t.Errorf("%d.String() = %q, want %q", tt.f, got, tt.want)
		}
	}
}

func TestTimeSeriesPoint(t *testing.T) {
	p := TimeSeriesPoint{Value: 42.5}
	if p.Value != 42.5 {
		t.Errorf("Value = %f", p.Value)
	}
}

func TestWindowToInterval(t *testing.T) {
	tests := []struct {
		d    time.Duration
		want string
	}{
		{time.Second, "second"},
		{30 * time.Second, "second"},
		{time.Minute, "minute"},
		{30 * time.Minute, "minute"},
		{time.Hour, "hour"},
		{12 * time.Hour, "hour"},
		{24 * time.Hour, "day"},
		{3 * 24 * time.Hour, "day"},
		{7 * 24 * time.Hour, "week"},
		{14 * 24 * time.Hour, "week"},
		{30 * 24 * time.Hour, "month"},
		{365 * 24 * time.Hour, "month"},
	}
	for _, tt := range tests {
		if got := windowToInterval(tt.d); got != tt.want {
			t.Errorf("windowToInterval(%v) = %q, want %q", tt.d, got, tt.want)
		}
	}
}

func TestTSOptionWithTags(t *testing.T) {
	tags := map[string]string{"host": "server1"}
	o := applyTSOpts([]TSOption{WithTags(tags)})
	if o.tags["host"] != "server1" {
		t.Errorf("tags = %v", o.tags)
	}
}

func TestTSOptionWithDownsample(t *testing.T) {
	o := applyTSOpts([]TSOption{WithDownsample(time.Hour, Avg)})
	if o.downsample == nil {
		t.Fatal("downsample should be set")
	}
	if o.downsample.window != time.Hour {
		t.Errorf("window = %v", o.downsample.window)
	}
	if o.downsample.fn != Avg {
		t.Errorf("fn = %v", o.downsample.fn)
	}
}

func TestAggFuncStringDefault(t *testing.T) {
	var f AggFunc = 99
	if f.String() != "avg" {
		t.Errorf("unknown AggFunc.String() = %q, want avg", f.String())
	}
}

func TestValidAggFuncs(t *testing.T) {
	expected := []string{"sum", "avg", "min", "max", "count", "first", "last"}
	for _, fn := range expected {
		if !validAggFuncs[fn] {
			t.Errorf("validAggFuncs missing %q", fn)
		}
	}
	// Verify invalid ones are not in the map
	if validAggFuncs["DROP"] {
		t.Error("DROP should not be a valid agg func")
	}
}

func TestTSRequiresNucleus(t *testing.T) {
	q := &mockCDCQuerier{}
	client := plainPGClient()
	ts := &TimeSeriesModel{pool: q, client: client}
	now := time.Now()

	tests := []struct {
		name string
		fn   func() error
	}{
		{"Write", func() error { return ts.Write(context.Background(), "m", nil) }},
		{"Last", func() error { _, err := ts.Last(context.Background(), "m"); return err }},
		{"Count", func() error { _, err := ts.Count(context.Background(), "m"); return err }},
		{"RangeCount", func() error {
			_, err := ts.RangeCount(context.Background(), "m", now, now)
			return err
		}},
		{"RangeAvg", func() error {
			_, err := ts.RangeAvg(context.Background(), "m", now, now)
			return err
		}},
		{"Retention", func() error { _, err := ts.Retention(context.Background(), "m", 30); return err }},
		{"Match", func() error { _, err := ts.Match(context.Background(), "m", "*"); return err }},
		{"TimeBucket", func() error { _, err := ts.TimeBucket(context.Background(), "hour", now); return err }},
		{"Query", func() error { _, err := ts.Query(context.Background(), "m", now, now); return err }},
		{"Aggregate", func() error {
			_, err := ts.Aggregate(context.Background(), "m", now, now, time.Hour, Avg)
			return err
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fn()
			if err == nil {
				t.Fatal("expected error for non-Nucleus database")
			}
		})
	}
}

func TestTSPointTags(t *testing.T) {
	p := TimeSeriesPoint{
		Timestamp: time.Now(),
		Value:     99.9,
		Tags:      map[string]string{"host": "srv1", "region": "us-east"},
	}
	if p.Tags["host"] != "srv1" {
		t.Errorf("Tags = %v", p.Tags)
	}
}

func TestTSOptionCombined(t *testing.T) {
	tags := map[string]string{"host": "srv1"}
	o := applyTSOpts([]TSOption{
		WithTags(tags),
		WithDownsample(time.Hour, Sum),
	})
	if o.tags["host"] != "srv1" {
		t.Errorf("tags = %v", o.tags)
	}
	if o.downsample == nil {
		t.Fatal("downsample should be set")
	}
	if o.downsample.fn != Sum {
		t.Errorf("fn = %v, want Sum", o.downsample.fn)
	}
}

func TestTSOptionDefaults(t *testing.T) {
	o := applyTSOpts(nil)
	if o.tags != nil {
		t.Errorf("default tags = %v", o.tags)
	}
	if o.downsample != nil {
		t.Errorf("default downsample = %v", o.downsample)
	}
}
