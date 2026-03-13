package nucleus

import (
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
