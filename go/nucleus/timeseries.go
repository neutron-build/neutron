package nucleus

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// validAggFuncs is the allowlist of aggregation functions safe to interpolate into SQL.
var validAggFuncs = map[string]bool{
	"sum": true, "avg": true, "min": true, "max": true,
	"count": true, "first": true, "last": true,
}

// TimeSeriesModel provides time-series operations over Nucleus SQL functions.
type TimeSeriesModel struct {
	pool   querier
	client *Client
}

// TimeSeriesPoint represents a single data point.
type TimeSeriesPoint struct {
	Timestamp time.Time
	Value     float64
	Tags      map[string]string
}

// AggFunc defines aggregation functions for time-series queries.
type AggFunc int

const (
	Sum AggFunc = iota
	Avg
	Min
	Max
	Count
	First
	Last
)

func (f AggFunc) String() string {
	switch f {
	case Sum:
		return "sum"
	case Avg:
		return "avg"
	case Min:
		return "min"
	case Max:
		return "max"
	case Count:
		return "count"
	case First:
		return "first"
	case Last:
		return "last"
	default:
		return "avg"
	}
}

// windowToInterval converts a time.Duration to a Nucleus TIME_BUCKET interval string.
func windowToInterval(d time.Duration) string {
	switch {
	case d >= 30*24*time.Hour:
		return "month"
	case d >= 7*24*time.Hour:
		return "week"
	case d >= 24*time.Hour:
		return "day"
	case d >= time.Hour:
		return "hour"
	case d >= time.Minute:
		return "minute"
	default:
		return "second"
	}
}

// TSOption configures time-series queries.
type TSOption func(*tsOpts)

type tsOpts struct {
	tags       map[string]string
	downsample *downsampleOpts
}

type downsampleOpts struct {
	window time.Duration
	fn     AggFunc
}

// WithTags filters time-series data by tags.
func WithTags(tags map[string]string) TSOption {
	return func(o *tsOpts) { o.tags = tags }
}

// WithDownsample downsamples results into time buckets.
func WithDownsample(window time.Duration, fn AggFunc) TSOption {
	return func(o *tsOpts) { o.downsample = &downsampleOpts{window: window, fn: fn} }
}

func applyTSOpts(opts []TSOption) tsOpts {
	var o tsOpts
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// Write inserts time-series data points into a measurement (series).
// The engine's TS_INSERT has no tags parameter, so points with Tags set
// are rejected rather than silently dropping the tags.
func (ts *TimeSeriesModel) Write(ctx context.Context, measurement string, points []TimeSeriesPoint) error {
	if err := ts.client.requireNucleus("TimeSeries.Write"); err != nil {
		return err
	}
	for _, p := range points {
		if len(p.Tags) > 0 {
			return fmt.Errorf("nucleus: ts write: tags are not supported by TS_INSERT")
		}
		tsMs := p.Timestamp.UnixMilli()
		_, err := ts.pool.Exec(ctx, "SELECT TS_INSERT($1, $2, $3)", measurement, tsMs, p.Value)
		if err != nil {
			return fmt.Errorf("nucleus: ts insert: %w", err)
		}
	}
	return nil
}

// Last returns the most recent value for a series.
func (ts *TimeSeriesModel) Last(ctx context.Context, measurement string) (*float64, error) {
	if err := ts.client.requireNucleus("TimeSeries.Last"); err != nil {
		return nil, err
	}
	var val *float64
	err := ts.pool.QueryRow(ctx, "SELECT TS_LAST($1)", measurement).Scan(&val)
	return val, wrapErr("ts last", err)
}

// Count returns the total number of data points in a series.
func (ts *TimeSeriesModel) Count(ctx context.Context, measurement string) (int64, error) {
	if err := ts.client.requireNucleus("TimeSeries.Count"); err != nil {
		return 0, err
	}
	var n int64
	err := ts.pool.QueryRow(ctx, "SELECT TS_COUNT($1)", measurement).Scan(&n)
	return n, wrapErr("ts count", err)
}

// RangeCount returns the number of data points in a time range.
func (ts *TimeSeriesModel) RangeCount(ctx context.Context, measurement string, from, to time.Time) (int64, error) {
	if err := ts.client.requireNucleus("TimeSeries.RangeCount"); err != nil {
		return 0, err
	}
	var n int64
	err := ts.pool.QueryRow(ctx, "SELECT TS_RANGE_COUNT($1, $2, $3)",
		measurement, from.UnixMilli(), to.UnixMilli()).Scan(&n)
	return n, wrapErr("ts range_count", err)
}

// RangeAvg returns the average value of data points in a time range.
func (ts *TimeSeriesModel) RangeAvg(ctx context.Context, measurement string, from, to time.Time) (*float64, error) {
	if err := ts.client.requireNucleus("TimeSeries.RangeAvg"); err != nil {
		return nil, err
	}
	var val *float64
	err := ts.pool.QueryRow(ctx, "SELECT TS_RANGE_AVG($1, $2, $3)",
		measurement, from.UnixMilli(), to.UnixMilli()).Scan(&val)
	return val, wrapErr("ts range_avg", err)
}

// Retention sets the global data retention policy. The engine's
// TS_RETENTION takes a single max-age argument and applies to all series.
func (ts *TimeSeriesModel) Retention(ctx context.Context, maxAge time.Duration) (bool, error) {
	if err := ts.client.requireNucleus("TimeSeries.Retention"); err != nil {
		return false, err
	}
	var result string
	err := ts.pool.QueryRow(ctx, "SELECT TS_RETENTION($1)", maxAge.Milliseconds()).Scan(&result)
	return result == "OK", wrapErr("ts retention", err)
}

// Match reports whether text matches a full-text query (engine TS_MATCH).
func (ts *TimeSeriesModel) Match(ctx context.Context, text, query string) (bool, error) {
	if err := ts.client.requireNucleus("TimeSeries.Match"); err != nil {
		return false, err
	}
	var ok bool
	err := ts.pool.QueryRow(ctx, "SELECT TS_MATCH($1, $2)", text, query).Scan(&ok)
	return ok, wrapErr("ts match", err)
}

// TimeBucket truncates a timestamp to a bucket boundary.
// Intervals: "second", "minute", "hour", "day", "week", "month".
func (ts *TimeSeriesModel) TimeBucket(ctx context.Context, interval string, timestamp time.Time) (int64, error) {
	if err := ts.client.requireNucleus("TimeSeries.TimeBucket"); err != nil {
		return 0, err
	}
	var bucket int64
	err := ts.pool.QueryRow(ctx, "SELECT TIME_BUCKET($1, $2)", interval, timestamp.UnixMilli()).Scan(&bucket)
	return bucket, wrapErr("ts time_bucket", err)
}

// Query retrieves the raw data points stored in a time range.
//
// It used to refuse: "raw point retrieval is not supported by the engine".
// That was true of the SQL surface and false of the store — Python was
// synthesising the same answer from sixty bucketed TS_RANGE_AVG calls at the
// time, so the three SDKs gave three different answers to one question.
// TS_RANGE now exposes the points directly and every SDK uses it.
//
// If WithDownsample is specified the query still delegates to Aggregate;
// WithTags remains unsupported by the engine.
func (ts *TimeSeriesModel) Query(ctx context.Context, measurement string, from, to time.Time, opts ...TSOption) ([]TimeSeriesPoint, error) {
	if err := ts.client.requireNucleus("TimeSeries.Query"); err != nil {
		return nil, err
	}

	o := applyTSOpts(opts)

	if len(o.tags) > 0 {
		return nil, fmt.Errorf("nucleus: ts query: tag filtering is not supported by the engine")
	}

	// If downsample is requested, delegate to Aggregate
	if o.downsample != nil {
		return ts.Aggregate(ctx, measurement, from, to, o.downsample.window, o.downsample.fn)
	}

	startMs, endMs := from.UnixMilli(), to.UnixMilli()
	if endMs <= startMs {
		return []TimeSeriesPoint{}, nil
	}

	var raw string
	if err := ts.pool.QueryRow(ctx, "SELECT TS_RANGE($1, $2, $3)", measurement, startMs, endMs).Scan(&raw); err != nil {
		return nil, wrapErr("ts query", err)
	}
	if raw == "" {
		return []TimeSeriesPoint{}, nil
	}

	var wire []struct {
		T int64   `json:"t"`
		V float64 `json:"v"`
	}
	if err := json.Unmarshal([]byte(raw), &wire); err != nil {
		return nil, wrapErr("ts query decode", err)
	}

	points := make([]TimeSeriesPoint, len(wire))
	for i, w := range wire {
		points[i] = TimeSeriesPoint{
			Timestamp: time.UnixMilli(w.T).UTC(),
			Value:     w.V,
		}
	}
	return points, nil
}

// maxAggregateBuckets caps the number of per-bucket engine calls Aggregate
// will issue for a single query.
const maxAggregateBuckets = 10000

// Aggregate downsamples a time range into fixed-width windows.
// The engine exposes only TS_RANGE_COUNT and TS_RANGE_AVG, so only
// Count and Avg are supported; one engine call is issued per window.
// Windows with no data are omitted.
func (ts *TimeSeriesModel) Aggregate(ctx context.Context, measurement string, from, to time.Time, window time.Duration, fn AggFunc) ([]TimeSeriesPoint, error) {
	if err := ts.client.requireNucleus("TimeSeries.Aggregate"); err != nil {
		return nil, err
	}

	if fn != Count && fn != Avg {
		return nil, fmt.Errorf("nucleus: ts aggregate: %s is not supported by the engine (only count and avg)", fn.String())
	}
	if window <= 0 {
		return nil, fmt.Errorf("nucleus: ts aggregate: window must be positive")
	}

	fromMs := from.UnixMilli()
	toMs := to.UnixMilli()
	windowMs := window.Milliseconds()
	if toMs < fromMs {
		return nil, fmt.Errorf("nucleus: ts aggregate: to precedes from")
	}
	if (toMs-fromMs)/windowMs >= maxAggregateBuckets {
		return nil, fmt.Errorf("nucleus: ts aggregate: range/window yields more than %d buckets", maxAggregateBuckets)
	}

	var points []TimeSeriesPoint
	for start := fromMs; start <= toMs; start += windowMs {
		end := start + windowMs - 1
		if end > toMs {
			end = toMs
		}
		switch fn {
		case Count:
			var n int64
			err := ts.pool.QueryRow(ctx, "SELECT TS_RANGE_COUNT($1, $2, $3)",
				measurement, start, end).Scan(&n)
			if err != nil {
				return nil, wrapErr("ts aggregate", err)
			}
			if n > 0 {
				points = append(points, TimeSeriesPoint{Timestamp: time.UnixMilli(start), Value: float64(n)})
			}
		case Avg:
			var val *float64
			err := ts.pool.QueryRow(ctx, "SELECT TS_RANGE_AVG($1, $2, $3)",
				measurement, start, end).Scan(&val)
			if err != nil {
				return nil, wrapErr("ts aggregate", err)
			}
			if val != nil {
				points = append(points, TimeSeriesPoint{Timestamp: time.UnixMilli(start), Value: *val})
			}
		}
	}
	return points, nil
}
