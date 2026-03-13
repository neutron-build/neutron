package nucleus

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

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
	Sum   AggFunc = iota
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
func (ts *TimeSeriesModel) Write(ctx context.Context, measurement string, points []TimeSeriesPoint) error {
	if err := ts.client.requireNucleus("TimeSeries.Write"); err != nil {
		return err
	}
	for _, p := range points {
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

// Retention sets the data retention period for a series.
func (ts *TimeSeriesModel) Retention(ctx context.Context, measurement string, days int64) (bool, error) {
	if err := ts.client.requireNucleus("TimeSeries.Retention"); err != nil {
		return false, err
	}
	var ok bool
	err := ts.pool.QueryRow(ctx, "SELECT TS_RETENTION($1, $2)", measurement, days).Scan(&ok)
	return ok, wrapErr("ts retention", err)
}

// Match finds series names matching a pattern.
func (ts *TimeSeriesModel) Match(ctx context.Context, measurement, pattern string) (string, error) {
	if err := ts.client.requireNucleus("TimeSeries.Match"); err != nil {
		return "", err
	}
	var result string
	err := ts.pool.QueryRow(ctx, "SELECT TS_MATCH($1, $2)", measurement, pattern).Scan(&result)
	return result, wrapErr("ts match", err)
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

// tsRawPoint is the internal representation of a time series point from Nucleus.
type tsRawPoint struct {
	TimestampMs int64   `json:"timestamp_ms"`
	Value       float64 `json:"value"`
}

// Query retrieves raw data points in a time range.
// Supports WithTags for filtering and WithDownsample for aggregation.
// If WithDownsample is specified, the query delegates to Aggregate.
func (ts *TimeSeriesModel) Query(ctx context.Context, measurement string, from, to time.Time, opts ...TSOption) ([]TimeSeriesPoint, error) {
	if err := ts.client.requireNucleus("TimeSeries.Query"); err != nil {
		return nil, err
	}

	o := applyTSOpts(opts)

	// If downsample is requested, delegate to Aggregate
	if o.downsample != nil {
		return ts.Aggregate(ctx, measurement, from, to, o.downsample.window, o.downsample.fn)
	}

	// Build tag filter as JSON if tags are provided
	var tagsArg string
	if len(o.tags) > 0 {
		tagsJSON, err := json.Marshal(o.tags)
		if err != nil {
			return nil, fmt.Errorf("nucleus: ts query marshal tags: %w", err)
		}
		tagsArg = string(tagsJSON)
	}

	// Use TS_RANGE_COUNT first to check if there are data points,
	// then query the underlying time series table for raw data.
	// Nucleus stores time series data as (series, timestamp_ms, value) tuples.
	var raw string
	var err error
	if tagsArg != "" {
		err = ts.pool.QueryRow(ctx,
			"SELECT COALESCE((SELECT json_agg(row_to_json(t)) FROM (SELECT timestamp_ms, value FROM ts_data WHERE series = $1 AND timestamp_ms >= $2 AND timestamp_ms <= $3 AND tags @> $4::jsonb ORDER BY timestamp_ms) t), '[]')",
			measurement, from.UnixMilli(), to.UnixMilli(), tagsArg).Scan(&raw)
	} else {
		err = ts.pool.QueryRow(ctx,
			"SELECT COALESCE((SELECT json_agg(row_to_json(t)) FROM (SELECT timestamp_ms, value FROM ts_data WHERE series = $1 AND timestamp_ms >= $2 AND timestamp_ms <= $3 ORDER BY timestamp_ms) t), '[]')",
			measurement, from.UnixMilli(), to.UnixMilli()).Scan(&raw)
	}
	if err != nil {
		return nil, wrapErr("ts query", err)
	}

	var rawPoints []tsRawPoint
	if err := json.Unmarshal([]byte(raw), &rawPoints); err != nil {
		return nil, fmt.Errorf("nucleus: ts query unmarshal: %w", err)
	}

	points := make([]TimeSeriesPoint, len(rawPoints))
	for i, rp := range rawPoints {
		points[i] = TimeSeriesPoint{
			Timestamp: time.UnixMilli(rp.TimestampMs),
			Value:     rp.Value,
		}
	}
	return points, nil
}

// Aggregate queries with downsampling into time buckets.
// Uses TIME_BUCKET to group data points and applies the aggregation function.
func (ts *TimeSeriesModel) Aggregate(ctx context.Context, measurement string, from, to time.Time, window time.Duration, fn AggFunc) ([]TimeSeriesPoint, error) {
	if err := ts.client.requireNucleus("TimeSeries.Aggregate"); err != nil {
		return nil, err
	}

	interval := windowToInterval(window)
	aggFn := fn.String()

	// Build aggregation query using TIME_BUCKET
	q := fmt.Sprintf(
		`SELECT COALESCE((SELECT json_agg(row_to_json(t)) FROM (
			SELECT TIME_BUCKET($1, timestamp_ms) AS bucket_ms, %s(value) AS value
			FROM ts_data
			WHERE series = $2 AND timestamp_ms >= $3 AND timestamp_ms <= $4
			GROUP BY bucket_ms
			ORDER BY bucket_ms
		) t), '[]')`, aggFn)

	var raw string
	err := ts.pool.QueryRow(ctx, q, interval, measurement, from.UnixMilli(), to.UnixMilli()).Scan(&raw)
	if err != nil {
		return nil, wrapErr("ts aggregate", err)
	}

	type aggPoint struct {
		BucketMs int64   `json:"bucket_ms"`
		Value    float64 `json:"value"`
	}

	var rawPoints []aggPoint
	if err := json.Unmarshal([]byte(raw), &rawPoints); err != nil {
		return nil, fmt.Errorf("nucleus: ts aggregate unmarshal: %w", err)
	}

	points := make([]TimeSeriesPoint, len(rawPoints))
	for i, rp := range rawPoints {
		points[i] = TimeSeriesPoint{
			Timestamp: time.UnixMilli(rp.BucketMs),
			Value:     rp.Value,
		}
	}
	return points, nil
}
