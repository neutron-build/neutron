// ---------------------------------------------------------------------------
// @neutron/nucleus/timeseries — Time-Series model plugin
// ---------------------------------------------------------------------------

import type { Transport, NucleusPlugin, NucleusFeatures } from '../types.js';
import { requireNucleus } from '../helpers.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface TimeSeriesPoint {
  timestamp: Date;
  value: number;
  tags?: Record<string, string>;
}

export type AggFunc = 'sum' | 'avg' | 'min' | 'max' | 'count' | 'first' | 'last';

export type BucketInterval = 'second' | 'minute' | 'hour' | 'day' | 'week' | 'month';

export interface TimeSeriesQueryOptions {
  /** Filter by tags. */
  tags?: Record<string, string>;
  /** Downsample into buckets. */
  downsample?: {
    /** Bucket interval name. */
    interval: BucketInterval;
    /** Aggregation function. */
    fn: AggFunc;
  };
}

// ---------------------------------------------------------------------------
// TimeSeriesModel interface
// ---------------------------------------------------------------------------

export interface TimeSeriesModel {
  /** Write data points to a measurement (series). */
  write(measurement: string, points: TimeSeriesPoint[]): Promise<void>;

  /** Return the most recent value for a series. */
  last(measurement: string): Promise<number | null>;

  /** Return the total number of data points. */
  count(measurement: string): Promise<number>;

  /** Count data points in a time range. */
  rangeCount(measurement: string, from: Date, to: Date): Promise<number>;

  /** Average value in a time range. */
  rangeAvg(measurement: string, from: Date, to: Date): Promise<number | null>;

  /** Set the data retention period (in days). */
  retention(measurement: string, days: number): Promise<boolean>;

  /** Match series names against a pattern. */
  match(measurement: string, pattern: string): Promise<string>;

  /** Truncate a timestamp to a bucket boundary. */
  timeBucket(interval: BucketInterval, timestamp: Date): Promise<number>;

  /** Query raw data points in a time range. */
  query(measurement: string, from: Date, to: Date, opts?: TimeSeriesQueryOptions): Promise<TimeSeriesPoint[]>;

  /** Aggregate data points into time buckets. */
  aggregate(
    measurement: string,
    from: Date,
    to: Date,
    interval: BucketInterval,
    fn: AggFunc,
  ): Promise<TimeSeriesPoint[]>;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

class TimeSeriesModelImpl implements TimeSeriesModel {
  constructor(
    private readonly transport: Transport,
    private readonly features: NucleusFeatures,
  ) {}

  private require(): void {
    requireNucleus(this.features, 'TimeSeries');
  }

  async write(measurement: string, points: TimeSeriesPoint[]): Promise<void> {
    this.require();
    for (const p of points) {
      const tsMs = p.timestamp.getTime();
      await this.transport.execute('SELECT TS_INSERT($1, $2, $3)', [measurement, tsMs, p.value]);
    }
  }

  async last(measurement: string): Promise<number | null> {
    this.require();
    return this.transport.fetchval<number>('SELECT TS_LAST($1)', [measurement]);
  }

  async count(measurement: string): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT TS_COUNT($1)', [measurement])) ?? 0;
  }

  async rangeCount(measurement: string, from: Date, to: Date): Promise<number> {
    this.require();
    return (
      (await this.transport.fetchval<number>('SELECT TS_RANGE_COUNT($1, $2, $3)', [
        measurement, from.getTime(), to.getTime(),
      ])) ?? 0
    );
  }

  async rangeAvg(measurement: string, from: Date, to: Date): Promise<number | null> {
    this.require();
    return this.transport.fetchval<number>('SELECT TS_RANGE_AVG($1, $2, $3)', [
      measurement, from.getTime(), to.getTime(),
    ]);
  }

  async retention(measurement: string, days: number): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT TS_RETENTION($1, $2)', [measurement, days])) ?? false;
  }

  async match(measurement: string, pattern: string): Promise<string> {
    this.require();
    return (await this.transport.fetchval<string>('SELECT TS_MATCH($1, $2)', [measurement, pattern])) ?? '';
  }

  async timeBucket(interval: BucketInterval, timestamp: Date): Promise<number> {
    this.require();
    return (
      (await this.transport.fetchval<number>('SELECT TIME_BUCKET($1, $2)', [interval, timestamp.getTime()])) ?? 0
    );
  }

  async query(
    measurement: string,
    from: Date,
    to: Date,
    opts: TimeSeriesQueryOptions = {},
  ): Promise<TimeSeriesPoint[]> {
    this.require();

    // Delegate to aggregate if downsample is requested
    if (opts.downsample) {
      return this.aggregate(measurement, from, to, opts.downsample.interval, opts.downsample.fn);
    }

    const fromMs = from.getTime();
    const toMs = to.getTime();

    let sql: string;
    let params: unknown[];

    if (opts.tags && Object.keys(opts.tags).length > 0) {
      const tagsJson = JSON.stringify(opts.tags);
      sql =
        `SELECT COALESCE((SELECT json_agg(row_to_json(t)) FROM (` +
        `SELECT timestamp_ms, value FROM ts_data WHERE series = $1 AND timestamp_ms >= $2 AND timestamp_ms <= $3 AND tags @> $4::jsonb ORDER BY timestamp_ms` +
        `) t), '[]')`;
      params = [measurement, fromMs, toMs, tagsJson];
    } else {
      sql =
        `SELECT COALESCE((SELECT json_agg(row_to_json(t)) FROM (` +
        `SELECT timestamp_ms, value FROM ts_data WHERE series = $1 AND timestamp_ms >= $2 AND timestamp_ms <= $3 ORDER BY timestamp_ms` +
        `) t), '[]')`;
      params = [measurement, fromMs, toMs];
    }

    const raw = await this.transport.fetchval<string>(sql, params);
    if (!raw) return [];

    const points = JSON.parse(raw) as Array<{ timestamp_ms: number; value: number }>;
    return points.map((p) => ({
      timestamp: new Date(p.timestamp_ms),
      value: p.value,
    }));
  }

  async aggregate(
    measurement: string,
    from: Date,
    to: Date,
    interval: BucketInterval,
    fn: AggFunc,
  ): Promise<TimeSeriesPoint[]> {
    this.require();

    const sql =
      `SELECT COALESCE((SELECT json_agg(row_to_json(t)) FROM (` +
      `SELECT TIME_BUCKET($1, timestamp_ms) AS bucket_ms, ${fn}(value) AS value ` +
      `FROM ts_data WHERE series = $2 AND timestamp_ms >= $3 AND timestamp_ms <= $4 ` +
      `GROUP BY bucket_ms ORDER BY bucket_ms` +
      `) t), '[]')`;

    const raw = await this.transport.fetchval<string>(sql, [interval, measurement, from.getTime(), to.getTime()]);
    if (!raw) return [];

    const points = JSON.parse(raw) as Array<{ bucket_ms: number; value: number }>;
    return points.map((p) => ({
      timestamp: new Date(p.bucket_ms),
      value: p.value,
    }));
  }
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

/** Plugin: adds `.timeseries` to the client. */
export const withTimeSeries: NucleusPlugin<{ timeseries: TimeSeriesModel }> = {
  name: 'timeseries',
  init(transport: Transport, features: NucleusFeatures) {
    return { timeseries: new TimeSeriesModelImpl(transport, features) };
  },
};
