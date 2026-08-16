// ---------------------------------------------------------------------------
// @neutron-build/nucleus/timeseries — Time-Series model plugin
// ---------------------------------------------------------------------------

import type { Transport, NucleusPlugin, NucleusFeatures } from '../types.js';
import { requireNucleus } from '../helpers.js';
import { NucleusNotSupportedError } from '../errors.js';

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

/** Fixed bucket sizes in milliseconds ('month' approximated as 30 days). */
const BUCKET_MS: Record<BucketInterval, number> = {
  second: 1_000,
  minute: 60_000,
  hour: 3_600_000,
  day: 86_400_000,
  week: 604_800_000,
  month: 2_592_000_000,
};

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

  /**
   * Set the data retention period (in days).
   *
   * The engine retention policy is global across all series, not
   * per-measurement (`TS_RETENTION(max_age_ms)`).
   */
  retention(days: number): Promise<boolean>;

  /** Check whether a text matches a full-text query (`TS_MATCH(text, query)`). */
  match(text: string, query: string): Promise<boolean>;

  /** Truncate a timestamp to a bucket boundary ('month' approximated as 30 days). */
  timeBucket(interval: BucketInterval, timestamp: Date): Promise<number>;

  /**
   * Query raw data points in a time range.
   *
   * NOT SUPPORTED: the engine exposes no raw point-range fetch (only
   * TS_LAST/TS_COUNT/TS_RANGE_COUNT/TS_RANGE_AVG). Always throws
   * NucleusNotSupportedError unless `opts.downsample` is given, in which
   * case the call is delegated to `aggregate()`.
   */
  query(measurement: string, from: Date, to: Date, opts?: TimeSeriesQueryOptions): Promise<TimeSeriesPoint[]>;

  /**
   * Aggregate data points into time buckets.
   *
   * Only 'avg' and 'count' are supported — the engine's range surface is
   * TS_RANGE_AVG and TS_RANGE_COUNT; other aggregation functions throw
   * NucleusNotSupportedError.
   */
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

  async retention(days: number): Promise<boolean> {
    this.require();
    const maxAgeMs = days * 86_400_000;
    const result = await this.transport.fetchval<string>('SELECT TS_RETENTION($1)', [maxAgeMs]);
    return result === 'OK';
  }

  async match(text: string, query: string): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT TS_MATCH($1, $2)', [text, query])) ?? false;
  }

  async timeBucket(interval: BucketInterval, timestamp: Date): Promise<number> {
    this.require();
    return (
      (await this.transport.fetchval<number>('SELECT TIME_BUCKET($1, $2)', [
        BUCKET_MS[interval], timestamp.getTime(),
      ])) ?? 0
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

    // This used to throw NucleusNotSupportedError: "the engine has no raw
    // point-range fetch". That was true of the SQL surface and false of the
    // store — Python was synthesising the same answer from sixty bucketed
    // TS_RANGE_AVG calls at the time, and Go refused outright, so one question
    // had three answers. TS_RANGE now returns the points and every SDK uses it.
    const startMs = from.getTime();
    const endMs = to.getTime();
    if (endMs <= startMs) return [];

    const raw = await this.transport.fetchval<string>('SELECT TS_RANGE($1, $2, $3)', [
      measurement,
      startMs,
      endMs,
    ]);
    if (!raw) return [];

    return (JSON.parse(raw) as Array<{ t: number; v: number }>).map(({ t, v }) => ({
      timestamp: new Date(t),
      value: v,
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

    const VALID_AGG_FUNCS = ['sum', 'avg', 'min', 'max', 'count', 'first', 'last'] as const;
    if (!VALID_AGG_FUNCS.includes(fn)) {
      throw new Error(`Invalid aggregation function: ${fn}. Must be one of: ${VALID_AGG_FUNCS.join(', ')}`);
    }
    if (fn !== 'avg' && fn !== 'count') {
      throw new NucleusNotSupportedError(
        `timeseries.aggregate: the engine only exposes TS_RANGE_AVG and TS_RANGE_COUNT; '${fn}' is not supported.`,
      );
    }

    const bucketMs = BUCKET_MS[interval];
    const fromMs = from.getTime();
    const toMs = to.getTime();
    const firstBucket = Math.floor(fromMs / bucketMs) * bucketMs;
    const bucketCount = Math.floor((toMs - firstBucket) / bucketMs) + 1;
    if (bucketCount > 10_000) {
      throw new Error(`timeseries.aggregate: range spans ${bucketCount} buckets (max 10000)`);
    }

    const fnSql = fn === 'avg' ? 'SELECT TS_RANGE_AVG($1, $2, $3)' : 'SELECT TS_RANGE_COUNT($1, $2, $3)';
    const points: TimeSeriesPoint[] = [];
    for (let bucket = firstBucket; bucket <= toMs; bucket += bucketMs) {
      const start = Math.max(bucket, fromMs);
      const end = Math.min(bucket + bucketMs - 1, toMs);
      const value = await this.transport.fetchval<number>(fnSql, [measurement, start, end]);
      if (value === null) continue; // TS_RANGE_AVG returns NULL for empty buckets
      if (fn === 'count' && value === 0) continue;
      points.push({ timestamp: new Date(bucket), value });
    }
    return points;
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
