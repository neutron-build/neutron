// ---------------------------------------------------------------------------
// @neutron-build/nucleus/timeseries — Time-Series model plugin (X03)
// ---------------------------------------------------------------------------
// Capability discipline (I01 / X00): the engine's DOCUMENTED surface
// (TS_INSERT / TS_COUNT / TS_LAST / TS_RANGE_COUNT / TS_RANGE_AVG /
// TS_RETENTION — nucleus/docs/MODEL_SEMANTICS.md) is gated by
// requireNucleus like before. Two client methods depend on functions the
// X00 capability report records NO evidence for: TS_RANGE (raw point
// fetch) and TIME_BUCKET. Those are gated by a live SEMANTIC PROBE with
// negative controls, run lazily once per model instance and memoized:
// a fake implementation (constant results, global-aggregate-as-range,
// input-echo bucketing) fails the probe and the call throws
// NucleusNotSupportedError with the probe evidence — fail closed, never a
// silently wrong answer. The recorded engine evidence lives in
// conformance/live/orm/x03-nucleus-leg.mjs output.
//
// Retention honesty (nucleus/docs/MODEL_SEMANTICS.md "Time series"):
// TS_RETENTION(max_age_ms) is GLOBAL across every series, destructive,
// retroactive and irreversible — it applies at the next checkpoint tick
// against wall-clock now, deletes existing history older than the policy,
// and destroys backfilled historical points within one checkpoint
// interval of writing them. There is no per-series retention and no
// read-back of the current policy. retention(days) documents exactly
// that; per-series retention is not offered (it does not exist).

import type { Transport, NucleusPlugin, NucleusFeatures } from '../types.js';
import { requireNucleus } from '../helpers.js';
import { NucleusNotSupportedError } from '../errors.js';
import { randomBytes } from 'node:crypto';

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
   * per-measurement (`TS_RETENTION(max_age_ms)`) — and it is DESTRUCTIVE,
   * RETROACTIVE and IRREVERSIBLE: at the next checkpoint tick the engine
   * computes a cutoff from wall-clock now and drains every point older
   * than the policy from EVERY series (a 1-hour policy destroys a year of
   * history within one tick; a backfill older than the policy is destroyed
   * within one tick of being written). There is no dry run, no undo and no
   * read-back of the current policy. Call this only when that is what you
   * want for the entire engine.
   */
  retention(days: number): Promise<boolean>;

  /**
   * Query raw data points in a time range (`TS_RANGE`).
   *
   * NOT covered by the engine's documented surface (MODEL_SEMANTICS lists
   * no raw point fetch) — gated by a lazily-memoized semantic probe with
   * negative controls. Throws NucleusNotSupportedError with the probe
   * evidence when the engine's TS_RANGE does not return the exact points
   * of the range (fake or absent implementations fail closed).
   */
  query(measurement: string, from: Date, to: Date, opts?: TimeSeriesQueryOptions): Promise<TimeSeriesPoint[]>;

  /**
   * Check whether a text matches a full-text query (`TS_MATCH(text, query)`).
   *
   * @deprecated TS_MATCH is the engine's TEXT-SEARCH surface (its name
   * merely collides with the TS_ policy prefix) — it has no time-series
   * semantics, and the TS_ prefix additionally blocks it for
   * non-superusers once any RLS policy exists (MODEL_SEMANTICS "Time
   * series" policy note). Kept for compatibility only; do not use it from
   * a time-series workflow.
   */
  match(text: string, query: string): Promise<boolean>;

  /**
   * Truncate a timestamp to a bucket boundary ('month' approximated as 30
   * days).
   *
   * NOT covered by the engine's documented surface — gated by a
   * lazily-memoized semantic probe: TIME_BUCKET must floor an unaligned
   * timestamp to the bucket grid and map two different inputs to two
   * different buckets. Fake implementations fail closed.
   */
  timeBucket(interval: BucketInterval, timestamp: Date): Promise<number>;

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
// Semantic probe (X03): live evidence for the surfaces the X00 capability
// report records nothing for. Negative controls catch the classic fakes:
// a constant TS_COUNT, a global-aggregate-as-range TS_RANGE_AVG, an
// input-echoing TIME_BUCKET. Runs against a uniquely-named probe series
// (the engine has no point deletion — probe points are inert and never
// touch user series). Memoize per model instance, not per process: the
// evidence belongs to the transport it was measured on.
// ---------------------------------------------------------------------------

export interface TimeSeriesProbeCheck {
  readonly name: string;
  readonly passed: boolean;
  readonly detail: string;
}

export interface TimeSeriesModelEvidence {
  readonly series: string;
  readonly checks: readonly TimeSeriesProbeCheck[];
  /** TS_RANGE returned exactly the points of the requested range. */
  readonly rangeFetch: boolean;
  /** TIME_BUCKET floored unaligned inputs onto the bucket grid. */
  readonly bucketFn: boolean;
}

function num(v: unknown): number | null {
  return typeof v === 'number' && Number.isFinite(v) ? v : null;
}

export async function probeTimeSeriesModel(transport: Transport): Promise<TimeSeriesModelEvidence> {
  const series = `__neutron_ts_probe_${randomBytes(6).toString('hex')}`;
  // Probe points: the middle subrange [3000, 7000) holds values 2 and 6
  // only — a TS_RANGE_COUNT that answers 4, or a TS_RANGE_AVG that answers
  // the global mean (3.25 instead of the subrange's 4), is answering
  // globally, not per range.
  const points = [
    { t: 1000, v: 1 },
    { t: 3000, v: 2 },
    { t: 6000, v: 6 },
    { t: 9000, v: 4 },
  ];
  const checks: TimeSeriesProbeCheck[] = [];
  const check = (name: string, passed: boolean, detail: string): void => {
    checks.push({ name, passed, detail });
  };

  try {
    for (const p of points) {
      await transport.execute('SELECT TS_INSERT($1, $2, $3)', [series, p.t, p.v]);
    }
    check('ts_insert_acked', true, 'four probe points acked');
  } catch (err) {
    check('ts_insert_acked', false, `TS_INSERT rejected: ${err instanceof Error ? err.message : String(err)}`);
    return { series, checks, rangeFetch: false, bucketFn: false };
  }

  try {
    const total = num(await transport.fetchval('SELECT TS_COUNT($1)', [series]));
    check('ts_count_exact', total === 4, `TS_COUNT=${String(total)} (expected 4 — constants and fakes fail)`);
  } catch (err) {
    check('ts_count_exact', false, `TS_COUNT errored: ${err instanceof Error ? err.message : String(err)}`);
  }

  try {
    const subCount = num(await transport.fetchval('SELECT TS_RANGE_COUNT($1, $2, $3)', [series, 3000, 7000]));
    check('ts_range_count_scoped', subCount === 2, `TS_RANGE_COUNT(3000,7000)=${String(subCount)} (expected 2 — a global answer of 4 is a fake range)`);
  } catch (err) {
    check('ts_range_count_scoped', false, `TS_RANGE_COUNT errored: ${err instanceof Error ? err.message : String(err)}`);
  }

  try {
    const subAvg = num(await transport.fetchval('SELECT TS_RANGE_AVG($1, $2, $3)', [series, 3000, 7000]));
    check('ts_range_avg_scoped', subAvg === 4, `TS_RANGE_AVG(3000,7000)=${String(subAvg)} (expected 4; the GLOBAL mean 3.25 is the fake answer)`);
  } catch (err) {
    check('ts_range_avg_scoped', false, `TS_RANGE_AVG errored: ${err instanceof Error ? err.message : String(err)}`);
  }

  let rangeFetch = false;
  try {
    const raw = await transport.fetchval<string>('SELECT TS_RANGE($1, $2, $3)', [series, 2000, 7000]);
    let parsed: Array<{ t: number; v: number }> = [];
    if (typeof raw === 'string' && raw !== '') {
      try {
        parsed = JSON.parse(raw) as Array<{ t: number; v: number }>;
      } catch {
        parsed = [];
      }
    }
    const want = JSON.stringify([{ t: 3000, v: 2 }, { t: 6000, v: 6 }]);
    const gotSet = JSON.stringify(parsed.map((p) => ({ t: p.t, v: p.v })).sort((a, b) => a.t - b.t));
    const order = parsed.map((p) => p.t).join(',');
    rangeFetch = gotSet === want;
    check('ts_range_exact', rangeFetch, `TS_RANGE(2000,7000)=${gotSet.slice(0, 120)} (expected exactly ${want} — the two points of the range, no others; observed order [${order}])`);
  } catch (err) {
    check('ts_range_exact', false, `TS_RANGE errored: ${err instanceof Error ? err.message : String(err)}`);
  }

  let bucketFn = false;
  try {
    // Grid-INVARIANT checks (a correct engine may epoch-align or
    // calendar-align buckets): 1ms after the epoch must bucket to the
    // epoch itself for any 5000ms grid whose offset is a grid multiple
    // (all real timezone offsets are) — an input-echo fake returns 1; each
    // timestamp must land inside its own bucket; and two timestamps one
    // full interval apart must land in different buckets — constants fail.
    const b0 = num(await transport.fetchval('SELECT TIME_BUCKET($1, $2)', [5000, 1]));
    const b1 = num(await transport.fetchval('SELECT TIME_BUCKET($1, $2)', [5000, 12345]));
    const b2 = num(await transport.fetchval('SELECT TIME_BUCKET($1, $2)', [5000, 17345]));
    bucketFn = b0 === 0 && b1 !== null && b2 !== null && b1 <= 12345 && 12345 < b1 + 5000 && b2 <= 17345 && 17345 < b2 + 5000 && b1 !== b2;
    check('time_bucket_floors', bucketFn, `TIME_BUCKET(5000,1)=${String(b0)} (expected 0; an echo returns 1), TIME_BUCKET(5000,12345)=${String(b1)}, TIME_BUCKET(5000,17345)=${String(b2)} (each must contain its input; must differ)`);
  } catch (err) {
    check('time_bucket_floors', false, `TIME_BUCKET errored: ${err instanceof Error ? err.message : String(err)}`);
  }

  return { series, checks, rangeFetch, bucketFn };
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

class TimeSeriesModelImpl implements TimeSeriesModel {
  private evidence: Promise<TimeSeriesModelEvidence> | null = null;

  constructor(
    private readonly transport: Transport,
    private readonly features: NucleusFeatures,
  ) {}

  private require(): void {
    requireNucleus(this.features, 'TimeSeries');
  }

  /** Lazily-memoized probe evidence for the undocumented surfaces. Probe
   *  failure is a permanent answer for this transport instance — cached so
   *  every subsequent call fails closed without re-probing. */
  private probe(): Promise<TimeSeriesModelEvidence> {
    this.require();
    if (this.evidence === null) {
      this.evidence = probeTimeSeriesModel(this.transport).catch((err) => {
        this.evidence = null;
        throw err;
      });
    }
    return this.evidence;
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
    // Fail closed on unproven TIME_BUCKET semantics: the probe proves real
    // floor-bucketing before any user timestamp passes through.
    const evidence = await this.probe();
    if (!evidence.bucketFn) {
      const failed = evidence.checks.filter((c) => c.name === 'time_bucket_floors')[0];
      throw new NucleusNotSupportedError(
        `timeseries.timeBucket: TIME_BUCKET did not floor onto the bucket grid on this engine — disabled until engine support is proven (probe series ${evidence.series}: ${failed?.detail ?? 'probe incomplete'}). Evidence: ${JSON.stringify(evidence.checks)}`,
      );
    }
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

    // TS_RANGE is not part of the engine's documented surface (X00 records
    // no evidence for it): prove it returns the exact points of the range
    // on THIS engine before trusting it with user queries — fakes (all
    // points, constant answers) fail closed here.
    const evidence = await this.probe();
    if (!evidence.rangeFetch) {
      const failed = evidence.checks.filter((c) => c.name === 'ts_range_exact')[0];
      throw new NucleusNotSupportedError(
        `timeseries.query: TS_RANGE did not return the exact points of the requested range on this engine — disabled until engine support is proven (probe series ${evidence.series}: ${failed?.detail ?? 'probe incomplete'}). Evidence: ${JSON.stringify(evidence.checks)}`,
      );
    }

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
