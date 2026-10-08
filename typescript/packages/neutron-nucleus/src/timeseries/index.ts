// ---------------------------------------------------------------------------
// @neutron-build/nucleus/timeseries — Time-Series model plugin (X03)
// ---------------------------------------------------------------------------
// Capability discipline (I01 / X00): the engine's DOCUMENTED surface
// (TS_INSERT / TS_COUNT / TS_LAST / TS_RANGE_COUNT / TS_RANGE_AVG /
// TS_RETENTION — nucleus/docs/MODEL_SEMANTICS.md) is gated by
// requireNucleus like before. Two client methods depend on functions the
// X00 capability report records NO evidence for: TS_RANGE (raw point
// fetch) and TIME_BUCKET. Those are gated by a live SEMANTIC PROBE with
// negative controls, explicitly invoked through probeCapabilities and memoized:
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
import { randomBytes, createHash } from 'node:crypto';

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
  /** Side-effect-free TIME_BUCKET admission; does not insert points. */
  admitPureCapabilities(): Promise<TimeSeriesModelEvidence>;
  /** Qualified in-process handoff to a production connection with the same
   * endpoint/auth scope, live version and feature fingerprint. Never a boolean. */
  admissionProfile(options?: TimeSeriesAdmissionOptions): Promise<TimeSeriesAdmissionProfile>;
  /** Explicit diagnostics: writes four persistent points to a unique probe series.
   * No point-deletion primitive exists. Never called implicitly by a read. */
  probeCapabilities(options: { allowPersistentProbeWrites: true }): Promise<TimeSeriesModelEvidence>;
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
   * no raw point fetch) — gated by explicit probeCapabilities evidence with
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
   * explicit probeCapabilities evidence: TIME_BUCKET must floor an unaligned
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

export interface TimeSeriesAdmissionOptions {
  validForMs?: number;
  /** Required for point-based evidence. Must dispose the owned diagnostic
   * namespace/engine using a genuine backend/administrative lifecycle; closing
   * a connection alone does not remove points. No deletion primitive is supplied. */
  disposeDiagnosticNamespace?: () => Promise<void>;
}

export interface TimeSeriesAdmissionProfile {
  readonly protocol: 'timeseries-admission-v1';
  readonly identity: Readonly<{ endpoint: string; version: string; capabilities: string }>;
  readonly measuredAt: number;
  readonly expiresAt: number;
  readonly evidence: TimeSeriesModelEvidence;
}
// Only evidence measured by this module can mint a profile. JSON or arbitrary
// caller-created objects do not constitute admission. Profiles intentionally
// cannot survive process restart; requalify diagnostics after deployment.
const qualifiedProfiles = new WeakSet<TimeSeriesAdmissionProfile>();

async function admissionIdentity(transport: Transport) {
  const endpoint = await transport.capabilityEndpoint?.();
  if (!endpoint) throw new NucleusNotSupportedError('Diagnostic handoff needs an explicit endpoint/auth identity');
  const version = await transport.fetchval<unknown>('SELECT VERSION()', [], { readOnly: true });
  const raw = await transport.fetchval<unknown>('SELECT NUCLEUS_FEATURES()', [], { readOnly: true });
  if (typeof version !== 'string' || !version.includes('Nucleus') || typeof raw !== 'string')
    throw new NucleusNotSupportedError('Diagnostic identity/version/features could not be verified');
  const features = JSON.parse(raw) as Record<string, unknown>;
  if (!features || Array.isArray(features) || typeof features !== 'object' || features.timeseries !== true)
    throw new NucleusNotSupportedError('Explicit time-series capability identity is required for handoff');
  const canonical = JSON.stringify(Object.entries(features).sort(([a], [b]) => a.localeCompare(b)));
  return Object.freeze({ endpoint, version, capabilities: createHash('sha256').update(canonical).digest('hex') });
}
function freezeEvidence(evidence: TimeSeriesModelEvidence): TimeSeriesModelEvidence {
  return Object.freeze({ ...evidence, checks: Object.freeze(evidence.checks.map(check => Object.freeze({ ...check }))) });
}
async function probePureBuckets(transport: Transport): Promise<TimeSeriesModelEvidence> {
  const checks: TimeSeriesProbeCheck[] = [];
  let bucketFn = false;
  try {
    const b0 = num(await transport.fetchval('SELECT TIME_BUCKET($1, $2)', [5000, 1], { readOnly: true }));
    const b1 = num(await transport.fetchval('SELECT TIME_BUCKET($1, $2)', [5000, 12345], { readOnly: true }));
    const b2 = num(await transport.fetchval('SELECT TIME_BUCKET($1, $2)', [5000, 17345], { readOnly: true }));
    bucketFn = b0 === 0 && b1 !== null && b2 !== null && b1 <= 12345 && 12345 < b1 + 5000 && b2 <= 17345 && 17345 < b2 + 5000 && b1 !== b2;
    checks.push({ name: 'time_bucket_floors', passed: bucketFn, detail: `pure TIME_BUCKET controls: ${b0},${b1},${b2}` });
  } catch (error) {
    checks.push({ name: 'time_bucket_floors', passed: false, detail: `TIME_BUCKET rejected: ${error instanceof Error ? error.message : String(error)}` });
  }
  return freezeEvidence({ series: '', checks, rangeFetch: false, bucketFn });
}

/** Explicit write diagnostics. The caller owns and disposes the diagnostic
 * namespace/engine; there is no engine point-deletion primitive. */
export async function probeTimeSeriesModel(transport: Transport, options: { allowPersistentProbeWrites: true }): Promise<TimeSeriesModelEvidence> {
  if (options?.allowPersistentProbeWrites !== true) throw new Error('Persistent diagnostic write consent required');
  const pure = await probePureBuckets(transport);
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
    return freezeEvidence({ series, checks: [...checks, ...pure.checks], rangeFetch: false, bucketFn: pure.bucketFn });
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

  return freezeEvidence({ series, checks: [...checks, ...pure.checks], rangeFetch, bucketFn: pure.bucketFn });
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

class TimeSeriesModelImpl implements TimeSeriesModel {
  private evidence: Promise<TimeSeriesModelEvidence> | null = null;
  private measuredIdentity: Awaited<ReturnType<typeof admissionIdentity>> | null = null;
  private measuredAt = 0;
  private diagnosticStarted = false;
  private exportedProfile: Promise<TimeSeriesAdmissionProfile> | null = null;

  constructor(
    private readonly transport: Transport,
    private readonly features: NucleusFeatures,
    private readonly profile?: TimeSeriesAdmissionProfile,
  ) {}

  private require(): void {
    requireNucleus(this.features, 'TimeSeries');
  }

  async admitPureCapabilities(): Promise<TimeSeriesModelEvidence> {
    this.require();
    const before = this.transport.capabilityEndpoint ? await admissionIdentity(this.transport) : null;
    const pure = await probePureBuckets(this.transport);
    const after = before ? await admissionIdentity(this.transport) : null;
    if (before && JSON.stringify(before) !== JSON.stringify(after)) throw new NucleusNotSupportedError('Endpoint changed during pure admission');
    this.measuredIdentity = after;
    this.evidence = Promise.resolve(pure);
    this.measuredAt = Date.now();
    return pure;
  }

  async admissionProfile(options: TimeSeriesAdmissionOptions = {}): Promise<TimeSeriesAdmissionProfile> {
    this.require();
    if (this.exportedProfile) return this.exportedProfile;
    if (!this.evidence || !this.measuredIdentity) throw new NucleusNotSupportedError('Qualify diagnostics against endpoint identity before exporting a profile');
    const validForMs = options.validForMs ?? 300_000;
    if (!Number.isInteger(validForMs) || validForMs < 1 || validForMs > 86_400_000) throw new RangeError('validForMs must be 1..86400000');
    const evidence = freezeEvidence(await this.evidence);
    if (evidence.series !== '' && typeof options.disposeDiagnosticNamespace !== 'function')
      throw new NucleusNotSupportedError('Raw-range profile requires genuine owned diagnostic-namespace disposal; connection close alone is insufficient');
    // Memoize terminal disposal: never repeat an ambiguous cleanup or mint a
    // success profile after failed namespace disposal.
    return this.exportedProfile ??= (async () => {
      const identity = await admissionIdentity(this.transport);
      if (JSON.stringify(identity) !== JSON.stringify(this.measuredIdentity)) throw new NucleusNotSupportedError('Diagnostic identity changed during qualification');
      if (evidence.series !== '') await options.disposeDiagnosticNamespace!();
      const profile: TimeSeriesAdmissionProfile = Object.freeze({ protocol: 'timeseries-admission-v1', identity, measuredAt: this.measuredAt, expiresAt: this.measuredAt + validForMs, evidence });
      qualifiedProfiles.add(profile);
      return profile;
    })();
  }

  async probeCapabilities(options: { allowPersistentProbeWrites: true }): Promise<TimeSeriesModelEvidence> {
    this.require();
    if (options?.allowPersistentProbeWrites !== true) throw new Error('Time-series diagnostics require explicit persistent probe-write consent');
    if (!this.diagnosticStarted) {
      this.diagnosticStarted = true;
      this.evidence = (async () => {
        const before = this.transport.capabilityEndpoint ? await admissionIdentity(this.transport) : null;
        const evidence = await probeTimeSeriesModel(this.transport, options);
        const after = before ? await admissionIdentity(this.transport) : null;
        if (before && JSON.stringify(before) !== JSON.stringify(after)) throw new NucleusNotSupportedError('Endpoint identity changed during diagnostics');
        this.measuredIdentity = after;
        this.measuredAt = Date.now();
        return evidence;
      })().catch((err) => {
        this.evidence = null;
        this.diagnosticStarted = false;
        throw err;
      });
    }
    return this.evidence!;
  }

  private async probe(): Promise<TimeSeriesModelEvidence> {
    this.require();
    if (this.profile) {
      if (!qualifiedProfiles.has(this.profile) || !Object.isFrozen(this.profile) || this.profile.protocol !== 'timeseries-admission-v1' ||
          Date.now() < this.profile.measuredAt || Date.now() >= this.profile.expiresAt)
        throw new NucleusNotSupportedError('Time-series profile is unverified or stale');
      const identity = await admissionIdentity(this.transport);
      if (JSON.stringify(identity) !== JSON.stringify(this.profile.identity))
        throw new NucleusNotSupportedError('Time-series profile does not match endpoint/version/capability identity');
      return this.profile.evidence;
    }
    if (!this.evidence) throw new NucleusNotSupportedError('Time-series capability is unverified; call admitPureCapabilities for buckets, or qualify write diagnostics on an owned disposable namespace and pass its admission profile');
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
      ], { readOnly: true })) ?? 0
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
    ], { readOnly: true });
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

/** Add time-series reads admitted by qualified diagnostic evidence. Identity is
 * rechecked with pure queries before every gated production read. */
export function withTimeSeriesProfile(profile: TimeSeriesAdmissionProfile): NucleusPlugin<{ timeseries: TimeSeriesModel }> {
  if (!qualifiedProfiles.has(profile)) throw new NucleusNotSupportedError('Unverified time-series profile');
  return { name: 'timeseries', init: (transport, features) => ({ timeseries: new TimeSeriesModelImpl(transport, features, profile) }) };
}
