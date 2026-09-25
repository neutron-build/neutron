// ---------------------------------------------------------------------------
// @neutron-build/sql/timeseries — optional time-series workflow module (X03)
// ---------------------------------------------------------------------------
// Import as `@neutron-build/sql/timeseries`. The SQL-only root never loads
// this file; importing it is the explicit opt-in to the time-series surface.
//
// Everything here is CORE PostgreSQL — no extension, no server-side
// retention daemon. The module integrates typed time ranges, bucketed
// aggregation and retention/downsampling metadata onto the SQL substrate:
//
//   - `timeBucket(col, unit, { timeZone })` builds date_trunc(field, source
//     [, timezone]) as a STRUCTURAL expression node carrying the
//     "ts-bucketing" I01 capability (probe-resolved with semantic controls,
//     including the timezone argument — see engine.ts). Bucket results
//     follow the lossless temporal wire form: microsecond precision survives
//     the round trip and bucket boundaries are exact instants, not Date-ms
//     approximations. Composes with Q02 group/having and Q08 windows: bucket
//     in a projection + groupBy, or feed rolling aggregates with over().
//   - `tsBetween(col, { start, end })` is the canonical half-open time range
//     [start, end): start included, end excluded. Boundaries bind through
//     the column codec as canonical strings (microsecond exact) or Dates
//     (millisecond exact — documented). Validation compares boundaries at
//     microsecond resolution, so a range ending 1µs before another is not
//     conflated with it.
//   - `timeSeries(db, table, opts)` bundles retention/downsampling METADATA
//     with the workflow: bucketed queries, per-spec downsampling queries and
//     a retention DELETE plan whose boundary is computed from a pinned
//     `asOf` instant (tests) or the server clock (`select now()::text`, read
//     once) and bound as an exact parameter — no interval arithmetic in SQL,
//     no wall-clock nondeterminism inside the statement.
//
// What is deliberately NOT here (fail closed, with reasons, per the card):
// PostgreSQL core has no hypertables, no automatic chunking, no continuous
// aggregates and no retention daemon. `continuousAggregate()` always throws
// with the precise reason; `hypertableSupport(driver)` reports the
// timescaledb extension state honestly (installed / available / absent /
// unknown) and never claims integration that was not proven. Automatic
// downsampling materialization remains disabled everywhere until an engine
// demonstrates it.

import {
  fragment,
  ident as identNode,
  qual as qualNode,
  withRequirements,
  type FragmentNode,
  type FragmentPart,
  type ValueNode,
} from "./ast.js";
import { registerTemporalExpression } from "./codecs.js";
import { and, asc, avg, count, gte, lt, min, max, sum, type Condition } from "./expr.js";
import type { Driver } from "./drivers.js";
import {
  tableRefParts,
  type AnyColumnBuilder,
  type AnyPgTable,
  type ColumnBuilder,
  type ColumnDataType,
  type PgTable,
} from "./schema.js";
import type { NeutronDatabase } from "./db.js";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** date_trunc field units (PostgreSQL's own vocabulary — rendered as bound
 *  parameters, never spliced into SQL text). */
export type BucketUnit =
  | "microseconds"
  | "milliseconds"
  | "second"
  | "minute"
  | "hour"
  | "day"
  | "week"
  | "month"
  | "quarter"
  | "year";

const BUCKET_UNITS: ReadonlySet<string> = new Set([
  "microseconds", "milliseconds", "second", "minute", "hour", "day", "week", "month", "quarter", "year",
]);

/** IANA time zone name for the three-argument date_trunc form. Validated as
 *  zone-shaped (letters, digits, `_`, `-`, `/`, `+` — e.g. "Asia/Tokyo",
 *  "UTC", "Etc/GMT+8"); the server owns real zone lookup errors. */
export type BucketTimeZone = string;

const TIME_ZONE_SHAPE = /^[A-Za-z0-9_+\-/]{1,64}$/;

declare const bucketReadType: unique symbol;

/** A time-bucket expression: date_trunc(...) as a structural node whose
 *  result decodes like its source temporal column (canonical
 *  microsecond-exact string by default). The phantom carries the read type. */
export type TimeBucketExpr<T = string> = FragmentNode & { readonly [bucketReadType]: { readType: T } };

/** Half-open time range [start, end): start included, end excluded — the
 *  canonical time-series range semantics (a row exactly at `end` belongs to
 *  the NEXT range; the retention boundary keeps a row exactly at the cutoff
 *  and drops anything older). Both bounds may be canonical strings
 *  (microsecond exact) or Dates (millisecond exact). */
export interface TimeRange {
  readonly start: string | Date;
  readonly end: string | Date;
}

/** Retention metadata: keep the last `keepSeconds` of data; drop strictly
 *  older rows. Positive integer seconds. */
export interface RetentionPolicy {
  readonly keepSeconds: number;
}

/** Downsampling metadata: one named aggregate over one bucket size. */
export type DownsampleFn = "avg" | "sum" | "min" | "max" | "count";

export interface DownsampleSpec {
  readonly name: string;
  readonly bucket: BucketUnit;
  readonly fn: DownsampleFn;
  readonly timeZone?: BucketTimeZone;
}

export interface TimeSeriesOptions<TCols extends Record<string, AnyColumnBuilder>> {
  /** The timestamp column buckets, ranges and retention act on. Must be a
   *  timestamp or timestamptz column (date columns are rejected: date_trunc
   *  returns timestamp for them, so bucketing a date silently changes the
   *  column type — store a timestamp column instead). */
  readonly timestamp: Extract<TCols[keyof TCols], AnyColumnBuilder>;
  /** The measured value column bucketed aggregates reduce. Numeric column. */
  readonly value?: Extract<TCols[keyof TCols], AnyColumnBuilder>;
  /** Retention metadata (executed only through applyRetention — PostgreSQL
   *  has no retention daemon; nothing deletes data on its own). */
  readonly retention?: RetentionPolicy;
  /** Downsampling metadata (executed on demand through downsampleQuery —
   *  no materialization happens implicitly). */
  readonly downsampling?: readonly DownsampleSpec[];
}

// ---------------------------------------------------------------------------
// Boundary parsing (microsecond resolution)
// ---------------------------------------------------------------------------

const INSTANT_RE = /^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:?\d{2})?$/;

/** Parse an instant (Date or timestamp string, with or without offset) to
 *  epoch MICROSECONDS. Throws on unparseable input — retention boundaries
 *  and range validation must never silently mis-order. */
export function instantMicros(value: string | Date, what: string): number {
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) throw new Error(`${what}: invalid Date`);
    return value.getTime() * 1000;
  }
  if (typeof value !== "string") throw new Error(`${what}: expected a canonical string or Date (got ${typeof value})`);
  const m = INSTANT_RE.exec(value);
  if (!m) {
    throw new Error(`${what}: "${value}" is not a parseable instant (YYYY-MM-DDTHH:MM:SS[.ffffff][Z|±HH:MM])`);
  }
  const [, y, mo, d, h, mi, s, frac, off] = m;
  let offset = "Z";
  if (off !== undefined) {
    offset = off === "Z" ? "Z" : off.length === 5 ? `${off.slice(0, 3)}:${off.slice(3)}` : off;
  }
  // Parse the SECOND-resolution base separately: Date.parse keeps only
  // milliseconds, so folding the fraction into it would double-count.
  const msBase = Date.parse(`${y}-${mo}-${d}T${h}:${mi}:${s}${offset}`);
  if (Number.isNaN(msBase)) throw new Error(`${what}: "${value}" is not a valid instant`);
  const micros = frac === undefined ? 0 : Number(frac.padEnd(6, "0").slice(0, 6));
  return msBase * 1000 + micros;
}

/** Format epoch microseconds as a canonical instant string accepted by the
 *  column codecs (microsecond digits; UTC 'Z' for timestamptz, naive for
 *  timestamp). Second-aligned decomposition keeps negative instants exact
 *  (e.g. -1µs renders as 1969-12-31T23:59:59.999999Z). */
export function microsToCanonical(micros: number, withZone: boolean): string {
  if (!Number.isSafeInteger(micros)) throw new Error("microsToCanonical: expected integer microseconds");
  let frac = micros % 1_000_000;
  let secs = (micros - frac) / 1_000_000;
  if (frac < 0) {
    frac += 1_000_000;
    secs -= 1;
  }
  const base = new Date(secs * 1000).toISOString().slice(0, 19);
  const fracText = frac === 0 ? "" : `.${String(frac).padStart(6, "0").replace(/0+$/, "")}`;
  return withZone ? `${base}${fracText}Z` : `${base}${fracText}`;
}

function requireRange(range: TimeRange): void {
  if (typeof range !== "object" || range === null) throw new Error("time range: expected { start, end }");
  const start = instantMicros(range.start, "time range start");
  const end = instantMicros(range.end, "time range end");
  if (end <= start) {
    throw new Error(`time range: end (${String(range.end)}) must be after start (${String(range.start)}) — half-open [start, end)`);
  }
}

// ---------------------------------------------------------------------------
// timeBucket / tsBetween
// ---------------------------------------------------------------------------

function isColumnBuilder(v: unknown): v is AnyColumnBuilder {
  return typeof v === "object" && v !== null && typeof (v as { columnName?: unknown }).columnName === "string" && (v as { kind?: unknown }).kind === undefined;
}

function requireTemporalColumn(col: AnyColumnBuilder, who: string): void {
  if (col.dataType !== "timestamp" && col.dataType !== "timestamptz") {
    throw new Error(
      `${who}: takes a timestamp or timestamptz column — "${col.columnName}" is ${col.dataType}` +
        (col.dataType === "date"
          ? " (date_trunc returns timestamp for date input, silently changing the column type — store a timestamp column instead)"
          : ""),
    );
  }
}

function columnRef(col: AnyColumnBuilder): ValueNode {
  const owner = (col as { ownerTable?: unknown }).ownerTable;
  if (owner !== undefined && owner !== null) return qualNode(...tableRefParts(owner as AnyPgTable), col.columnName);
  return identNode(col.columnName);
}

type TemporalColumn = ColumnBuilder<"timestamp" | "timestamptz", boolean, boolean, unknown>;

function asTemporalColumn(col: AnyColumnBuilder, who: string): TemporalColumn {
  requireTemporalColumn(col, who);
  return col as unknown as TemporalColumn;
}

/** `date_trunc(unit, col)` or `date_trunc(unit, col, timeZone)` — bucket a
 *  temporal column to UTC-aligned (or named-zone) bucket boundaries. The
 *  result decodes like the source column: canonical microsecond-exact
 *  strings in default mode. Requires the "ts-bucketing" capability
 *  (probe-resolved; unknown or unsupported engines fail closed before any
 *  SQL runs).
 *
 *  The unit and zone render as INLINE LITERALS, not bind parameters — the
 *  same expression must be byte-identical in the projection, GROUP BY and
 *  ORDER BY for PostgreSQL to match them as one expression ($n placeholders
 *  renumber per render site and would break the match, exactly like the
 *  immutable-literal rule for fts index expressions). Both values are
 *  validated against fixed vocabularies (enum units; zone-name shape) and
 *  cannot contain quote characters, so inlining is injection-safe. */
export function timeBucket(col: AnyColumnBuilder, unit: BucketUnit, opts: { timeZone?: BucketTimeZone } = {}): TimeBucketExpr {
  if (!isColumnBuilder(col)) throw new Error("timeBucket: takes a schema column");
  const temporal = asTemporalColumn(col, "timeBucket");
  if (typeof unit !== "string" || !BUCKET_UNITS.has(unit)) {
    throw new Error(`timeBucket: unit must be one of ${[...BUCKET_UNITS].join(", ")} (got ${JSON.stringify(unit)})`);
  }
  if (typeof opts !== "object" || opts === null) throw new Error("timeBucket: opts must be { timeZone? }");
  for (const k of Object.keys(opts)) {
    if (k !== "timeZone") throw new Error(`timeBucket: unknown option "${k}" (known: timeZone)`);
  }
  let zoneLiteral: string | undefined;
  if (opts.timeZone !== undefined) {
    if (typeof opts.timeZone !== "string" || !TIME_ZONE_SHAPE.test(opts.timeZone)) {
      throw new Error(`timeBucket: timeZone must be an IANA zone name like "Asia/Tokyo" (got ${JSON.stringify(opts.timeZone)})`);
    }
    zoneLiteral = `'${opts.timeZone}'`;
  }
  const parts: Array<string | ValueNode> = ["date_trunc('", unit, "', ", columnRef(col)];
  if (zoneLiteral !== undefined) parts.push(", ", zoneLiteral);
  parts.push(")");
  // withRequirements COPIES the node (F01 brand discipline) — register the
  // temporal decode on the FINAL node, or the projection seam would miss it.
  const node = withRequirements(fragment(...(parts as readonly FragmentPart[])), ["ts-bucketing"]);
  registerTemporalExpression(node, col);
  return node as TimeBucketExpr;
}

/** Half-open range predicate `col >= start AND col < end` — the canonical
 *  time-series range [start, end). Both bounds bind through the column
 *  codec (canonical strings keep microseconds; Dates are millisecond
 *  exact). Validated at microsecond resolution: end must be after start. */
export function tsBetween(col: AnyColumnBuilder, range: TimeRange): Condition {
  if (!isColumnBuilder(col)) throw new Error("tsBetween: takes a schema column");
  const temporal = asTemporalColumn(col, "tsBetween");
  requireRange(range);
  return and(gte(temporal, range.start), lt(temporal, range.end));
}

// ---------------------------------------------------------------------------
// timeSeries() workflow — retention/downsampling metadata + typed queries
// ---------------------------------------------------------------------------

/** Frozen metadata of a timeSeries() definition — the evidence-friendly
 *  description of what was integrated (columns, retention, downsampling). */
export interface TimeSeriesMeta {
  readonly table: string;
  readonly timestampColumn: string;
  readonly timestampType: "timestamp" | "timestamptz";
  readonly valueColumn: string | null;
  readonly retention: RetentionPolicy | null;
  readonly downsampling: readonly DownsampleSpec[];
}

export interface BucketQueryOptions {
  readonly unit?: BucketUnit;
  readonly timeZone?: BucketTimeZone;
  readonly fn?: DownsampleFn;
}

/**
 * Integrate a table's time-series workflow with explicit retention and
 * downsampling metadata. Queries compose through the public builder API
 * (this module never renders SQL of its own); retention executes exactly
 * one DELETE (plus one clock read when `asOf` is not pinned).
 *
 *   const metrics = pgTable("metrics", { id: bigserial().primaryKey(), ts: timestamptz().notNull(), value: doublePrecision().notNull() });
 *   const m = timeSeries(db, metrics, {
 *     timestamp: metrics.ts, value: metrics.value,
 *     retention: { keepSeconds: 86400 },
 *     downsampling: [{ name: "hourly_avg", bucket: "hour", fn: "avg" }],
 *   });
 *   await m.bucketQuery({ start: "2026-09-24T00:00:00Z", end: "2026-09-25T00:00:00Z" }, { unit: "hour" });
 *   await m.applyRetention();   // or applyRetention(db-less asOf pinning)
 */
export function timeSeries<TCols extends Record<string, AnyColumnBuilder>>(
  db: NeutronDatabase<Record<string, never>>,
  table: PgTable<TCols>,
  opts: TimeSeriesOptions<TCols>,
) {
  const ts = opts.timestamp as AnyColumnBuilder;
  if (!isColumnBuilder(ts)) throw new Error("timeSeries: timestamp must be a schema column");
  const temporal = asTemporalColumn(ts, "timeSeries timestamp");
  const value = opts.value as AnyColumnBuilder | undefined;
  if (value !== undefined) {
    if (!isColumnBuilder(value)) throw new Error("timeSeries: value must be a schema column");
    const dt: ColumnDataType = value.dataType;
    if (dt !== "double" && dt !== "real" && dt !== "integer" && dt !== "smallint" && dt !== "bigint" && dt !== "numeric" && dt !== "serial") {
      throw new Error(`timeSeries: value column must be numeric — "${value.columnName}" is ${dt}`);
    }
  }
  if (opts.retention !== undefined) {
    if (typeof opts.retention.keepSeconds !== "number" || !Number.isSafeInteger(opts.retention.keepSeconds) || opts.retention.keepSeconds <= 0) {
      throw new Error("timeSeries retention: keepSeconds must be a positive integer");
    }
  }
  const specs = [...(opts.downsampling ?? [])];
  const seen = new Set<string>();
  for (const spec of specs) {
    if (typeof spec.name !== "string" || spec.name === "") throw new Error("timeSeries downsampling: every spec needs a name");
    if (seen.has(spec.name)) throw new Error(`timeSeries downsampling: duplicate spec name "${spec.name}"`);
    seen.add(spec.name);
    if (typeof spec.bucket !== "string" || !BUCKET_UNITS.has(spec.bucket)) throw new Error(`timeSeries downsampling "${spec.name}": invalid bucket unit`);
    if (spec.fn !== "avg" && spec.fn !== "sum" && spec.fn !== "min" && spec.fn !== "max" && spec.fn !== "count") {
      throw new Error(`timeSeries downsampling "${spec.name}": fn must be avg|sum|min|max|count`);
    }
    if (spec.fn !== "count" && value === undefined) {
      throw new Error(`timeSeries downsampling "${spec.name}": fn "${spec.fn}" needs a configured value column`);
    }
  }
  const tableName = tableRefParts(table).join(".");
  const meta: TimeSeriesMeta = Object.freeze({
    table: tableName,
    timestampColumn: ts.columnName,
    timestampType: temporal.dataType as "timestamp" | "timestamptz",
    valueColumn: value?.columnName ?? null,
    retention: opts.retention === undefined ? null : Object.freeze({ keepSeconds: opts.retention.keepSeconds }),
    downsampling: Object.freeze(specs.map((s) => Object.freeze({ ...s }))),
  });

  function bucketFor(unit: BucketUnit, timeZone: BucketTimeZone | undefined): TimeBucketExpr {
    return timeBucket(ts, unit, timeZone === undefined ? {} : { timeZone });
  }

  function aggregateNode(fn: DownsampleFn) {
    if (value === undefined || fn === "count") return count();
    switch (fn) {
      case "sum":
        return sum(value as ColumnBuilder<"double", boolean, boolean, unknown>);
      case "min":
        return min(value as ColumnBuilder<"double", boolean, boolean, unknown>);
      case "max":
        return max(value as ColumnBuilder<"double", boolean, boolean, unknown>);
      default:
        return avg(value as ColumnBuilder<"double", boolean, boolean, unknown>);
    }
  }

  function bucketedSelect(range: TimeRange, unit: BucketUnit, timeZone: BucketTimeZone | undefined, fn: DownsampleFn) {
    const bucket = bucketFor(unit, timeZone);
    const agg = aggregateNode(fn);
    if (value === undefined || fn === "count") {
      return db
        .select({ bucket, n: agg })
        .from(table as PgTable<Record<string, AnyColumnBuilder>>)
        .where(tsBetween(ts, range))
        .groupBy(bucket)
        .orderBy(asc(bucket));
    }
    return db
      .select({ bucket, value: agg })
      .from(table as PgTable<Record<string, AnyColumnBuilder>>)
      .where(tsBetween(ts, range))
      .groupBy(bucket)
      .orderBy(asc(bucket));
  }

  return {
    /** Frozen metadata (columns + retention + downsampling specs). */
    meta,
    /** A fresh bucket expression for this definition's timestamp column. */
    bucket: (unit: BucketUnit, opts2: { timeZone?: BucketTimeZone } = {}): TimeBucketExpr => bucketFor(unit, opts2.timeZone),
    /** The half-open range predicate on this definition's timestamp column. */
    range: (range: TimeRange): Condition => tsBetween(ts, range),
    /** Bucketed aggregate query over [start, end): one row per non-empty
     *  bucket, ordered by bucket ascending. fn defaults to "avg" (or
     *  "count" when no value column is configured). Empty buckets produce
     *  NO row (GROUP BY semantics — this module does not gap-fill; compose
     *  generate_series yourself if you need gaps). */
    bucketQuery: (range: TimeRange, opts2: BucketQueryOptions = {}) => {
      const unit = opts2.unit ?? "hour";
      const fn = opts2.fn ?? (value === undefined ? "count" : "avg");
      if (fn !== "count" && value === undefined) throw new Error(`bucketQuery fn "${fn}" needs a configured value column`);
      return bucketedSelect(range, unit, opts2.timeZone, fn);
    },
    /** Downsampling query for one configured spec (by name). */
    downsampleQuery: (range: TimeRange, specName: string) => {
      const spec = specs.find((s) => s.name === specName);
      if (spec === undefined) {
        throw new Error(`timeSeries "${tableName}": no downsampling spec named ${JSON.stringify(specName)} (configured: ${specs.map((s) => s.name).join(", ") || "none"})`);
      }
      return bucketedSelect(range, spec.bucket, spec.timeZone, spec.fn);
    },
    /** Execute the retention policy: DELETE rows with timestamp strictly
     *  older than the boundary (asOf - keepSeconds). A row exactly AT the
     *  boundary survives; anything 1µs older is dropped. With `asOf` pinned
     *  (canonical string or Date) this is exactly ONE statement; without it
     *  the server clock is read once (`select now()::text`) and the
     *  boundary is bound as an exact parameter. PostgreSQL has no retention
     *  daemon: retention happens only when this runs. */
    applyRetention: async (asOf?: string | Date): Promise<{ boundary: string; deleted: number }> => {
      if (meta.retention === null) throw new Error(`timeSeries "${tableName}": no retention policy configured`);
      let nowMicros: number;
      if (asOf !== undefined) {
        nowMicros = instantMicros(asOf, "applyRetention asOf");
      } else {
        const rows = await db.driver.query<{ now: unknown }>("select now()::text as now");
        const raw = rows[0]?.now;
        if (typeof raw !== "string" || raw === "") throw new Error("applyRetention: server clock read returned no text");
        nowMicros = instantMicros(raw.replace(" ", "T").replace(/([+-]\d{2})$/, "$1:00"), "applyRetention now()");
      }
      const boundaryMicros = nowMicros - meta.retention.keepSeconds * 1_000_000;
      const boundary = microsToCanonical(boundaryMicros, temporal.dataType === "timestamptz");
      // Strictly older than the boundary: a row exactly AT the boundary
      // survives (half-open retention semantics, mirroring tsBetween).
      const deleted = await db.delete(table as PgTable<Record<string, AnyColumnBuilder>>).where(lt(temporal, boundary));
      return { boundary, deleted };
    },
    /** Continuous aggregates / automatic downsampling materialization —
     *  DISABLED WITH A REASON: PostgreSQL core provides no continuous
     *  aggregates and no engine support is proven here. Always throws;
     *  run downsampleQuery() on demand instead, and check
     *  hypertableSupport(driver) before considering a timescaledb
     *  integration (not integrated, not proven by this module). */
    continuousAggregate: (): never => {
      throw new Error(
        "continuousAggregate: disabled — PostgreSQL core provides no continuous aggregates or automatic downsampling materialization, " +
          "and no engine support for them is proven here. Run downsampleQuery() on demand (exact, one statement), or check " +
          "hypertableSupport(driver) for the timescaledb extension state (integration out of scope, unproven).",
      );
    },
  };
}

/** Inferred workflow type (the factory's return type carries full builder
 *  typing; the alias keeps public declarations readable). */
export type TimeSeriesWorkflow = ReturnType<typeof timeSeries>;

// ---------------------------------------------------------------------------
// Hypertable / extension support — honest reporting, never a claim
// ---------------------------------------------------------------------------

export interface HypertableSupport {
  /** "installed" — the timescaledb extension is installed in this database.
   *  "available" — not installed, but the server could install it.
   *  "absent" — the server reports no such extension. "unknown" — the
   *  catalogs could not be read (e.g. engines without them). */
  readonly status: "installed" | "available" | "absent" | "unknown";
  readonly version: string | null;
  readonly availableVersion: string | null;
  readonly detail: string;
  /** Always false here: this module integrates NO hypertable DDL or
   *  continuous-aggregate planning, proven or otherwise. */
  readonly integrated: false;
}

/** Report the timescaledb extension state honestly (pg_extension /
 *  pg_available_extensions — the same catalogs as the pgvector gate). This
 *  NEVER implies the ORM operates on hypertables: `integrated` is false
 *  until an engine integration is actually built and proven. Catalog read
 *  failures (engines without these catalogs) report `unknown` with the
 *  server's reason, never a silent "absent". */
export async function hypertableSupport(driver: Driver): Promise<HypertableSupport> {
  try {
    const installed = await driver.query<{ version: string }>(
      "select extversion as version from pg_extension where extname = 'timescaledb'",
    );
    if (installed.length > 0) {
      return {
        status: "installed",
        version: installed[0].version,
        availableVersion: null,
        detail: "timescaledb is installed in this database — but this module integrates no hypertable DDL or continuous aggregates (integrated: false); plain-table workflows only",
        integrated: false,
      };
    }
  } catch (err) {
    return {
      status: "unknown",
      version: null,
      availableVersion: null,
      detail: `pg_extension read failed (${err instanceof Error ? err.message : String(err)}) — extension state unknown, never assumed`,
      integrated: false,
    };
  }
  try {
    const available = await driver.query<{ default_version: string }>(
      "select default_version from pg_available_extensions where name = 'timescaledb'",
    );
    if (available.length > 0) {
      return {
        status: "available",
        version: null,
        availableVersion: available[0].default_version,
        detail: "timescaledb is not installed but the server can install it (`create extension timescaledb`) — integration remains out of scope and unproven here",
        integrated: false,
      };
    }
    return {
      status: "absent",
      version: null,
      availableVersion: null,
      detail: "the server reports no timescaledb extension — hypertables, automatic chunking and continuous aggregates are unavailable on this engine; this module's plain-table workflows are core PostgreSQL",
      integrated: false,
    };
  } catch (err) {
    return {
      status: "unknown",
      version: null,
      availableVersion: null,
      detail: `pg_available_extensions read failed (${err instanceof Error ? err.message : String(err)}) — extension state unknown, never assumed`,
      integrated: false,
    };
  }
}
