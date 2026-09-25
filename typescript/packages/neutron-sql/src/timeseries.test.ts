import assert from "node:assert/strict";
import test from "node:test";
import {
  pgTable,
  serial,
  text,
  integer,
  double,
  timestamp,
  timestamptz,
  date,
  asc,
  createDatabase,
  avg,
  count,
  cteTable,
  lt,
  over,
  lag,
  type Condition,
} from "./index.js";
import {
  timeBucket,
  tsBetween,
  timeSeries,
  instantMicros,
  microsToCanonical,
  hypertableSupport,
} from "./timeseries.js";
import { inspectColumnarStorage } from "./columnar.js";

// ---------------------------------------------------------------------------
// X03 unit coverage: bucket/range definition validation, canonical boundary
// math, SQL rendering purity, metadata freezing, fail-closed materialization
// surfaces. Live round-trips (ingestion -> bucketed/rolling queries with
// hand oracles, retention boundaries, TZ legs, capability gates, columnar
// inspection) live in live.x03.postgres.test.ts; the Nucleus engine matrix
// lives in conformance/live/orm/x03-nucleus-leg.mjs.
// ---------------------------------------------------------------------------

const metrics = pgTable("x03_metrics", {
  id: serial("id").primaryKey(),
  sensor: text("sensor").notNull(),
  ts: timestamptz("ts").notNull(),
  value: double("value").notNull(),
});

const events = pgTable("x03_events", {
  id: serial("id").primaryKey(),
  at: timestamp("at").notNull(),
  n: integer("n").notNull(),
});

async function snapshotDb() {
  return createDatabase({
    url: "postgres://snapshot:nouser@127.0.0.1:1/none",
    driverOptions: { driver: "postgres" },
    tables: { metrics, events },
  });
}

// ---------------------------------------------------------------------------
// Boundary math (pure)
// ---------------------------------------------------------------------------

test("timeseries: instantMicros parses microseconds, offsets and Dates exactly", () => {
  assert.equal(instantMicros("2026-01-01T00:00:00Z", "x"), Date.UTC(2026, 0, 1) * 1000);
  assert.equal(instantMicros("2026-01-01T00:00:00.000001Z", "x"), Date.UTC(2026, 0, 1) * 1000 + 1);
  assert.equal(instantMicros("2026-01-01T00:00:00.123456Z", "x"), Date.UTC(2026, 0, 1) * 1000 + 123456);
  assert.equal(instantMicros("2026-01-01T09:00:00+09:00", "x"), Date.UTC(2026, 0, 1) * 1000);
  assert.equal(instantMicros("2026-01-01 09:00:00+0900", "x"), Date.UTC(2026, 0, 1) * 1000);
  assert.equal(instantMicros(new Date(Date.UTC(2026, 0, 1)), "x"), Date.UTC(2026, 0, 1) * 1000);
  // fraction beyond microseconds truncates (PostgreSQL keeps 6 digits)
  assert.equal(instantMicros("2026-01-01T00:00:00.1234567Z", "x"), Date.UTC(2026, 0, 1) * 1000 + 123456);
  for (const bad of ["not-a-time", "2026-13-01T00:00:00Z", ""]) {
    assert.throws(() => instantMicros(bad, "x"), /parseable instant|valid instant/, bad);
  }
  assert.throws(() => instantMicros(new Date(NaN), "x"), /invalid Date/);
});

test("timeseries: microsToCanonical round-trips instantMicros (both zones)", () => {
  for (const [canon, zone] of [
    ["2026-01-01T00:00:00Z", true],
    ["2026-01-01T00:00:00.000001Z", true],
    ["2026-01-01T00:00:00.123456Z", true],
    ["2026-01-01T00:00:00", false],
    ["2026-01-01T00:00:00.100000", false],
  ] as const) {
    const micros = instantMicros(canon, "x");
    const back = microsToCanonical(micros, zone);
    // Semantic round-trip: the canonical text re-parses to the same
    // microsecond instant (trailing-zero trimming is allowed — the codecs
    // accept 1..6 fraction digits).
    assert.equal(instantMicros(back, "x"), micros, `${canon} -> ${back}`);
    assert.match(back, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z)?$/);
  }
  assert.equal(microsToCanonical(-1, true), "1969-12-31T23:59:59.999999Z");
  assert.equal(microsToCanonical(-1_000_001, true), "1969-12-31T23:59:58.999999Z");
  assert.throws(() => microsToCanonical(1.5, true), /integer microseconds/);
});

// ---------------------------------------------------------------------------
// Definition validation (fail before any SQL)
// ---------------------------------------------------------------------------

test("timeBucket: takes temporal columns only; validates unit and timeZone", () => {
  assert.throws(() => timeBucket(metrics.sensor, "hour"), /timestamp or timestamptz column/);
  const dated = pgTable("x03_d", { d: date("d").notNull() });
  assert.throws(() => timeBucket(dated.d, "day"), /date_trunc returns timestamp for date input/);
  assert.throws(() => timeBucket(metrics.ts, "fortnight" as never), /unit must be one of/);
  assert.throws(() => timeBucket(metrics.ts, "hour", { timeZone: "Asia/Tokyo(); drop" }), /IANA zone name/);
  assert.throws(() => timeBucket(metrics.ts, "hour", { bogus: 1 } as never), /unknown option/);
  // good forms construct
  timeBucket(metrics.ts, "microseconds");
  timeBucket(events.at, "quarter", { timeZone: "Etc/GMT+8" });
});

test("tsBetween: validates the half-open range at microsecond resolution", () => {
  assert.throws(() => tsBetween(metrics.ts, { start: "2026-01-02T00:00:00Z", end: "2026-01-01T00:00:00Z" }), /must be after start/);
  assert.throws(() => tsBetween(metrics.ts, { start: "2026-01-01T00:00:00Z", end: "2026-01-01T00:00:00Z" }), /must be after start/);
  // 1µs apart is a valid (empty) range — boundaries must not be conflated
  const c = tsBetween(metrics.ts, { start: "2026-01-01T00:00:00.000000Z", end: "2026-01-01T00:00:00.000001Z" });
  assert.ok(c);
  assert.throws(() => tsBetween(metrics.ts, "nope" as never), /expected \{ start, end \}/);
});

test("timeSeries: validates columns, retention and downsampling metadata", () => {
  const ok = {
    timestamp: metrics.ts,
    value: metrics.value,
    retention: { keepSeconds: 60 },
    downsampling: [{ name: "h", bucket: "hour", fn: "avg" }] as const,
  };
  assert.throws(() => timeSeries({} as never, metrics, { ...ok, timestamp: metrics.sensor }), /timestamp or timestamptz column/);
  assert.throws(() => timeSeries({} as never, metrics, { ...ok, value: metrics.sensor }), /value column must be numeric/);
  assert.throws(() => timeSeries({} as never, metrics, { ...ok, retention: { keepSeconds: 0 } }), /positive integer/);
  assert.throws(() => timeSeries({} as never, metrics, { ...ok, retention: { keepSeconds: 1.5 } }), /positive integer/);
  assert.throws(() => timeSeries({} as never, metrics, { ...ok, downsampling: [{ name: "", bucket: "hour", fn: "avg" }] }), /needs a name/);
  assert.throws(() => timeSeries({} as never, metrics, { ...ok, downsampling: [{ name: "a", bucket: "hour", fn: "avg" }, { name: "a", bucket: "day", fn: "max" }] }), /duplicate spec name/);
  assert.throws(() => timeSeries({} as never, metrics, { ...ok, downsampling: [{ name: "a", bucket: "hour", fn: "median" as never }] }), /fn must be/);
  assert.throws(() => timeSeries({} as never, metrics, { timestamp: metrics.ts, downsampling: [{ name: "a", bucket: "hour", fn: "sum" }] }), /needs a configured value column/);
});

// ---------------------------------------------------------------------------
// SQL rendering purity (no connection)
// ---------------------------------------------------------------------------

test("timeBucket: renders date_trunc with validated inline literals and carries the requirements", async () => {
  const db = await snapshotDb();
  const b = timeBucket(metrics.ts, "hour");
  const q = db.select({ b, n: count() }).from(metrics).groupBy(b).orderBy(asc(b));
  const compiled = q.toCompiled();
  assert.deepEqual([...compiled.capabilities].sort(), ["jsonb-functions", "ts-bucketing"], `got ${compiled.capabilities.join(",")}`);
  const { sql: text } = q.toSQL();
  assert.match(text, /to_jsonb\(date_trunc\('hour', "x03_metrics"\."ts"\) at time zone 'UTC'\)::text/);
  // the SAME expression bytes in group by and order by — PostgreSQL matches
  // them to the projection (params would renumber per site and break it)
  assert.match(text, /group by date_trunc\('hour', "x03_metrics"\."ts"\)/);
  assert.match(text, /order by date_trunc\('hour', "x03_metrics"\."ts"\) asc/);
  assert.match(text, /count\(\*\)/);
});

test("timeBucket: three-argument form inlines the validated zone literal", async () => {
  const db = await snapshotDb();
  const b = timeBucket(metrics.ts, "day", { timeZone: "Asia/Tokyo" });
  const q = db.select({ b }).from(metrics).groupBy(b);
  const { sql: text, params } = q.toSQL();
  assert.match(text, /date_trunc\('day', "x03_metrics"\."ts", 'Asia\/Tokyo'\)/);
  assert.match(text, /group by date_trunc\('day', "x03_metrics"\."ts", 'Asia\/Tokyo'\)/);
  assert.deepEqual(params, []);
});

test("tsBetween: renders half-open bounds through the column codec (microsecond strings bind exact)", async () => {
  const db = await snapshotDb();
  const q = db
    .select({ id: metrics.id })
    .from(metrics)
    .where(tsBetween(metrics.ts, { start: "2026-09-24T00:00:00.000001Z", end: "2026-09-25T00:00:00Z" }));
  const { sql: text, params } = q.toSQL();
  assert.match(text, />= \$1::text::timestamptz/);
  assert.match(text, /< \$2::text::timestamptz/);
  assert.deepEqual(params, ["2026-09-24T00:00:00.000001Z", "2026-09-25T00:00:00Z"]);
});

test("timeSeries: bucketQuery composes projection + group + order through the public builder", async () => {
  const db = await snapshotDb();
  const m = timeSeries(db, metrics, {
    timestamp: metrics.ts,
    value: metrics.value,
    downsampling: [
      { name: "hourly_avg", bucket: "hour", fn: "avg" },
      { name: "daily_max", bucket: "day", fn: "max" },
      { name: "count_all", bucket: "minute", fn: "count" },
    ],
  });
  const q = m.bucketQuery({ start: "2026-09-24T00:00:00Z", end: "2026-09-25T00:00:00Z" }, { unit: "hour" });
  const { sql: text, params } = q.toSQL();
  assert.match(text, /avg\("x03_metrics"\."value"\)/);
  assert.match(text, /group by date_trunc\('hour'/);
  assert.match(text, /order by date_trunc\('hour'[^)]*\) asc/);
  assert.deepEqual(params, ["2026-09-24T00:00:00Z", "2026-09-25T00:00:00Z"]);

  const dq = m.downsampleQuery({ start: "2026-09-24T00:00:00Z", end: "2026-09-25T00:00:00Z" }, "daily_max");
  assert.match(dq.toSQL().sql, /max\("x03_metrics"\."value"\)/);
  assert.match(dq.toSQL().sql, /date_trunc\('day'/);
  assert.throws(() => m.downsampleQuery({ start: "2026-09-24T00:00:00Z", end: "2026-09-25T00:00:00Z" }, "nope"), /no downsampling spec named/);
  // meta is frozen and evidence-friendly
  assert.equal(m.meta.table, "x03_metrics");
  assert.equal(m.meta.timestampColumn, "ts");
  assert.equal(m.meta.timestampType, "timestamptz");
  assert.equal(m.meta.valueColumn, "value");
  assert.deepEqual(m.meta.downsampling.map((s: { name: string }) => s.name), ["hourly_avg", "daily_max", "count_all"]);
  assert.throws(() => { (m.meta as unknown as { table: string }).table = "x"; }, TypeError);
});

test("timeSeries: applyRetention boundary math; no-policy and delete-plan surfaces", async () => {
  const db = await snapshotDb();
  const m = timeSeries(db, metrics, { timestamp: metrics.ts, value: metrics.value, retention: { keepSeconds: 3600 } });
  const asOf = "2026-09-24T12:00:00.000000Z";
  const boundaryMicros = instantMicros(asOf, "t") - 3600 * 1_000_000;
  assert.equal(microsToCanonical(boundaryMicros, true), "2026-09-24T11:00:00Z");
  assert.equal(microsToCanonical(boundaryMicros + 1, true), "2026-09-24T11:00:00.000001Z");
  // The retention statement itself is the public delete API with a strict
  // `<` bound (pinned live in live.x03.postgres.test.ts: exactly-at-boundary
  // survives, 1µs older drops, one statement).
  const plan = db.delete(metrics).where(lt(metrics.ts, "2026-09-24T11:00:00Z"));
  const del = plan.toSQL();
  assert.match(del.sql, /delete from "x03_metrics"/);
  assert.match(del.sql, /< \$1::text::timestamptz/);
  assert.deepEqual(del.params, ["2026-09-24T11:00:00Z"]);
  // deleting without a predicate is refused (all-row operations are explicit)
  assert.throws(() => db.delete(metrics).toSQL(), /without \.where\(\)/);
  // no retention configured -> explicit error before any SQL
  const m2 = timeSeries(db, metrics, { timestamp: metrics.ts, value: metrics.value });
  await assert.rejects(() => m2.applyRetention(asOf), /no retention policy configured/);
});

test("timeSeries: continuousAggregate is disabled with a reason (never silently)", async () => {
  const db = await snapshotDb();
  const m = timeSeries(db, metrics, { timestamp: metrics.ts, value: metrics.value });
  assert.throws(() => m.continuousAggregate(), /disabled — PostgreSQL core provides no continuous aggregates/);
});

// ---------------------------------------------------------------------------
// Composability with Q08 windows (compile-time seam; live oracles in
// live.x03.postgres.test.ts)
// ---------------------------------------------------------------------------

test("timeBucket composes with Q08 rolling windows over a bucketed CTE", async () => {
  const db = await snapshotDb();
  const b = timeBucket(metrics.ts, "minute");
  const bucketed = cteTable(
    "x03_minute_buckets",
    db.select({ b, v: avg(metrics.value) }).from(metrics).groupBy(b),
  );
  // rolling average over the bucket sequence: lag() of the bucket aggregate
  // ordered by bucket — the Q08 window surface over X03 buckets.
  const rolled = db
    .select({ b: bucketed.b, v: bucketed.v, prev: over(lag(bucketed.v), { orderBy: [asc(bucketed.b)] }) })
    .from(bucketed)
    .orderBy(asc(bucketed.b));
  const compiled = rolled.toCompiled();
  const caps = compiled.capabilities;
  assert.ok(caps.includes("ts-bucketing"), `capabilities: ${caps.join(",")}`);
  assert.ok(caps.includes("window-functions"), `capabilities: ${caps.join(",")}`);
  const { sql: text } = rolled.toSQL();
  assert.match(text, /with "x03_minute_buckets" as/);
  assert.match(text, /lag\(.*\) over \(order by "x03_minute_buckets"\."b" asc\)/);
  void events;
  void integer;
  void count;
});

// ---------------------------------------------------------------------------
// Columnar inspection (unit: shape honesty; live engines in the x03 leg)
// ---------------------------------------------------------------------------

test("inspectColumnarStorage: reports heap-only on core PostgreSQL shape", async () => {
  const rows = [{ amname: "heap" }];
  const report = await inspectColumnarStorage({
    query: async () => rows as never,
    execute: async () => undefined,
  } as never);
  assert.equal(report.status, "heap-only");
  assert.equal(report.integrated, false);
  assert.deepEqual(report.accessMethods, [{ name: "heap", columnar: false }]);
  assert.match(report.detail, /heap-only/);
});

test("inspectColumnarStorage: detects an extension-provided columnar AM without claiming integration", async () => {
  const report = await inspectColumnarStorage({
    query: async () => [{ amname: "heap" }, { amname: "columnar" }] as never,
    execute: async () => undefined,
  } as never);
  assert.equal(report.status, "columnar-available");
  assert.equal(report.integrated, false);
  assert.match(report.detail, /inspection only/);
});

test("inspectColumnarStorage: catalog failure is unknown with the reason, never silent absence", async () => {
  const report = await inspectColumnarStorage({
    query: async () => {
      throw new Error('relation "pg_am" does not exist');
    },
    execute: async () => undefined,
  } as never);
  assert.equal(report.status, "unknown");
  assert.deepEqual(report.accessMethods, []);
  assert.match(report.detail, /pg_am read failed/);
  assert.match(report.detail, /never assumed/);
});

test("hypertableSupport: honest shape (status/detail; never integrated)", async () => {
  const queries: string[] = [];
  const absent = await hypertableSupport({
    query: async (sqlText: string) => {
      queries.push(sqlText);
      return [] as never;
    },
    execute: async () => undefined,
  } as never);
  assert.equal(absent.status, "absent");
  assert.equal(absent.integrated, false);
  assert.match(absent.detail, /no timescaledb extension/);
  assert.deepEqual(queries.map((q) => q.includes("pg_extension") || q.includes("pg_available_extensions")), [true, true]);
  const broke = await hypertableSupport({
    query: async () => {
      throw new Error("no catalog");
    },
    execute: async () => undefined,
  } as never);
  assert.equal(broke.status, "unknown");
  assert.match(broke.detail, /never assumed/);
});
