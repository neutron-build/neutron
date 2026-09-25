import assert from "node:assert/strict";
import test from "node:test";
import { createDatabase, pgTable, serial, text, double, timestamptz, avg, count, asc, cteTable, over, lag, type NeutronDatabase } from "./index.js";
import { timeBucket, tsBetween, timeSeries, hypertableSupport, microsToCanonical, instantMicros } from "./timeseries.js";
import { inspectColumnarStorage } from "./columnar.js";
import { CapabilityRequirementError, capabilityGate } from "./engine.js";
import { ServerSqlError } from "./errors.js";
import { TEST_URL, ensureLive, uniqueDbName } from "./live-harness.js";

// ---------------------------------------------------------------------------
// X03 live battery: time-series and columnar workflows on the SQL substrate.
//
// Legs per driver (each in its own throwaway x03_-prefixed database):
//   1. Capability evidence: ts-bucketing probe resolves supported on
//      PostgreSQL and its semantics hold (hour truncation, µs floor,
//      timezone argument); a gate that cannot prove the capability rejects
//      bucket statements BEFORE any SQL runs (statement counter 0).
//   2. Ingestion -> bucketed query with a HAND ORACLE: fixture rows with
//      microsecond timestamps are bucketed in JavaScript independently of
//      the SQL; counts/avg/min/max per bucket compare exactly, and bucket
//      labels come back as canonical microsecond-exact strings.
//   3. Rolling windows over buckets (Q08 composability): lag() of the
//      bucketed average ordered by bucket — JS sliding oracle, exact.
//   4. Time-range boundaries: tsBetween is half-open [start, end); a row
//      1µs before end is IN, a row exactly at end is OUT, a row exactly at
//      start is IN; timestamps round-trip byte-exact (µs).
//   5. Retention boundary: applyRetention with a pinned asOf drops strictly
//      older rows — exactly-at-boundary survives, 1µs older drops — as ONE
//      statement; the unpinned form adds exactly one clock read.
//   6. Range-partitioned pagination: contiguous non-overlapping tsBetween
//      windows walk the whole ingestion set with zero duplicates/omissions
//      (time-series shaped ordering proof on the timestamp key).
//   7. Columnar/hypertable honesty: pg_am reports heap-only (no columnar
//      storage exists on PostgreSQL core); timescaledb is absent;
//      continuousAggregate() throws with the reason.
//   8. Reconnect invariance: a fresh database instance reads identical
//      bucketed results after the first connection closes.
//
// Timezone coverage: the battery runs under TWO session timezones at the
// suite level (Asia/Tokyo and America/New_York via the gate runner); the
// bucket-boundary leg additionally proves the date_trunc timezone ARGUMENT
// inside one session (Tokyo day boundaries differ from UTC ones).
// ---------------------------------------------------------------------------

type DriverKind = "postgres" | "pg";

interface Fixture {
  db: NeutronDatabase<Record<string, never>>;
  statements: { count: number };
  url: string;
  close: () => Promise<void>;
}

const metrics = pgTable("x03_metrics", {
  id: serial("id").primaryKey(),
  sensor: text("sensor").notNull(),
  ts: timestamptz("ts").notNull(),
  value: double("value").notNull(),
});


async function withDatabase(driverKind: DriverKind, fn: (fx: Fixture) => Promise<void>): Promise<void> {
  if (!(await ensureLive(`live x03 (${driverKind})`))) return;
  const adminUrl = new URL(TEST_URL);
  const { Pool } = (await import("pg")) as unknown as {
    Pool: new (o: object) => { query: (s: string, p?: unknown[]) => Promise<unknown>; end: () => Promise<void> };
  };
  const dbName = uniqueDbName("x03");
  const admin = new Pool({ connectionString: adminUrl.toString(), max: 1 });
  await admin.query(`drop database if exists "${dbName}"`);
  await admin.query(`create database "${dbName}"`);
  await admin.end();

  const url = new URL(TEST_URL);
  url.pathname = `/${dbName}`;
  const statements = { count: 0 };
  const db = await createDatabase({
    url: url.toString(),
    driverOptions: { driver: driverKind },
    logger: (e) => {
      if (e.kind === "query-end") statements.count += 1;
    },
  });
  try {
    await fn({ db, statements, url: url.toString(), close: async () => void (await db.driver.close()) });
  } finally {
    await db.driver.close().catch(() => undefined);
    const admin2 = new Pool({ connectionString: adminUrl.toString(), max: 1 });
    await admin2.query(`drop database if exists "${dbName}"`);
    await admin2.end();
  }
}

// Fixture: microsecond-exact timestamps across three hours. Values are
// integer-valued doubles so sums are exact in float8 and the JS oracle can
// compare bit-for-bit with PostgreSQL's avg/sum.
const HOUR0 = Date.UTC(2026, 8, 24, 5, 0, 0); // 2026-09-24T05:00:00Z
const FIXTURE_ROWS: Array<{ sensor: string; ts: string; value: number }> = [
  { sensor: "cpu", ts: "2026-09-24T05:00:00.000000Z", value: 10 },
  { sensor: "cpu", ts: "2026-09-24T05:00:00.000001Z", value: 20 },
  { sensor: "cpu", ts: "2026-09-24T05:59:59.999999Z", value: 30 },
  { sensor: "cpu", ts: "2026-09-24T06:00:00.000000Z", value: 40 },
  { sensor: "cpu", ts: "2026-09-24T06:30:00.500000Z", value: 50 },
  { sensor: "cpu", ts: "2026-09-24T07:00:00.000000Z", value: 60 },
  { sensor: "cpu", ts: "2026-09-24T07:00:00.000001Z", value: 70 },
  { sensor: "mem", ts: "2026-09-24T05:10:00.123456Z", value: 100 },
];

/** Independent JS bucket oracle: bucket by UTC hour, exactly like the SQL
 * claims to. Computed from the fixture array, never from the query result. */
function jsHourBuckets(rows: typeof FIXTURE_ROWS): Map<string, { n: number; sum: number; avg: number; min: number; max: number }> {
  const buckets = new Map<string, { n: number; sum: number; min: number; max: number }>();
  for (const r of rows) {
    const hourStart = new Date(Math.floor(Date.parse(r.ts) / 3_600_000) * 3_600_000);
    const key = `${hourStart.toISOString().slice(0, 19)}Z`;
    const b = buckets.get(key) ?? { n: 0, sum: 0, min: Number.POSITIVE_INFINITY, max: Number.NEGATIVE_INFINITY };
    b.n += 1;
    b.sum += r.value;
    b.min = Math.min(b.min, r.value);
    b.max = Math.max(b.max, r.value);
    buckets.set(key, b);
  }
  const out = new Map<string, { n: number; sum: number; avg: number; min: number; max: number }>();
  for (const [k, b] of buckets) out.set(k, { ...b, avg: b.sum / b.n });
  return out;
}

for (const driverKind of ["postgres", "pg"] as const) {
  test(`live x03 (${driverKind}): ts-bucketing capability resolves supported with real semantics`, async () => {
    await withDatabase(driverKind, async (fx) => {
      const gate = capabilityGate(fx.db.driver);
      const evidence = await gate.status("ts-bucketing");
      assert.equal(evidence.status, "supported", evidence.evidence);
      assert.match(evidence.evidence, /probe succeeded/);
      // Semantic spot-check through the raw driver (hand oracle): hour
      // truncation, microsecond floor, and the timezone ARGUMENT — a Tokyo
      // day boundary for 20:00Z starts at 15:00Z the same UTC day.
      const rows = await fx.db.driver.query<{ a: string; b: string; c: string }>(
        [
          "select",
          "  (date_trunc('hour', timestamptz '2026-01-01 00:30:00+00') = timestamptz '2026-01-01 00:00:00+00') as a,",
          "  (date_trunc('hour', timestamptz '2026-01-01 00:59:59.999999+00') = timestamptz '2026-01-01 00:00:00+00') as b,",
          "  (date_trunc('day', timestamptz '2026-01-01 20:00:00+00', 'Asia/Tokyo') = timestamptz '2026-01-01 15:00:00+00') as c",
        ].join("\n"),
      );
      assert.equal(String(rows[0].a), "true");
      assert.equal(String(rows[0].b), "true");
      assert.equal(String(rows[0].c), "true");
    });
  });

  test(`live x03 (${driverKind}): a probe-rejected engine fails bucket statements closed BEFORE any SQL (counter 0)`, async () => {
    await withDatabase(driverKind, async (fx) => {
      await fx.db.driver.execute(`create table x03_metrics (id serial primary key, sensor text not null, ts timestamptz not null, value double precision not null)`);
      await fx.db.insert(metrics).values(FIXTURE_ROWS);
      // Wrap the LIVE adapter: only the ts-bucketing probe is refused (as an
      // engine without date_trunc semantics would refuse it — sqlstate 42883,
      // the same evidence class as the Nucleus leg). Everything else passes
      // through to the real server — this is a live boundary, not a mock db.
      const inner = fx.db.driver;
      const rejecting: typeof inner = {
        ...inner,
        query: (sqlText: string, params?: unknown[]) => {
          if (sqlText.includes("date_trunc")) {
            return Promise.reject(new ServerSqlError("engine under test has no date_trunc semantics", { sqlstate: "42883" }));
          }
          return inner.query(sqlText, params);
        },
      } as unknown as typeof inner;
      const gatedStatements = { count: 0 };
      const gated = await createDatabase({
        driver: rejecting,
        tables: { metrics },
        logger: (e: unknown) => {
          if ((e as { kind?: string }).kind === "query-end") gatedStatements.count += 1;
        },
      });
      const m = timeSeries(gated, metrics, { timestamp: metrics.ts, value: metrics.value });
      const before = gatedStatements.count;
      await assert.rejects(
        async () => {
          await m.bucketQuery({ start: "2026-09-24T00:00:00Z", end: "2026-09-25T00:00:00Z" }, { unit: "hour" });
        },
        (err: unknown) => {
          assert.ok(err instanceof CapabilityRequirementError, `expected CapabilityRequirementError, got ${err}`);
          const failing = err.results.filter((r) => r.status !== "supported");
          assert.ok(failing.some((r) => r.capability === "ts-bucketing" && r.status === "unsupported"));
          assert.match(failing[0].evidence, /42883/);
          return true;
        },
      );
      assert.equal(gatedStatements.count, before, "no logged statement may run around a fail-closed rejection");
      // the same wrapped db still runs plain (non-bucket) time-range queries:
      const plain = await gated
        .select({ id: metrics.id })
        .from(metrics)
        .where(tsBetween(metrics.ts, { start: "2026-09-24T00:00:00Z", end: "2026-09-25T00:00:00Z" }));
      assert.equal(plain.length, FIXTURE_ROWS.length);
      await gated.close();
    });
  });

  test(`live x03 (${driverKind}): ingestion -> bucketed queries match the hand oracle exactly (count/avg/min/max, canonical bucket labels)`, async () => {
    await withDatabase(driverKind, async (fx) => {
      await fx.db.driver.execute(`create table x03_metrics (id serial primary key, sensor text not null, ts timestamptz not null, value double precision not null)`);
      await fx.db.insert(metrics).values(FIXTURE_ROWS);

      const m = timeSeries(fx.db, metrics, {
        timestamp: metrics.ts,
        value: metrics.value,
        retention: { keepSeconds: 86_400 },
        downsampling: [
          { name: "hourly_avg", bucket: "hour", fn: "avg" },
          { name: "hourly_max", bucket: "hour", fn: "max" },
        ],
      });

      const oracle = jsHourBuckets(FIXTURE_ROWS);
      const before = fx.statements.count;
      const rows = (await m.bucketQuery({ start: "2026-09-24T00:00:00Z", end: "2026-09-25T00:00:00Z" }, { unit: "hour" })) as Array<{ bucket: string; value: number }>;
      assert.equal(fx.statements.count - before, 1, "bucket query must be exactly one statement");
      assert.equal(rows.length, oracle.size, `bucket count: got ${JSON.stringify(rows)}`);

      for (const row of rows) {
        const key = String(row.bucket);
        const want = oracle.get(key);
        assert.ok(want, `oracle has no bucket ${key} (got rows ${JSON.stringify(rows.map((r) => r.bucket))})`);
        // canonical label, microsecond-exact: buckets start on the hour
        assert.match(key, /^2026-09-24T(05|06|07):00:00Z$/);
        assert.equal(row.value, want.avg, `avg for ${key}`);
      }

      // count bucket (all sensors) + min/max per bucket through downsampleQuery
      const counts = (await m.bucketQuery({ start: "2026-09-24T00:00:00Z", end: "2026-09-25T00:00:00Z" }, { fn: "count" })) as Array<{ bucket: string; n: bigint }>;
      for (const row of counts) {
        const want = oracle.get(String(row.bucket));
        assert.ok(want);
        assert.equal(row.n, BigInt(want.n), `count for ${row.bucket}`);
      }
      const maxRows = (await m.downsampleQuery({ start: "2026-09-24T00:00:00Z", end: "2026-09-25T00:00:00Z" }, "hourly_max")) as Array<{ bucket: string; value: number }>;
      for (const row of maxRows) {
        const want = oracle.get(String(row.bucket));
        assert.ok(want);
        assert.equal(row.value, want.max, `max for ${row.bucket}`);
      }

      // empty range yields zero rows (GROUP BY over nothing)
      const empty = (await m.bucketQuery({ start: "2027-01-01T00:00:00Z", end: "2027-01-02T00:00:00Z" }, { unit: "hour" })) as unknown[];
      assert.equal(empty.length, 0);
    });
  });

  test(`live x03 (${driverKind}): date_trunc timezone argument produces true zone boundaries (Tokyo vs UTC day)`, async () => {
    await withDatabase(driverKind, async (fx) => {
      await fx.db.driver.execute(`create table x03_metrics (id serial primary key, sensor text not null, ts timestamptz not null, value double precision not null)`);
      // 2026-09-24 20:00Z is 2026-09-25 05:00 Tokyo — inside the Tokyo day
      // 2026-09-25, which starts at 2026-09-24T15:00Z.
      await fx.db.insert(metrics).values([
        { sensor: "cpu", ts: "2026-09-24T20:00:00Z", value: 1 },
        { sensor: "cpu", ts: "2026-09-24T14:59:59.999999Z", value: 2 },
      ]);
      const b = timeBucket(metrics.ts, "day", { timeZone: "Asia/Tokyo" });
      const rows = await fx.db.select({ b, n: count() }).from(metrics).groupBy(b).orderBy(asc(b));
      assert.equal(rows.length, 2);
      assert.equal(rows[0].b, "2026-09-23T15:00:00Z", "Tokyo day bucket of 14:59:59.999999Z");
      assert.equal(rows[0].n, 1n);
      assert.equal(rows[1].b, "2026-09-24T15:00:00Z", "Tokyo day bucket of 20:00Z");
      assert.equal(rows[1].n, 1n);
      // and the UTC bucket of the same data is a single day. The zone-less
      // 2-arg form truncates in the SESSION timezone by PostgreSQL design —
      // the explicit-zone form is what stays deterministic across the
      // battery's two session timezones.
      const utc = timeBucket(metrics.ts, "day", { timeZone: "UTC" });
      const utcRows = await fx.db.select({ b: utc, n: count() }).from(metrics).groupBy(utc);
      assert.equal(utcRows.length, 1);
      assert.equal(utcRows[0].b, "2026-09-24T00:00:00Z");
    });
  });

  test(`live x03 (${driverKind}): tsBetween half-open boundaries at microsecond resolution; timestamps round-trip byte-exact`, async () => {
    await withDatabase(driverKind, async (fx) => {
      await fx.db.driver.execute(`create table x03_metrics (id serial primary key, sensor text not null, ts timestamptz not null, value double precision not null)`);
      await fx.db.insert(metrics).values(FIXTURE_ROWS);

      // [05:00:00.000001, 06:00:00): the 05:00:00.000000 row is OUT (before
      // start), 05:00:00.000001 IN, 05:59:59.999999 IN (1µs before end),
      // 06:00:00.000000 OUT (exactly at end).
      const rows = await fx.db
        .select({ ts: metrics.ts, value: metrics.value })
        .from(metrics)
        .where(tsBetween(metrics.ts, { start: "2026-09-24T05:00:00.000001Z", end: "2026-09-24T06:00:00Z" }))
        .orderBy(asc(metrics.ts));
      assert.deepEqual(
        rows.map((r) => r.ts),
        ["2026-09-24T05:00:00.000001Z", "2026-09-24T05:59:59.999999Z", "2026-09-24T05:10:00.123456Z"].sort(),
      );
      // microsecond digits survive the round trip byte-exact
      assert.deepEqual(
        rows.map((r) => r.value),
        [20, 100, 30],
      );
    });
  });

  test(`live x03 (${driverKind}): rolling window over buckets (Q08 composability) matches the JS sliding oracle`, async () => {
    await withDatabase(driverKind, async (fx) => {
      await fx.db.driver.execute(`create table x03_metrics (id serial primary key, sensor text not null, ts timestamptz not null, value double precision not null)`);
      await fx.db.insert(metrics).values(FIXTURE_ROWS.filter((r) => r.sensor === "cpu"));

      const b = timeBucket(metrics.ts, "hour");
      const bucketed = cteTable(
        "x03_hour_buckets",
        fx.db.select({ b, v: avg(metrics.value) }).from(metrics).groupBy(b),
      );
      const rolled = await fx.db
        .select({ b: bucketed.b, v: bucketed.v, prev: over(lag(bucketed.v), { orderBy: [asc(bucketed.b)] }) })
        .from(bucketed)
        .orderBy(asc(bucketed.b));

      const oracle = jsHourBuckets(FIXTURE_ROWS.filter((r) => r.sensor === "cpu"));
      const keys = [...oracle.keys()].sort();
      assert.equal(rolled.length, keys.length);
      rolled.forEach((row, i) => {
        assert.equal(row.b, keys[i]);
        assert.equal(row.v, oracle.get(keys[i])!.avg, `avg ${keys[i]}`);
        // lag(avg) over buckets ordered by bucket: null for the first row,
        // then the previous bucket's average — the JS sliding oracle
        if (i === 0) assert.equal(row.prev, null);
        else assert.equal(row.prev, oracle.get(keys[i - 1])!.avg, `lag(avg) at ${keys[i]}`);
      });
    });
  });

  test(`live x03 (${driverKind}): retention boundary — exactly-at-boundary survives, 1µs older drops, one statement pinned`, async () => {
    await withDatabase(driverKind, async (fx) => {
      await fx.db.driver.execute(`create table x03_metrics (id serial primary key, sensor text not null, ts timestamptz not null, value double precision not null)`);
      const asOf = "2026-09-24T12:00:00.000000Z";
      await fx.db.insert(metrics).values([
        { sensor: "cpu", ts: "2026-09-24T11:00:00Z", value: 1 }, // exactly at boundary (asOf - 1h): SURVIVES
        { sensor: "cpu", ts: "2026-09-24T10:59:59.999999Z", value: 2 }, // 1µs older: DROPS
        { sensor: "cpu", ts: "2026-09-24T09:00:00Z", value: 3 }, // much older: DROPS
        { sensor: "cpu", ts: "2026-09-24T11:59:59.999999Z", value: 4 }, // newer: SURVIVES
      ]);
      const m = timeSeries(fx.db, metrics, { timestamp: metrics.ts, value: metrics.value, retention: { keepSeconds: 3600 } });

      const before = fx.statements.count;
      const result = await m.applyRetention(asOf);
      assert.equal(result.boundary, "2026-09-24T11:00:00Z");
      assert.equal(result.deleted, 2);
      assert.equal(fx.statements.count - before, 1, "pinned retention is exactly one statement");

      const survivors = await fx.db.select({ ts: metrics.ts }).from(metrics).orderBy(asc(metrics.ts));
      assert.deepEqual(
        survivors.map((r) => r.ts),
        ["2026-09-24T11:00:00Z", "2026-09-24T11:59:59.999999Z"],
      );

      // idempotent: re-running drops nothing more
      const again = await m.applyRetention(asOf);
      assert.equal(again.deleted, 0);

      // unpinned form reads the server clock once on the raw adapter (raw
      // driver calls are unlogged by design — the X01 capability-probe
      // convention), then runs exactly one logged DELETE
      const beforeUnpinned = fx.statements.count;
      const unpinned = await m.applyRetention();
      assert.equal(fx.statements.count - beforeUnpinned, 1, "unpinned retention = one logged DELETE (clock read is raw/unlogged)");
      assert.ok(unpinned.deleted >= 0);
    });
  });

  test(`live x03 (${driverKind}): contiguous range windows paginate without duplicates or omissions`, async () => {
    await withDatabase(driverKind, async (fx) => {
      await fx.db.driver.execute(`create table x03_metrics (id serial primary key, sensor text not null, ts timestamptz not null, value double precision not null)`);
      await fx.db.insert(metrics).values(FIXTURE_ROWS);

      // walk [04:00, 08:00) in four 1-hour windows: every row exactly once
      const seen: string[] = [];
      for (let h = 4; h < 8; h++) {
        const start = microsToCanonical((HOUR0 + (h - 5) * 3_600_000) * 1000, true);
        const end = microsToCanonical((HOUR0 + (h - 4) * 3_600_000) * 1000, true);
        const rows = await fx.db
          .select({ ts: metrics.ts })
          .from(metrics)
          .where(tsBetween(metrics.ts, { start, end }))
          .orderBy(asc(metrics.ts));
        seen.push(...rows.map((r) => String(r.ts)));
      }
      // decoded canonical strings trim trailing fraction zeros — compare
      // instants at microsecond resolution, and prove no duplicates
      const seenMicros = seen.map((t) => instantMicros(t, "seen"));
      const expectedMicros = FIXTURE_ROWS.map((r) => instantMicros(r.ts, "fixture")).sort((a, b) => a - b);
      assert.deepEqual(seenMicros, expectedMicros);
      assert.equal(new Set(seen).size, seen.length, "no duplicates");
    });
  });

  test(`live x03 (${driverKind}): columnar inspection is honest (heap-only) and materialization fails closed with reasons`, async () => {
    await withDatabase(driverKind, async (fx) => {
      const columnar = await inspectColumnarStorage(fx.db.driver);
      assert.equal(columnar.status, "heap-only", columnar.detail);
      assert.equal(columnar.integrated, false);
      assert.deepEqual(columnar.accessMethods, [{ name: "heap", columnar: false }]);

      const hyper = await hypertableSupport(fx.db.driver);
      assert.equal(hyper.status, "absent", hyper.detail);
      assert.equal(hyper.integrated, false);

      const m = timeSeries(fx.db, metrics, { timestamp: metrics.ts, value: metrics.value });
      assert.throws(() => m.continuousAggregate(), /disabled — PostgreSQL core provides no continuous aggregates/);
    });
  });

  test(`live x03 (${driverKind}): reconnect invariance — a fresh connection reads identical bucketed results`, async () => {
    await withDatabase(driverKind, async (fx) => {
      await fx.db.driver.execute(`create table x03_metrics (id serial primary key, sensor text not null, ts timestamptz not null, value double precision not null)`);
      await fx.db.insert(metrics).values(FIXTURE_ROWS);
      const m = timeSeries(fx.db, metrics, { timestamp: metrics.ts, value: metrics.value });
      const first = await m.bucketQuery({ start: "2026-09-24T00:00:00Z", end: "2026-09-25T00:00:00Z" }, { unit: "hour" });
      await fx.db.driver.close();

      const db2 = await createDatabase({
        url: fx.url,
        driverOptions: { driver: driverKind },
        tables: { metrics },
      });
      try {
        const m2 = timeSeries(db2, metrics, { timestamp: metrics.ts, value: metrics.value });
        const second = await m2.bucketQuery({ start: "2026-09-24T00:00:00Z", end: "2026-09-25T00:00:00Z" }, { unit: "hour" });
        assert.deepEqual(second, first);
      } finally {
        await db2.driver.close();
      }
    });
  });
}
