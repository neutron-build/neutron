import assert from "node:assert/strict";
import test from "node:test";
import {
  createDatabase,
  eq,
  asc,
  sql,
  pgTable,
  serial,
  integer,
  text,
  bigint,
  numeric,
  double,
  timestamp,
  timestamptz,
  date,
  jsonb,
  bytea,
  relations,
  schemaToDDL,
  jsonNull,
  type NeutronDatabase,
} from "./index.js";
import { TEST_URL, ensureLive, uniqueDbName } from "./live-harness.js";

// F03 live suite (V08): lossless values across every route. int8 never passes
// through JS Number (bigint default; string mode; checked safe-number mode
// rejects overflow), numerics keep exact scale, temporals round-trip as
// canonical microsecond-exact strings on both drivers under any process
// timezone, bytea survives every path, SQL NULL stays distinct from JSON
// null on writes, and decode/encode failures carry column context. The
// oracles are raw driver queries with explicit ::text casts and hand-written
// literals — never the ORM's own decode helpers.
//
// Process-timezone coverage: this file asserts exact canonical values that
// must not depend on the process timezone; the recorded evidence runs it
// under TZ=UTC, TZ=Asia/Tokyo and TZ=America/New_York.

const DB_NAME = uniqueDbName("f03");

const samples = pgTable("f03_samples", {
  id: serial("id").primaryKey(),
  big: bigint("big").notNull(),
  bigStr: bigint("big_str", { mode: "string" }).notNull(),
  bigNum: bigint("big_num", { mode: "number" }).notNull(),
  amt: numeric("amt").notNull(),
  amtDec: numeric("amt_dec", { decoder: (raw: string) => ({ raw, len: raw.length }) }).notNull(),
  ts: timestamp("ts").notNull(),
  tsDate: timestamp("ts_date", { mode: "date" }).notNull(),
  atz: timestamptz("atz").notNull(),
  atzDate: timestamptz("atz_date", { mode: "date" }).notNull(),
  d: date("d").notNull(),
  bin: bytea("bin"),
  doc: jsonb("doc"),
  weight: double("weight").notNull(),
});

const ledgers = pgTable("f03_ledgers", {
  id: bigint("id").primaryKey(),
  label: text("label").notNull(),
});

const entries = pgTable("f03_entries", {
  id: serial("id").primaryKey(),
  ledgerId: bigint("ledger_id").notNull().references(() => ledgers.id),
  amount: numeric("amount").notNull(),
  postedAt: timestamptz("posted_at").notNull(),
});

const rates = pgTable("f03_rates", {
  rate: numeric("rate").primaryKey(),
  note: text("note").notNull(),
});

const ledgersRelations = relations(ledgers, ({ many }) => ({ entries: many(entries) }));
const entriesRelations = relations(entries, ({ one }) => ({
  ledger: one(ledgers, { fields: [entries.ledgerId], references: [ledgers.id] }),
}));

type Tables = { samples: typeof samples; ledgers: typeof ledgers; entries: typeof entries; rates: typeof rates };
type RelationsMap = { ledgers: typeof ledgersRelations; entries: typeof entriesRelations };
type TestDb = NeutronDatabase<Tables, RelationsMap>;

async function withSuite(driverKind: "postgres" | "pg", fn: (db: TestDb) => Promise<void>): Promise<void> {
  if (!(await ensureLive(`live codecs (${driverKind})`))) {
    return;
  }
  const { Pool } = (await import("pg")) as unknown as {
    Pool: new (o: object) => { query: (s: string, p?: unknown[]) => Promise<unknown>; end: () => Promise<void> };
  };
  const admin = new Pool({ connectionString: TEST_URL, max: 1 });
  await admin.query(`drop database if exists "${DB_NAME}"`);
  await admin.query(`create database "${DB_NAME}"`);
  await admin.end();

  const url = new URL(TEST_URL);
  url.pathname = `/${DB_NAME}`;
  const db = await createDatabase({
    url: url.toString(),
    driverOptions: { driver: driverKind },
    tables: { samples, ledgers, entries, rates },
    relations: { ledgers: ledgersRelations, entries: entriesRelations },
  });
  for (const stmt of schemaToDDL([samples, ledgers, entries, rates])) {
    await db.driver.execute(stmt);
  }
  try {
    await fn(db);
  } finally {
    await db.close();
    const admin2 = new Pool({ connectionString: TEST_URL, max: 1 });
    await admin2.query(`drop database if exists "${DB_NAME}"`);
    await admin2.end();
  }
}

const INT8_MAX = "9223372036854775807";
const INT8_MIN = "-9223372036854775808";

for (const driverKind of ["postgres", "pg"] as const) {
  test(`live codecs (${driverKind}): V08 int8 extrema and safe-integer boundaries across raw/projected/returning paths`, async () => {
    await withSuite(driverKind, async (db) => {
      const inserted = await db
        .insert(samples)
        .values({
          big: 9007199254740993n,
          bigStr: "-9007199254740995",
          bigNum: 42,
          amt: "0",
          amtDec: "0",
          ts: "2026-01-01T00:00:00",
          tsDate: "2026-01-01T00:00:00",
          atz: "2026-01-01T00:00:00Z",
          atzDate: "2026-01-01T00:00:00Z",
          d: "2026-01-01",
          weight: 0.5,
        })
        .returning();
      assert.equal(inserted[0].big, 9007199254740993n);
      assert.equal(typeof inserted[0].big, "bigint", "returning path: default bigint mode");
      assert.equal(inserted[0].bigStr, "-9007199254740995");
      assert.equal(typeof inserted[0].bigStr, "string", "returning path: string mode stays a string");
      assert.equal(inserted[0].bigNum, 42);
      assert.equal(typeof inserted[0].bigNum, "number", "returning path: safe-number mode");

      // Extrema (beyond safe range in the default and string modes).
      const extrema = await db
        .insert(samples)
        .values({
          big: BigInt(INT8_MAX),
          bigStr: INT8_MIN,
          bigNum: 0,
          amt: "0",
          amtDec: "0",
          ts: "2026-01-01T00:00:00",
          tsDate: "2026-01-01T00:00:00",
          atz: "2026-01-01T00:00:00Z",
          atzDate: "2026-01-01T00:00:00Z",
          d: "2026-01-01",
          weight: 0.5,
        })
        .returning();
      assert.equal(extrema[0].big, BigInt(INT8_MAX));
      assert.equal(extrema[0].bigStr, INT8_MIN);

      // Raw select path.
      const rows = await db.select().from(samples).orderBy(asc(samples.id));
      assert.equal(rows[0].big, 9007199254740993n);
      assert.equal(rows[1].big, BigInt(INT8_MAX));

      // Projected select, including an expression projection (unknown).
      const projected = await db.select({ big: samples.big, bigStr: samples.bigStr, n: sql`1 + 1` }).from(samples);
      assert.equal(projected[0].big, 9007199254740993n);
      assert.equal(projected[0].bigStr, "-9007199254740995");
      assert.equal(projected[0].n, 2);

      // Safe-number mode REJECTS overflow at decode time with column
      // context — the loss is caught, never silently returned as a double.
      // (The unsafe value is written first through the exact string write
      // path, which number-mode reads then refuse to narrow.)
      await db.insert(samples).values({
        big: 7n, bigStr: "7", bigNum: "9007199254740993", amt: "0", amtDec: "0",
        ts: "2026-01-01T00:00:00", tsDate: "2026-01-01T00:00:00",
        atz: "2026-01-01T00:00:00Z", atzDate: "2026-01-01T00:00:00Z",
        d: "2026-01-01", weight: 0.5,
      });
      await assert.rejects(
        async () => db.select({ bigNum: samples.bigNum }).from(samples),
        /column "bigNum" \("big_num"\) on f03_samples: int8 value 9007199254740993 overflows the JS safe integer range/,
      );

      // Independent oracle: raw driver reads the text representation.
      const oracle = await db.driver.query<{ big: string; big_str: string }>(
        "select big::text, big_str::text from f03_samples order by id",
      );
      assert.equal(oracle[0].big, "9007199254740993");
      assert.equal(oracle[1].big, INT8_MAX);
      assert.equal(oracle[0].big_str, "-9007199254740995");

      // Write validation: unsafe number and non-integer strings reject with
      // column context before execution.
      await assert.rejects(
        async () => db.insert(samples).values({ big: 9007199254740993, bigStr: "1", bigNum: 1, amt: "0", amtDec: "0", ts: "2026-01-01T00:00:00", tsDate: "2026-01-01T00:00:00", atz: "2026-01-01T00:00:00Z", atzDate: "2026-01-01T00:00:00Z", d: "2026-01-01", weight: 0.5 }),
        /column "big" \("big"\) on f03_samples: number 9007199254740992 is not a safe integer/,
      );
      await assert.rejects(
        async () => db.insert(samples).values({ big: "1.5" as never, bigStr: "1", bigNum: 1, amt: "0", amtDec: "0", ts: "2026-01-01T00:00:00", tsDate: "2026-01-01T00:00:00", atz: "2026-01-01T00:00:00Z", atzDate: "2026-01-01T00:00:00Z", d: "2026-01-01", weight: 0.5 }),
        /"1\.5" is not an integer string/,
      );
      await assert.rejects(
        async () => db.insert(samples).values({ big: "9223372036854775808" as never, bigStr: "1", bigNum: 1, amt: "0", amtDec: "0", ts: "2026-01-01T00:00:00", tsDate: "2026-01-01T00:00:00", atz: "2026-01-01T00:00:00Z", atzDate: "2026-01-01T00:00:00Z", d: "2026-01-01", weight: 0.5 }),
        /out of range/,
      );
    });
  });

  test(`live codecs (${driverKind}): V08 decimals — >30 digits, trailing zeros, exponent normalization, user decoder`, async () => {
    await withSuite(driverKind, async (db) => {
      const dec31 = "1234567890123456789012345678901.23";
      await db.insert(samples).values({
        big: 1n,
        bigStr: "1",
        bigNum: 1,
        amt: dec31,
        amtDec: dec31,
        ts: "2026-01-01T00:00:00",
        tsDate: "2026-01-01T00:00:00",
        atz: "2026-01-01T00:00:00Z",
        atzDate: "2026-01-01T00:00:00Z",
        d: "2026-01-01",
        weight: 0.5,
      });
      const rows = await db.select().from(samples);
      assert.equal(rows[0].amt, dec31, ">30 digit decimal round-trips exactly");
      // User decoder receives the exact wire string.
      assert.deepEqual(rows[0].amtDec, { raw: dec31, len: dec31.length });

      // Trailing-zero scale survives update through the encoded write path.
      await db.update(samples).set({ amt: "1.50" }).where(eq(samples.id, rows[0].id));
      const after = await db.select({ amt: samples.amt }).from(samples).where(eq(samples.id, rows[0].id));
      assert.equal(after[0].amt, "1.50", "scale preserved through update");

      // Exponent input normalizes per PostgreSQL numeric semantics; the codec
      // returns whatever PostgreSQL stores, honestly.
      await db.update(samples).set({ amt: "1.5e1" }).where(eq(samples.id, rows[0].id));
      const norm = await db.select({ amt: samples.amt }).from(samples).where(eq(samples.id, rows[0].id));
      assert.equal(norm[0].amt, "15");

      const oracle = await db.driver.query<{ amt: string }>("select amt::text from f03_samples where id = $1", [rows[0].id]);
      assert.equal(oracle[0].amt, "15");
    });
  });

  test(`live codecs (${driverKind}): V08 temporals — microseconds, DST instants, date boundaries, Date modes, determinism`, async () => {
    await withSuite(driverKind, async (db) => {
      const tsUs = "2026-03-08T07:30:00.123456";
      const atzUs = "2026-03-08T07:30:00.123456Z"; // spring-forward boundary in America/New_York (02:30 local does not exist)
      const atzFallA = "2026-11-01T05:30:00.123456Z"; // fall-back: both 01:30 EDT and 01:30 EST exist as distinct instants
      const atzFallB = "2026-11-01T06:30:00.123456Z";
      const dMin = "0001-01-01";
      const dMax = "9999-12-31";
      await db.insert(samples).values([
        {
          big: 1n, bigStr: "1", bigNum: 1, amt: "0", amtDec: "0",
          ts: tsUs, tsDate: tsUs, atz: atzUs, atzDate: atzUs, d: dMin,
          bin: Buffer.from([0x00, 0xff, 0x10]), weight: 0.5,
        },
        {
          big: 2n, bigStr: "2", bigNum: 2, amt: "0", amtDec: "0",
          ts: "2026-11-01T00:30:00.000001", tsDate: "2026-11-01T00:30:00.000001",
          atz: atzFallA, atzDate: atzFallA, d: dMax,
          bin: new Uint8Array(0), weight: 0.5,
        },
        {
          big: 3n, bigStr: "3", bigNum: 3, amt: "0", amtDec: "0",
          ts: "2026-11-01T00:30:00.000002", tsDate: "2026-11-01T00:30:00.000002",
          atz: atzFallB, atzDate: atzFallB, d: "2028-02-29",
          weight: 0.5,
        },
      ]);

      const rows = await db.select().from(samples).orderBy(asc(samples.id));
      // microseconds survive every path — a Date-mode read would have
      // truncated .123456 to .123; the canonical strings keep six digits.
      assert.equal(rows[0].ts, tsUs);
      assert.equal(rows[0].atz, atzUs, "timestamptz renders its canonical UTC string");
      assert.equal(rows[1].atz, atzFallA);
      assert.equal(rows[2].atz, atzFallB);
      assert.notEqual(rows[1].atz, rows[2].atz, "fall-back ambiguity survives as distinct instants");
      assert.equal(rows[0].d, dMin, "date lower boundary");
      assert.equal(rows[1].d, dMax, "date upper boundary");
      assert.equal(rows[2].d, "2028-02-29", "leap day");

      // Date modes: exact instant, millisecond truncation (documented).
      assert.ok(rows[0].tsDate instanceof Date);
      assert.equal(rows[0].tsDate.toISOString(), "2026-03-08T07:30:00.123Z", "timestamp Date mode = UTC wall clock, ms");
      assert.ok(rows[0].atzDate instanceof Date);
      assert.equal(rows[0].atzDate.toISOString(), "2026-03-08T07:30:00.123Z", "timestamptz Date mode = exact instant, ms");

      // Empty bytea round-trips; NULL stays NULL.
      assert.ok(rows[1].bin instanceof Uint8Array);
      assert.equal(rows[1].bin.length, 0);
      assert.equal(rows[2].bin, null);
      assert.ok(rows[0].bin instanceof Uint8Array);
      assert.equal(Buffer.from(rows[0].bin).toString("hex"), "00ff10");

      // Predicate with a canonical Z string hits exactly the DST row.
      const hit = await db.select({ atz: samples.atz }).from(samples).where(eq(samples.atz, atzUs));
      assert.equal(hit.length, 1);
      assert.equal(hit[0].atz, atzUs);

      // Independent oracle: raw driver, server-side text rendering.
      const oracle = await db.driver.query<{ ts: string; atz: string; d: string }>(
        "select ts::text, (atz at time zone 'UTC')::text as atz, d::text from f03_samples order by id",
      );
      assert.equal(oracle[0].ts.replace(" ", "T"), tsUs);
      assert.equal(oracle[0].atz.replace(" ", "T") + "Z", atzUs);
      assert.equal(oracle[0].d, dMin);

      // Date WRITE determinism: the same Date instant stores the same
      // timestamp wall clock on every machine (UTC wall clock). This exact
      // string is asserted identically under every TZ the suite runs in.
      const instant = new Date(Date.UTC(2026, 0, 2, 3, 4, 5, 678));
      await db.update(samples).set({ ts: instant }).where(eq(samples.big, 1n));
      const upd = await db.select({ ts: samples.ts }).from(samples).where(eq(samples.big, 1n));
      assert.equal(upd[0].ts, "2026-01-02T03:04:05.678");
      await db.update(samples).set({ atz: instant }).where(eq(samples.big, 1n));
      const upd2 = await db.select({ atz: samples.atz }).from(samples).where(eq(samples.big, 1n));
      assert.equal(upd2[0].atz, "2026-01-02T03:04:05.678Z");

      // Offset-carrying timestamptz writes are honored exactly.
      await db.update(samples).set({ atz: "2026-03-08T02:30:00-05:00" }).where(eq(samples.big, 2n));
      const shifted = await db.select({ atz: samples.atz }).from(samples).where(eq(samples.big, 2n));
      assert.equal(shifted[0].atz, "2026-03-08T07:30:00Z", "-05:00 offset converts to the UTC instant");

      // infinity pass-through: exact strings survive; Date mode would reject.
      await db.update(samples).set({ atz: "infinity", atzDate: "infinity" }).where(eq(samples.big, 3n));
      const inf = await db.select({ atz: samples.atz }).from(samples).where(eq(samples.big, 3n));
      assert.equal(inf[0].atz, "infinity");
      await assert.rejects(
        async () => db.select({ atzDate: samples.atzDate }).from(samples).where(eq(samples.big, 3n)),
        /cannot be represented as a Date/,
      );

      // Temporal write validation rejects offsets on timestamps and garbage.
      await assert.rejects(
        async () => db.update(samples).set({ ts: "2026-01-01T00:00:00Z" as never }).where(eq(samples.big, 1n)),
        /column "ts" \("ts"\) on f03_samples: "2026-01-01T00:00:00Z" is not a canonical timestamp string/,
      );
      await assert.rejects(
        async () => db.update(samples).set({ atz: "2026-01-01 00:00:00" as never }).where(eq(samples.big, 1n)),
        /is not a canonical timestamptz string/,
      );
      await assert.rejects(
        async () => db.update(samples).set({ d: "2026-1-1" as never }).where(eq(samples.big, 1n)),
        /is not bindable for date columns/,
      );
    });
  });

  test(`live codecs (${driverKind}): V08 SQL NULL versus JSON null writes, nested values, non-finite rejection`, async () => {
    await withSuite(driverKind, async (db) => {
      const base = {
        big: 1n, bigStr: "1", bigNum: 1, amt: "0", amtDec: "0",
        ts: "2026-01-01T00:00:00", tsDate: "2026-01-01T00:00:00",
        atz: "2026-01-01T00:00:00Z", atzDate: "2026-01-01T00:00:00Z",
        d: "2026-01-01", weight: 0.5,
      };
      await db.insert(samples).values({ ...base, doc: null });
      await db.insert(samples).values({ ...base, doc: jsonNull });
      await db.insert(samples).values({ ...base, doc: "null" });
      await db.insert(samples).values({ ...base, doc: { a: 1, b: [null, "x", true, 1.5], nested: { deep: [1, [2]] } } });
      await db.insert(samples).values({ ...base });

      // Independent oracle: database state inspected with raw SQL.
      const state = await db.driver.query<{ id: number; is_null: boolean; jtype: string | null; text: string | null }>(
        "select id, (doc is null) as is_null, jsonb_typeof(doc)::text as jtype, doc::text as text from f03_samples order by id",
      );
      assert.equal(state[0].is_null, true, "JS null writes SQL NULL");
      assert.equal(state[0].jtype, null);
      assert.equal(state[1].is_null, false);
      assert.equal(state[1].jtype, "null", "jsonNull writes the JSON null value");
      assert.equal(state[1].text, "null");
      assert.equal(state[2].jtype, "string", "the JS string 'null' writes the JSON string");
      assert.equal(state[2].text, '"null"');
      assert.equal(state[4].is_null, true, "omitted key requests DEFAULT — nullable without default stores NULL");

      const rows = await db.select().from(samples).orderBy(asc(samples.id));
      assert.equal(rows[0].doc, null);
      assert.equal(rows[1].doc, null, "JSON null reads back as JS null (distinct write, ambiguous read — documented)");
      assert.equal(rows[2].doc, "null");
      assert.deepEqual(rows[3].doc, { a: 1, b: [null, "x", true, 1.5], nested: { deep: [1, [2]] } }, "nested arrays/objects round-trip");

      const upd = await db.update(samples).set({ doc: jsonNull }).where(eq(samples.id, 5)).returning();
      const updState = await db.driver.query<{ jtype: string }>("select jsonb_typeof(doc)::text as jtype from f03_samples where id = 5");
      assert.equal(updState[0].jtype, "null", "update path: jsonNull stays JSON null");
      void upd;

      // bigint inside json values is still rejected (raw JSON codec is a
      // later, explicit opt-in).
      await assert.rejects(
        async () => db.insert(samples).values({ ...base, doc: { big: 1n } as never }),
        /JSON-representable/,
      );
      // Non-finite doubles reject with column context before execution.
      await assert.rejects(
        async () => db.insert(samples).values({ ...base, weight: Infinity }),
        /column "weight" \("weight"\) on f03_samples: double columns accept finite numbers/,
      );
      await assert.rejects(
        async () => db.insert(samples).values({ ...base, weight: NaN }),
        /finite/,
      );
    });
  });

  test(`live codecs (${driverKind}): V08 relation children decode through the same codecs; int8/numeric PK mutation`, async () => {
    await withSuite(driverKind, async (db) => {
      await db.insert(ledgers).values({ id: 9007199254740993n, label: "L1" });
      await db.insert(entries).values([
        { ledgerId: 9007199254740993n, amount: "99999999999999999999.99", postedAt: "2026-03-08T07:30:00.123456Z" },
        { ledgerId: 9007199254740993n, amount: "0.10", postedAt: "2026-11-01T05:30:00.000001Z" },
      ]);

      const l1 = await db.query.ledgers.findFirst({ where: eq(ledgers.label, "L1"), with: { entries: true } });
      assert.ok(l1);
      assert.equal(l1.id, 9007199254740993n, "parent int8 PK decodes as bigint");
      assert.equal(l1.entries.length, 2);
      // Child leaves: exact int8/numeric/timestamptz — JSON.parse of numbers
      // would have corrupted the first two before decoding.
      assert.equal(l1.entries[0].ledgerId, 9007199254740993n);
      assert.equal(l1.entries[0].amount, "99999999999999999999.99");
      assert.equal(l1.entries[1].amount, "0.10");
      assert.equal(l1.entries[0].postedAt, "2026-03-08T07:30:00.123456Z", "child timestamptz keeps microseconds and UTC form");

      // To-one back through the composite-relevant int8 FK.
      const e = await db.query.entries.findFirst({ where: eq(entries.amount, "0.10"), with: { ledger: true } });
      assert.ok(e && e.ledger);
      assert.equal(e.ledger.id, 9007199254740993n);

      // int8 + numeric primary-key mutation with raw-SQL verification.
      await db.update(ledgers).set({ label: "L1x" }).where(eq(ledgers.id, 9007199254740993n));
      const led = await db.driver.query<{ label: string }>("select label from f03_ledgers where id = $1", ["9007199254740993"]);
      assert.equal(led[0].label, "L1x");

      await db.insert(rates).values({ rate: "0.10", note: "r" });
      await db.update(rates).set({ note: "r2" }).where(eq(rates.rate, "0.10"));
      const rateRow = await db.driver.query<{ note: string }>("select note from f03_rates where rate = $1", ["0.10"]);
      assert.equal(rateRow[0].note, "r2", "numeric PK predicate matches exactly (0.10 scale intact)");
      await db.delete(rates).where(eq(rates.rate, "0.10"));
      const after = await db.driver.query<{ n: string }>("select count(*)::text as n from f03_rates");
      assert.equal(after[0].n, "0");
    });
  });
}
