import assert from "node:assert/strict";
import test from "node:test";
import { after } from "node:test";
import pg from "pg";
import {
  QueryCanceledError,
  ServerSqlError,
  NeutronSqlError,
  alias,
  asc,
  count,
  createDatabase,
  cteTable,
  desc,
  eq,
  getSqlState,
  inArray,
  integer,
  lte,
  over,
  bigint,
  numeric,
  pgTable,
  rowNumber,
  rank,
  denseRank,
  percentRank,
  cumeDist,
  ntile,
  lag,
  lead,
  firstValue,
  lastValue,
  nthValue,
  serial,
  sql,
  sum,
  text,
  timestamp,
  timestamptz,
  bytea,
  wrapPgPool,
  wrapPostgresJs,
  type Driver,
  type PgPoolLike,
  type SqlEvent,
} from "./index.js";
import { TEST_URL, ensureLive, uniqueDbName } from "./live-harness.js";

// Q08 live battery (V13/V16 subset owned by this card): window functions vs
// hand-written SQL oracles (partition/order/frame variants incl. RANGE/ROWS/
// GROUPS boundaries + EXCLUDE, value functions, precision through int8/
// numeric/us-tz/bytea, composition over CTEs and aggregates), row locking
// with observable semantics under two connections (FOR UPDATE wait, NOWAIT
// 55P03, SKIP LOCKED work queue, strength compatibility), explicit batch
// plans (results tuple, atomicity, REPEATABLE READ snapshot differential,
// opt-in retry, enclosing-scope participation) and bounded streaming over
// server-side cursors (batch bounds, early iterator exit returning the
// connection promptly — V16 — cancellation reaching the server, pool-count
// stability). Real drivers, real Postgres, both legs; statement counts via
// the structured logger enforce planned statement counts everywhere.

const events = pgTable("q08_events", {
  id: serial("id").primaryKey(),
  actor: integer("actor").notNull(),
  day: integer("day").notNull(),
  amount: bigint("amount"),
  price: numeric("price"),
  note: text("note"),
  seen: timestamptz("seen"),
  logged: timestamp("logged"),
  payload: bytea("payload"),
});

const jobs = pgTable("q08_jobs", {
  id: serial("id").primaryKey(),
  queue: text("queue").notNull(),
  state: text("state").notNull(),
  attempts: integer("attempts").notNull(),
});

const kv = pgTable("q08_kv", {
  k: text("k").primaryKey(),
  v: integer("v").notNull(),
});

const strict = pgTable("q08_strict", {
  id: integer("id").primaryKey(),
  label: text("label").notNull(),
});

type Row = Record<string, unknown>;

interface Q08Ctx {
  driverKind: "postgres" | "pg";
  /** URL of THIS suite's throwaway database (raw second connections). */
  dbUrl: string;
  db: Awaited<ReturnType<typeof createDatabase>>;
  driver: Driver;
  /** Raw pg pool when driverKind === "pg" (pool metrics), else null. */
  pool: pg.Pool | null;
  /** Raw postgres.js client when driverKind === "postgres", else null. */
  pjs: { end(o?: { timeout?: number }): Promise<void> } | null;
  admin: pg.Pool;
  /** Live statement counter fed by the database logger. */
  statements: { count: number };
  /** One-shot listeners on logger events (for statement interleaving). */
  listeners: Array<(e: SqlEvent) => void>;
  close(): Promise<void>;
}

const contexts: Q08Ctx[] = [];

const EVENT_ROWS: Array<[actor: number, day: number, amount: string | null, price: string | null, seen: string | null, payload: Buffer | null]> = [
  [1, 1, "100", "10.5000", "2026-01-02 03:04:05.123456+00", Buffer.from([0x00, 0x01, 0x02])],
  [1, 2, "200", "20.2500", "2026-01-03 03:04:05.654321+00", Buffer.from([0x03])],
  [1, 3, "100", "30.0000", null, null],
  [2, 1, "10", "1.1000", "2026-02-01 00:00:00+00", null],
  [2, 2, "20", null, "2026-02-02 12:34:56.999999+00", Buffer.from([0xff, 0xfe])],
  [3, 1, "2305843009213693952", "7.0000", "2026-03-01 01:02:03.000001+00", null],
  [3, 2, "70", "7.0000", "2026-03-02 01:02:03.000002+00", null],
  [3, 3, "70", null, "2026-03-03 01:02:03.000003+00", null],
  [3, 4, "300", "3.1415900000", "2026-03-04 01:02:03.000004+00", Buffer.alloc(0)],
  [3, 5, null, null, "2026-03-05 01:02:03.000005+00", null],
];

async function createCtx(driverKind: "postgres" | "pg"): Promise<Q08Ctx> {
  const DB_NAME = uniqueDbName(`q08_${driverKind}`);
  const admin = new pg.Pool({ connectionString: TEST_URL, max: 2 });
  await admin.query(`drop database if exists "${DB_NAME}"`);
  await admin.query(`create database "${DB_NAME}"`);
  const url = new URL(TEST_URL);
  url.pathname = `/${DB_NAME}`;

  const statements = { count: 0 };
  const listeners: Array<(e: SqlEvent) => void> = [];
  let pool: pg.Pool | null = null;
  let pjs: { end(o?: { timeout?: number }): Promise<void> } | null = null;
  let driver: Driver;
  if (driverKind === "pg") {
    pool = new pg.Pool({ connectionString: url.toString(), max: 4 });
    driver = wrapPgPool(pool as unknown as PgPoolLike);
  } else {
    const postgres = (await import("postgres")) as unknown as { default: (u: string, o?: object) => import("./index.js").PostgresJsClient };
    pjs = postgres.default(url.toString(), { max: 4 });
    driver = wrapPostgresJs(pjs as import("./index.js").PostgresJsClient);
  }
  const db = await createDatabase({
    driver,
    logger: (e: SqlEvent) => {
      if (e.kind === "query-end") statements.count += 1;
      for (const l of [...listeners]) l(e);
    },
  });

  await driver.execute(`create table "q08_events" (
    "id" serial primary key,
    "actor" integer not null,
    "day" integer not null,
    "amount" bigint,
    "price" numeric(20, 4),
    "note" text,
    "seen" timestamptz,
    "logged" timestamp,
    "payload" bytea)`);
  await driver.execute(`create table "q08_jobs" ("id" serial primary key, "queue" text not null, "state" text not null, "attempts" integer not null)`);
  await driver.execute(`create table "q08_kv" ("k" text primary key, "v" integer not null)`);
  await driver.execute(`create table "q08_strict" ("id" integer primary key, "label" text not null)`);
  await driver.execute(`create function "q08_nap"() returns boolean language plpgsql as $$ begin perform pg_sleep(0.05); return true; end $$`);
  // Gate for deterministic cross-transaction ordering: blocks until the
  // advisory lock is released elsewhere, then passes it on. Advisory locks
  // live in cluster state, so this orders statements regardless of the
  // transaction's MVCC snapshot.
  await driver.execute(`create function "q08_gate"(k bigint) returns boolean language plpgsql as $$ begin perform pg_advisory_lock(k); perform pg_advisory_unlock(k); return true; end $$`);
  for (let i = 0; i < EVENT_ROWS.length; i++) {
    const [actor, day, amount, price, seen, payload] = EVENT_ROWS[i];
    await driver.execute(
      `insert into "q08_events" ("actor", "day", "amount", "price", "seen", "payload") values (${actor}, ${day}, ${amount === null ? null : `${amount}`}, ${price === null ? null : `'${price}'`}, ${seen === null ? null : `'${seen}'`}, ${payload === null ? null : `'\\x${payload.toString("hex")}'`})`,
    );
  }
  await driver.execute(`insert into "q08_jobs" ("queue", "state", "attempts") values ('inbox', 'ready', 0), ('inbox', 'ready', 0), ('inbox', 'ready', 0), ('inbox', 'ready', 0), ('inbox', 'ready', 0)`);
  await driver.execute(`insert into "q08_kv" ("k", "v") values ('snap', 0), ('retry', 0), ('atomic', 0)`);

  const ctx: Q08Ctx = {
    driverKind,
    dbUrl: url.toString(),
    db,
    driver,
    pool,
    pjs,
    admin,
    statements,
    listeners,
    async close(): Promise<void> {
      await db.close();
      if (pool !== null) await pool.end();
      if (pjs !== null) await pjs.end({ timeout: 5 });
      await admin.query(`drop database if exists "${DB_NAME}"`);
      await admin.end();
    },
  };
  contexts.push(ctx);
  return ctx;
}

const setups: Record<string, Promise<Q08Ctx> | undefined> = {};
async function ctxFor(driverKind: "postgres" | "pg"): Promise<Q08Ctx | null> {
  if (!(await ensureLive(`live q08 (${driverKind})`))) return null;
  setups[driverKind] ??= createCtx(driverKind);
  return setups[driverKind]!;
}

after(async () => {
  for (const ctx of contexts) await ctx.close();
});

/** Compare compiled-query rows with a hand-written SQL oracle (the V13
 *  rule: independent SQL, not the implementation's own output). Normalizes
 *  driver-native forms (pg returns int8 as string, postgres.js as string
 *  for > int4 too) to bigint/number so both sides compare exactly. */
async function compare(ctx: Q08Ctx, label: string, got: Row[], oracleSql: string, normalize?: (row: Row) => Row): Promise<void> {
  const want = (await ctx.driver.query(oracleSql)) as Row[];
  const n = normalize ?? ((r: Row) => r);
  assert.deepEqual(got.map(n), want.map(n), `${label}: compiled results differ from hand-written SQL`);
}

const normInt = (v: unknown): unknown => (v === null || v === undefined ? v : BigInt(v as string | number | bigint | boolean));
const normFp = (v: unknown): unknown => (typeof v === "number" ? v.toFixed(6) : v);

/** A second, independent connection (raw pg client on the same database). */
async function rawClient(ctx: Q08Ctx): Promise<pg.Client> {
  const client = new pg.Client({ connectionString: ctx.dbUrl });
  await client.connect();
  return client;
}

// ---------------------------------------------------------------------------
// Window functions — hand-SQL oracle twins
// ---------------------------------------------------------------------------

for (const driverKind of ["postgres", "pg"] as const) {
  test(`live q08 (${driverKind}): ranking windows match hand SQL (partition, order, ties, NULLs)`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const rows = await ctx.db
      .select({
        id: events.id,
        rn: over(rowNumber(), { partitionBy: [events.actor], orderBy: [desc(events.amount)] }),
        rk: over(rank(), { partitionBy: [events.actor], orderBy: [desc(events.amount)] }),
        drk: over(denseRank(), { partitionBy: [events.actor], orderBy: [desc(events.amount)] }),
      })
      .from(events)
      .orderBy(asc(events.id));
    ctx.statements.count = 0;
    void (await ctx.db
      .select({ rn: over(rowNumber(), { partitionBy: [events.actor], orderBy: [desc(events.amount)] }) })
      .from(events));
    assert.equal(ctx.statements.count, 1, "window query must be exactly one statement");
    await compare(
      ctx,
      "ranking windows",
      rows as unknown as Row[],
      `select "id",
         row_number() over (partition by "actor" order by "amount" desc) as rn,
         rank() over (partition by "actor" order by "amount" desc) as rk,
         dense_rank() over (partition by "actor" order by "amount" desc) as drk
       from "q08_events" order by "id"`,
      (r) => ({ id: r.id, rn: normInt(r.rn), rk: normInt(r.rk), drk: normInt(r.drk) }) as Row,
    );
    // Spot-check decode types: int8 rankings arrive as bigint, never number.
    assert.equal(typeof rows[0].rn, "bigint");
  });

  test(`live q08 (${driverKind}): value windows lag/lead/first/last/nth match hand SQL`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const rows = await ctx.db
      .select({
        id: events.id,
        prevAmount: over(lag(events.amount), { partitionBy: [events.actor], orderBy: [asc(events.day)] }),
        next2Day: over(lead(events.day, 2), { partitionBy: [events.actor], orderBy: [asc(events.day)] }),
        firstPrice: over(firstValue(events.price), { partitionBy: [events.actor], orderBy: [asc(events.day)] }),
        lastAmt: over(lastValue(events.amount), {
          partitionBy: [events.actor],
          orderBy: [asc(events.day)],
          frame: { mode: "rows", start: "unbounded preceding", end: "unbounded following" },
        }),
        nthAmt: over(nthValue(events.amount, 2), {
          partitionBy: [events.actor],
          orderBy: [asc(events.day)],
          frame: { mode: "rows", start: "unbounded preceding", end: "unbounded following" },
        }),
      })
      .from(events)
      .orderBy(asc(events.id));
    await compare(
      ctx,
      "value windows",
      rows as unknown as Row[],
      `select "id",
         lag("amount", 1) over (partition by "actor" order by "day" asc) as "prevAmount",
         lead("day", 2) over (partition by "actor" order by "day" asc) as "next2Day",
         first_value("price") over (partition by "actor" order by "day" asc) as "firstPrice",
         last_value("amount") over (partition by "actor" order by "day" asc rows between unbounded preceding and unbounded following) as "lastAmt",
         nth_value("amount", 2) over (partition by "actor" order by "day" asc rows between unbounded preceding and unbounded following) as "nthAmt"
       from "q08_events" order by "id"`,
      (r) => {
        const out: Row = { id: r.id, prevAmount: normInt(r.prevAmount), next2Day: r.next2Day, firstPrice: r.firstPrice, lastAmt: normInt(r.lastAmt), nthAmt: normInt(r.nthAmt) };
        return out;
      },
    );
  });

  test(`live q08 (${driverKind}): ntile/percent_rank/cume_dist match hand SQL`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const rows = await ctx.db
      .select({
        id: events.id,
        quartile: over(ntile(3), { orderBy: [asc(events.day)] }),
        pr: over(percentRank(), { partitionBy: [events.actor], orderBy: [asc(events.day)] }),
        cd: over(cumeDist(), { partitionBy: [events.actor], orderBy: [asc(events.day)] }),
      })
      .from(events)
      .orderBy(asc(events.id));
    await compare(
      ctx,
      "distribution windows",
      rows as unknown as Row[],
      `select "id",
         ntile(3) over (order by "day" asc) as "quartile",
         percent_rank() over (partition by "actor" order by "day" asc) as pr,
         cume_dist() over (partition by "actor" order by "day" asc) as cd
       from "q08_events" order by "id"`,
      (r) => ({ id: r.id, quartile: r.quartile, pr: normFp(r.pr), cd: normFp(r.cd) }) as Row,
    );
  });

  test(`live q08 (${driverKind}): frame modes ROWS/RANGE/GROUPS with EXCLUDE variants match hand SQL`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const cases: Array<{ label: string; frame: Parameters<typeof over>[1] extends undefined ? never : NonNullable<Parameters<typeof over>[1]>["frame"]; oracle: string }> = [
      {
        label: "rows between 1 preceding and 1 following",
        frame: { mode: "rows", start: { preceding: 1 }, end: { following: 1 } },
        oracle: `sum("amount") over (order by "day" asc rows between 1 preceding and 1 following)`,
      },
      {
        label: "rows unbounded preceding (end omitted)",
        frame: { mode: "rows", start: "unbounded preceding" },
        oracle: `sum("amount") over (order by "day" asc rows unbounded preceding)`,
      },
      {
        label: "range unbounded preceding to current row (ties share the frame)",
        frame: { mode: "range", start: "unbounded preceding", end: "current row" },
        oracle: `sum("amount") over (order by "day" asc range between unbounded preceding and current row)`,
      },
      {
        label: "groups unbounded preceding to current row",
        frame: { mode: "groups", start: "unbounded preceding", end: "current row" },
        oracle: `sum("amount") over (order by "day" asc groups between unbounded preceding and current row)`,
      },
      {
        label: "groups exclude current row",
        frame: { mode: "groups", start: "unbounded preceding", end: "current row", exclude: "current row" },
        oracle: `sum("amount") over (order by "day" asc groups between unbounded preceding and current row exclude current row)`,
      },
      {
        label: "rows exclude group",
        frame: { mode: "rows", start: "unbounded preceding", end: "current row", exclude: "group" },
        oracle: `sum("amount") over (order by "day" asc rows between unbounded preceding and current row exclude group)`,
      },
      {
        label: "rows exclude ties",
        frame: { mode: "rows", start: "unbounded preceding", end: "current row", exclude: "ties" },
        oracle: `sum("amount") over (order by "day" asc rows between unbounded preceding and current row exclude ties)`,
      },
    ];
    for (const c of cases) {
      const rows = await ctx.db
        .select({ id: events.id, s: over(sum(events.amount), { orderBy: [asc(events.day)], frame: c.frame }) })
        .from(events)
        .orderBy(asc(events.id));
      await compare(
        ctx,
        c.label,
        rows as unknown as Row[],
        `select "id", ${c.oracle} as s from "q08_events" order by "id"`,
        (r) => ({ id: r.id, s: normInt(r.s) }) as Row,
      );
    }
  });

  test(`live q08 (${driverKind}): aggregates as windows compose with over()`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const rows = await ctx.db
      .select({
        id: events.id,
        running: over(sum(events.amount), { orderBy: [asc(events.day)] }),
        actorCount: over(count(), { partitionBy: [events.actor] }),
      })
      .from(events)
      .orderBy(asc(events.id));
    await compare(
      ctx,
      "aggregate windows",
      rows as unknown as Row[],
      `select "id", sum("amount") over (order by "day" asc) as running,
              count(*) over (partition by "actor") as "actorCount"
       from "q08_events" order by "id"`,
      (r) => ({ id: r.id, running: normInt(r.running), actorCount: normInt(r.actorCount) }) as Row,
    );
  });

  test(`live q08 (${driverKind}): windows compose over CTEs and aggregates (Q02 contract)`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    // Window inside a CTE, outer query filters the window result (the
    // top-N-per-group pattern).
    const ranked = cteTable(
      "ranked",
      ctx.db.select({
        id: events.id,
        actor: events.actor,
        amount: events.amount,
        rn: over(rowNumber(), { partitionBy: [events.actor], orderBy: [desc(events.amount)] }),
      }).from(events),
    );
    const topTwo = await ctx.db.select({ id: ranked.id, actor: ranked.actor, amount: ranked.amount }).from(ranked).where(lte(ranked.rn, 2)).orderBy(asc(ranked.actor), asc(ranked.rn));
    await compare(
      ctx,
      "window over CTE",
      topTwo as unknown as Row[],
      `with "ranked" as (
         select "id", "actor", "amount",
                row_number() over (partition by "actor" order by "amount" desc) as rn
         from "q08_events"
       )
       select "id", "actor", "amount" from "ranked" where rn <= 2 order by "actor", rn`,
      (r) => ({ id: r.id, actor: r.actor, amount: normInt(r.amount) }) as Row,
    );

    // Window over an aggregated derived table: rank actors by their totals.
    const perActor = ctx.db.select({ actor: events.actor, total: sum(events.amount) }).from(events).groupBy(events.actor);
    const wrapped = cteTable("per_actor", perActor);
    const byTotal = await ctx.db
      .select({ actor: wrapped.actor, total: wrapped.total, rk: over(rank(), { orderBy: [desc(wrapped.total)] }) })
      .from(wrapped);
    await compare(
      ctx,
      "window over aggregate",
      byTotal as unknown as Row[],
      `with "per_actor" as (select "actor", sum("amount") as total from "q08_events" group by "actor")
       select "actor", total, rank() over (order by total desc) as rk from "per_actor"`,
      (r) => ({ actor: r.actor, total: normInt(r.total), rk: normInt(r.rk) }) as Row,
    );
  });

  test(`live q08 (${driverKind}): precision is lossless through windows (int8 > 2^53, numeric, us timestamptz, bytea)`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const rows = await ctx.db
      .select({
        id: events.id,
        bigLag: over(lag(events.amount), { orderBy: [asc(events.id)] }),
        seenLag: over(lag(events.seen), { orderBy: [asc(events.id)] }),
        payloadLag: over(lag(events.payload), { orderBy: [asc(events.id)] }),
        priceLead: over(lead(events.price), { orderBy: [asc(events.id)] }),
      })
      .from(events)
      .where(inArray(events.actor, [3, 2]))
      .orderBy(asc(events.id));
    const byId = new Map(rows.map((r) => [Number(r.id), r]));
    // actor 3 row (the 2^61 amount) is id 6: its lag is actor 2's id 5 amount
    // 20n; its own amount must survive the window round trip exactly.
    const bigRow = byId.get(6)!;
    assert.equal(bigRow.bigLag, 20n);
    const oracle = (await ctx.driver.query(`select "amount" from "q08_events" where "id" = 6`)) as Array<{ amount: string }>;
    assert.equal(BigInt(oracle[0].amount), 2305843009213693952n);
    // microsecond timestamptz survives as the canonical UTC string.
    assert.equal(bigRow.seenLag, "2026-02-02T12:34:56.999999Z");
    // bytea through a window value function decodes to Uint8Array, exact bytes.
    const payloadRow = byId.get(6)!;
    assert.ok(payloadRow.payloadLag instanceof Uint8Array, `expected Uint8Array, got ${typeof payloadRow.payloadLag}`);
    assert.equal(Buffer.from(payloadRow.payloadLag).toString("hex"), "fffe");
    // numeric keeps its scale through the window (id 6 leads to id 7 whose
    // numeric(20,4) price is 7.0000 — exact text, scale preserved).
    const priceRow = byId.get(6)!;
    assert.equal(priceRow.priceLead, "7.0000");
  });

  test(`live q08 (${driverKind}): ORDER BY a window result orders by the computed value`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const rn = over(rowNumber(), { orderBy: [desc(events.amount)] });
    const rows = await ctx.db.select({ id: events.id, rn }).from(events).orderBy(desc(rn)).limit(3);
    const oracle = (await ctx.driver.query(
      `select "id" from "q08_events" order by row_number() over (order by "amount" desc) desc limit 3`,
    )) as Array<{ id: number }>;
    assert.deepEqual(rows.map((r) => Number(r.id)), oracle.map((r) => r.id));
  });

  test(`live q08 (${driverKind}): misplaced windows fail before any SQL runs (statement counter 0)`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    ctx.statements.count = 0;
    assert.throws(
      () => ctx.db.select().from(events).where(sql`${over(rowNumber(), {})} > ${1}`).toSQL(),
      /not allowed in WHERE/,
    );
    assert.throws(
      () => ctx.db.select({ n: count() }).from(events).for("update").toSQL(),
      /not allowed with aggregate functions/,
    );
    assert.equal(ctx.statements.count, 0);
  });
}

// ---------------------------------------------------------------------------
// Locking — observable semantics under two connections
// ---------------------------------------------------------------------------

for (const driverKind of ["postgres", "pg"] as const) {
  test(`live q08 (${driverKind}): FOR UPDATE waits for a concurrent holder, then returns the row`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const holder = await rawClient(ctx);
    try {
      await holder.query("begin");
      await holder.query(`update "q08_jobs" set "state" = 'held' where "id" = 1`);
      let resolved = false;
      const waiter = (async () => {
        const rows = await ctx.db.select({ id: jobs.id }).from(jobs).where(eq(jobs.id, 1)).for("update");
        resolved = true;
        return rows;
      })();
      await new Promise((r) => setTimeout(r, 400));
      assert.equal(resolved, false, "FOR UPDATE must block while another transaction holds the row lock");
      await holder.query("rollback");
      const rows = (await waiter) as unknown as Array<{ id: number }>;
      assert.deepEqual(rows.map((r) => Number(r.id)), [1]);
    } finally {
      await holder.end();
    }
  });

  test(`live q08 (${driverKind}): NOWAIT fails immediately with SQLSTATE 55P03`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const holder = await rawClient(ctx);
    try {
      await holder.query("begin");
      await holder.query(`update "q08_jobs" set "state" = 'held' where "id" = 2`);
      const started = Date.now();
      await assert.rejects(
        async () => await ctx.db.select({ id: jobs.id }).from(jobs).where(eq(jobs.id, 2)).for("update", { noWait: true }),
        (err: unknown) => {
          assert.ok(err instanceof ServerSqlError, `expected ServerSqlError, got ${String(err)}`);
          assert.equal(getSqlState(err), "55P03");
          return true;
        },
      );
      assert.ok(Date.now() - started < 2000, "NOWAIT must not wait");
    } finally {
      await holder.query("rollback");
      await holder.end();
    }
  });

  test(`live q08 (${driverKind}): SKIP LOCKED claims only unlocked rows (work-queue semantics)`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const holder = await rawClient(ctx);
    try {
      await holder.query("begin");
      await holder.query(`select "id" from "q08_jobs" where "id" in (1, 2) for update`);
      const claimed = await ctx.db.transaction(async (tx) => {
        const rows = await tx.select({ id: jobs.id }).from(jobs).where(eq(jobs.state, "ready")).for("update", { skipLocked: true }).orderBy(asc(jobs.id));
        const ids = rows.map((r) => Number(r.id));
        assert.deepEqual(ids, [3, 4, 5], "skip locked returns exactly the unlocked rows");
        // The claimed rows are ours: mutate them inside the same scope.
        for (const id of ids) {
          await tx.update(jobs).set({ state: "claimed", attempts: 1 }).where(eq(jobs.id, id));
        }
        return ids;
      });
      await holder.query("rollback");
      assert.deepEqual(claimed, [3, 4, 5]);
      const states = (await ctx.driver.query(`select "id", "state", "attempts" from "q08_jobs" order by "id"`)) as Array<{ id: number; state: string; attempts: number }>;
      assert.deepEqual(
        states.map((s) => s.state),
        ["ready", "ready", "claimed", "claimed", "claimed"],
      );
    } finally {
      await holder.end();
    }
  });

  test(`live q08 (${driverKind}): lock strengths are observable (key share vs update compatibility)`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const holder = await rawClient(ctx);
    try {
      await holder.query("begin");
      await holder.query(`select "k" from "q08_kv" where "k" = 'snap' for no key update`);
      // FOR KEY SHARE is compatible with FOR NO KEY UPDATE: succeeds nowait.
      const shared = await ctx.db.select({ k: kv.k }).from(kv).where(eq(kv.k, "snap")).for("key share", { noWait: true });
      assert.equal(shared.length, 1);
      // FOR UPDATE is NOT compatible: fails nowait with 55P03.
      await assert.rejects(
        async () => await ctx.db.select({ k: kv.k }).from(kv).where(eq(kv.k, "snap")).for("update", { noWait: true }),
        (err: unknown) => getSqlState(err) === "55P03",
      );
    } finally {
      await holder.query("rollback");
      await holder.end();
    }
  });

  test(`live q08 (${driverKind}): OF locks only the named from item`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const e = alias(events, "e");
    const holder = await rawClient(ctx);
    try {
      await holder.query("begin");
      // Lock a row of the JOINED table (events id 1); jobs rows stay free.
      await holder.query(`select "id" from "q08_events" where "id" = 1 for update`);
      // Locking only the from table (jobs) succeeds despite the held events
      // row: the join output row is not lock-blocked through jobs.
      const rows = await ctx.db
        .select({ id: jobs.id })
        .from(jobs)
        .innerJoin(e, sql`${e.id} = ${jobs.id}`)
        .where(sql`${jobs.id} = ${1}`)
        .for("update", { of: jobs, noWait: true });
      assert.equal(rows.length, 1);
      // Locking the joined table's rows hits the holder: NOWAIT 55P03.
      await assert.rejects(
        async () =>
          await ctx.db
            .select({ id: jobs.id })
            .from(jobs)
            .innerJoin(e, sql`${e.id} = ${jobs.id}`)
            .where(sql`${jobs.id} = ${1}`)
            .for("update", { of: e, noWait: true }),
        (err: unknown) => getSqlState(err) === "55P03",
      );
    } finally {
      await holder.query("rollback");
      await holder.end();
    }
  });

  test(`live q08 (${driverKind}): locking outside a transaction executes as one statement`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    ctx.statements.count = 0;
    const rows = await ctx.db.select({ id: jobs.id }).from(jobs).limit(2).for("update");
    assert.equal(rows.length, 2);
    assert.equal(ctx.statements.count, 1);
  });
}

// ---------------------------------------------------------------------------
// Explicit batch plans
// ---------------------------------------------------------------------------

for (const driverKind of ["postgres", "pg"] as const) {
  test(`live q08 (${driverKind}): batch returns each statement's result in order (rows, counts, returning)`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    ctx.statements.count = 0;
    const [selected, updated, inserted, counted] = await ctx.db.batch([
      ctx.db.select({ k: kv.k, v: kv.v }).from(kv).where(eq(kv.k, "snap")),
      ctx.db.update(kv).set({ v: 5 }).where(eq(kv.k, "snap")),
      ctx.db.insert(strict).values({ id: 1, label: "a" }).returning(["id"]),
      ctx.db.select({ n: count() }).from(strict),
    ]);
    assert.equal(selected.length, 1);
    assert.equal(selected[0].k, "snap");
    assert.equal(selected[0].v, 0);
    assert.equal(updated, 1);
    assert.deepEqual([...inserted].map((r) => Number(r.id)), [1]);
    assert.equal(counted[0].n, 1n);
    // Exactly the four planned statements — plus BEGIN/COMMIT are tx events,
    // not query events.
    assert.equal(ctx.statements.count, 4);
    // explain() matches what ran.
    const plan = ctx.db
      .batch([
        ctx.db.select({ k: kv.k, v: kv.v }).from(kv).where(eq(kv.k, "snap")),
        ctx.db.update(kv).set({ v: 5 }).where(eq(kv.k, "snap")),
        ctx.db.insert(strict).values({ id: 1, label: "a" }).returning(["id"]),
        ctx.db.select({ n: count() }).from(strict),
      ])
      .explain();
    assert.equal(plan.statementCount, 4);
    assert.equal(plan.transaction.ownership, "own");
  });

  test(`live q08 (${driverKind}): batch is atomic — a late failure rolls back earlier statements`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    await ctx.driver.execute(`delete from "q08_strict"`);
    await ctx.driver.execute(`update "q08_kv" set "v" = 0 where "k" = 'atomic'`);
    ctx.statements.count = 0;
    await assert.rejects(
      async () =>
        await ctx.db.batch([
          ctx.db.update(kv).set({ v: 99 }).where(eq(kv.k, "atomic")),
          ctx.db.insert(strict).values({ id: 1, label: "first" }),
          ctx.db.insert(strict).values({ id: 1, label: "duplicate pk fails here" }),
          ctx.db.insert(strict).values({ id: 2, label: "never runs" }),
        ]),
      (err: unknown) => getSqlState(err) === "23505",
    );
    const after = (await ctx.driver.query(`select "v" from "q08_kv" where "k" = 'atomic'`)) as Array<{ v: number }>;
    assert.equal(after[0].v, 0, "first statement's write must be rolled back");
    const rows = (await ctx.driver.query(`select count(*)::int as n from "q08_strict"`)) as Array<{ n: number }>;
    assert.equal(rows[0].n, 0);
    assert.equal(ctx.statements.count, 2, "exactly the two statements before the failing one complete (it emits query-error, not query-end)");
  });

  test(`live q08 (${driverKind}): owned batch defaults to one REPEATABLE READ snapshot (differential oracle)`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const GATE = 918273645n;
    /** Statement 1 blocks in q08_gate() until the writer (holding the
     *  advisory lock) has COMMITTED its update and released — so statement 2
     *  provably runs after the concurrent commit. Under REPEATABLE READ both
     *  statements read the BEGIN snapshot; under READ COMMITTED statement 2
     *  takes a fresh snapshot. */
    const runCase = async (isolation: "repeatable-read" | "read-committed"): Promise<number[]> => {
      await ctx.driver.execute(`update "q08_kv" set "v" = 0 where "k" = 'snap'`);
      const writer = await rawClient(ctx);
      const writerDone = (async () => {
        await writer.query("select pg_advisory_lock($1)", [GATE]);
        await new Promise((r) => setTimeout(r, 150));
        await writer.query(`update "q08_kv" set "v" = 42 where "k" = 'snap'`);
        await writer.query("commit").catch(() => {});
        await writer.query("begin");
        await writer.query("select pg_advisory_unlock($1)", [GATE]);
        await writer.end();
      })();
      const [a, b] = await ctx.db.batch(
        [
          ctx.db.select({ v: kv.v }).from(kv).where(sql`${kv.k} = ${"snap"} and q08_gate(${GATE})`),
          ctx.db.select({ v: kv.v }).from(kv).where(eq(kv.k, "snap")),
        ],
        { isolation },
      );
      await writerDone;
      return [(a as Array<{ v: number }>)[0].v, (b as Array<{ v: number }>)[0].v];
    };
    const rr = await runCase("repeatable-read");
    assert.deepEqual(rr, [0, 0], "REPEATABLE READ: both statements read one snapshot (taken before the concurrent commit)");
    const rc = await runCase("read-committed");
    assert.deepEqual(rc, [0, 42], "READ COMMITTED differential: the second statement sees the concurrent commit");
  });

  test(`live q08 (${driverKind}): opt-in batch retry replays the whole batch on 40001 exactly once more`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const GATE = 918273646n;
    await ctx.driver.execute(`update "q08_kv" set "v" = 0 where "k" = 'retry'`);
    const attemptEvents: number[] = [];
    ctx.listeners.push((e: SqlEvent) => {
      if (e.kind === "tx-begin" && e.attempt !== undefined) attemptEvents.push(e.attempt);
    });
    const writer = await rawClient(ctx);
    const writerDone = (async () => {
      await writer.query("select pg_advisory_lock($1)", [GATE]);
      await new Promise((r) => setTimeout(r, 150));
      await writer.query(`update "q08_kv" set "v" = 100 where "k" = 'retry'`);
      await writer.query("commit").catch(() => {});
      await writer.query("begin");
      await writer.query("select pg_advisory_unlock($1)", [GATE]);
      await writer.end();
    })();
    // REPEATABLE READ snapshot -> the gated read orders attempt 1 before the
    // writer's commit; the update then hits the concurrently-modified row:
    // serialization failure (40001) -> whole-batch retry.
    const [before] = await ctx.db.batch(
      [
        ctx.db.select({ v: kv.v }).from(kv).where(sql`${kv.k} = ${"retry"} and q08_gate(${GATE})`),
        ctx.db.update(kv).set({ v: sql`${kv.v} + ${1}` }).where(eq(kv.k, "retry")),
      ],
      { retry: { idempotent: true, maxAttempts: 3 } },
    );
    await writerDone;
    ctx.listeners.length = 0;
    // The resolved tuple comes from the SUCCESSFUL attempt (attempt 2,
    // after the writer committed): it reads the raced value 100.
    assert.equal((before as Array<{ v: number }>)[0].v, 100, "the returned tuple is the successful attempt's read (after the race)");
    const final = (await ctx.driver.query(`select "v" from "q08_kv" where "k" = 'retry'`)) as Array<{ v: number }>;
    assert.equal(final[0].v, 101, "retried attempt increments the raced value exactly once");
    assert.ok(attemptEvents.length >= 2, `retry must annotate attempts (got ${JSON.stringify(attemptEvents)})`);
  });

  test(`live q08 (${driverKind}): batch inside db.transaction joins the enclosing scope`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    // Modes/retry belong to the enclosing transaction: rejected at the
    // batch's prepare step (before any statement runs).
    await assert.rejects(
      ctx.db.transaction(async (tx) => {
        await tx.batch([tx.select({ v: kv.v }).from(kv)], { isolation: "serializable" });
      }),
      /belong to the enclosing db\.transaction/,
    );
    // Joined scope: the enclosing rollback takes the batch's writes with it.
    await ctx.db.transaction(async (tx) => {
      const [rows] = await tx.batch([tx.select({ v: kv.v }).from(kv).where(eq(kv.k, "atomic")), tx.update(kv).set({ v: 7 }).where(eq(kv.k, "atomic"))]);
      assert.equal((rows as Array<{ v: number }>)[0].v, 0);
      throw new NeutronSqlError("deliberate: enclosing rollback must discard the batch");
    }).catch(() => "rolled back");
    const after = (await ctx.driver.query(`select "v" from "q08_kv" where "k" = 'atomic'`)) as Array<{ v: number }>;
    assert.equal(after[0].v, 0);
  });

  test(`live q08 (${driverKind}): a batch with an invalid statement compiles before any connection is touched`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    ctx.statements.count = 0;
    // A compile-time failure in a later item (window in WHERE) stops the
    // whole batch before any connection is touched.
    await assert.rejects(
      async () =>
        await ctx.db.batch([
          ctx.db.update(kv).set({ v: 1 }).where(eq(kv.k, "atomic")),
          ctx.db.select().from(kv).where(sql`${over(rowNumber(), {})} = ${1}`),
        ]),
      /not allowed in WHERE/,
    );
    assert.equal(ctx.statements.count, 0);
  });
}

// ---------------------------------------------------------------------------
// Bounded streaming over server-side cursors
// ---------------------------------------------------------------------------

/** Wait until the pg pool reports every connection idle (max 5s), returning
 *  how long it took — V16's "early exit returns its connection promptly". */
async function waitIdle(pool: pg.Pool, timeoutMs = 5000): Promise<number> {
  const started = Date.now();
  while (pool.totalCount !== pool.idleCount && Date.now() - started < timeoutMs) {
    await new Promise((r) => setTimeout(r, 10));
  }
  assert.equal(pool.totalCount, pool.idleCount, `pool leak: total=${pool.totalCount} idle=${pool.idleCount}`);
  return Date.now() - started;
}

for (const driverKind of ["postgres", "pg"] as const) {
  test(`live q08 (${driverKind}): full stream consumption fetches in bounded batches and commits`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    // 10 seed rows + 240 more for batch boundaries (100/100/50 of 250).
    const extra: Array<{ queue: string; state: string; attempts: number }> = [];
    for (let i = 0; i < 240; i++) extra.push({ queue: "bulk", state: "bulk", attempts: 0 });
    await ctx.db.insert(jobs).values(extra);
    ctx.statements.count = 0;
    const txEvents: SqlEvent["kind"][] = [];
    ctx.listeners.push((e) => {
      if (e.kind === "tx-commit" || e.kind === "tx-rollback") txEvents.push(e.kind);
    });

    const batches: number[] = [];
    let total = 0;
    for await (const batch of ctx.db.select().from(jobs).where(eq(jobs.queue, "bulk")).streamBatches({ batchSize: 100 })) {
      batches.push(batch.length);
      total += batch.length;
      assert.ok(batch.length <= 100, "no batch may exceed batchSize");
    }
    assert.deepEqual(batches, [100, 100, 40], "batches are bounded and the last is short");
    assert.equal(total, 240);
    assert.deepEqual(txEvents, ["tx-commit"], "an exhausted owned stream commits its transaction");
    // DECLARE + 3 FETCH + CLOSE = 5 query statements (BEGIN/COMMIT are tx events).
    assert.equal(ctx.statements.count, 5);
    ctx.listeners.length = 0;

    // Row mode decodes through the same codecs (serial id -> number).
    const first = await ctx.db.select({ id: jobs.id, queue: jobs.queue }).from(jobs).where(eq(jobs.queue, "bulk")).stream({ batchSize: 50 });
    const row = await first.next();
    assert.equal(row.done, false);
    assert.equal(typeof (row.value as { id: unknown }).id, "number");
    await first.return();
    if (ctx.pool) await waitIdle(ctx.pool);
  });

  test(`live q08 (${driverKind}): early iterator exit returns the connection promptly and keeps the pool usable`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const pool = ctx.pool;
    const baseline = pool ? { total: pool.totalCount, idle: pool.idleCount } : null;
    ctx.statements.count = 0;
    const txEvents: SqlEvent["kind"][] = [];
    ctx.listeners.push((e) => {
      if (e.kind === "tx-commit" || e.kind === "tx-rollback") txEvents.push(e.kind);
    });
    let seen = 0;
    const start = Date.now();
    for await (const row of ctx.db.select().from(jobs).where(eq(jobs.queue, "bulk")).stream({ batchSize: 40 })) {
      seen++;
      void row;
      if (seen === 50) break;
    }
    const elapsed = Date.now() - start;
    assert.deepEqual(txEvents, ["tx-rollback"], "early exit rolls the owned transaction back");
    // DECLARE + 2 FETCH = 3 statements (the cursor dies with the ROLLBACK).
    assert.equal(ctx.statements.count, 3);
    if (pool) {
      const returnedMs = await waitIdle(pool);
      assert.ok(returnedMs < 1500, `connection return took ${returnedMs}ms after break (V16: promptly)`);
      assert.equal(pool.totalCount, baseline!.total, "no pool growth from streaming");
    }
    assert.ok(elapsed < 5000);
    // The pool serves ordinary queries immediately afterwards.
    const ok = await ctx.db.select({ n: count() }).from(jobs);
    assert.equal(typeof ok[0].n, "bigint");
    // And a second stream opens its own cursor cleanly.
    let second = 0;
    for await (const row of ctx.db.select().from(jobs).where(eq(jobs.queue, "bulk")).stream({ batchSize: 60 })) {
      second++;
      void row;
      if (second === 5) break;
    }
    assert.equal(second, 5);
    ctx.listeners.length = 0;
    if (pool) await waitIdle(pool);
  });

  test(`live q08 (${driverKind}): return() and throw() close the stream like break`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const s1 = ctx.db.select().from(jobs).where(eq(jobs.queue, "bulk")).stream({ batchSize: 25 });
    await s1.next();
    const r = await s1.return();
    assert.equal(r.done, true);
    const s2 = ctx.db.select().from(jobs).where(eq(jobs.queue, "bulk")).stream({ batchSize: 25 });
    await s2.next();
    await assert.rejects(s2.throw(new Error("consumer failed")), /consumer failed/);
    // Both cursors are gone: the table's rows are not locked by us anywhere.
    const probe = await rawClient(ctx);
    try {
      await probe.query("begin");
      await probe.query(`select "id" from "q08_jobs" where "queue" = 'bulk' for update nowait`);
      await probe.query("rollback");
    } finally {
      await probe.end();
    }
    if (ctx.pool) await waitIdle(ctx.pool);
  });

  test(`live q08 (${driverKind}): AbortSignal between pulls cancels the stream and releases its connection`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const controller = new AbortController();
    const stream = ctx.db.select().from(jobs).where(eq(jobs.queue, "bulk")).stream({ batchSize: 20, signal: controller.signal });
    const first = await stream.next();
    assert.equal(first.done, false);
    controller.abort();
    await assert.rejects(stream.next(), (err: unknown) => err instanceof QueryCanceledError);
    if (ctx.pool) await waitIdle(ctx.pool);
    if (ctx.pool) assert.equal(ctx.pool.totalCount, ctx.pool.idleCount);
  });

  test(`live q08 (${driverKind}): deadline cancels a slow in-flight FETCH at the server (SQLSTATE 57014 path)`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    // q08_nap() sleeps 50ms per row evaluated in WHERE: a FETCH of 3 rows
    // takes ~150ms, so a 60ms deadline deterministically lands mid-round-trip.
    const stream = ctx.db
      .select({ id: jobs.id })
      .from(jobs)
      .where(sql`${jobs.queue} = ${"bulk"} and q08_nap()`)
      .stream({ batchSize: 3, deadlineMs: 60 });
    await assert.rejects(
      stream.next(),
      (err: unknown) => err instanceof QueryCanceledError,
    );
    if (ctx.pool) await waitIdle(ctx.pool);
    // The nap lives in WHERE — verify it was really evaluated (the plan used
    // the function) by a direct slow query.
    const t0 = Date.now();
    const slow = await ctx.db.select({ nap: sql`q08_nap()` }).from(jobs).limit(1);
    assert.equal(slow.length, 1);
    assert.ok(Date.now() - t0 >= 40, `expected the nap function to run, took ${Date.now() - t0}ms`);
  });

  test(`live q08 (${driverKind}): stream inside db.transaction reads its own writes; scope settles cleanly`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    await assert.rejects(
      ctx.db.transaction(async (tx) => {
        await tx.select().from(kv).stream({ batchSize: 5, isolation: "serializable" });
      }),
      /takes no transaction modes/,
    );

    const out = await ctx.db.transaction(async (tx) => {
      await tx.update(kv).set({ v: 11 }).where(eq(kv.k, "snap"));
      const seen: number[] = [];
      for await (const row of tx.select({ k: kv.k, v: kv.v }).from(kv).stream({ batchSize: 1 })) {
        if (row.k === "snap") seen.push(row.v);
      }
      // Early exit must NOT poison the enclosing transaction.
      let n = 0;
      for await (const row of tx.select({ k: kv.k }).from(kv).stream({ batchSize: 1 })) {
        n++;
        void row;
        if (n === 1) break;
      }
      const after = await tx.select({ v: kv.v }).from(kv).where(eq(kv.k, "snap"));
      return { seen, after };
    });
    assert.deepEqual(out.seen, [11], "the stream sees the transaction's own uncommitted write");
    assert.deepEqual(out.after.map((r: { v: number }) => r.v), [11]);
    const committed = (await ctx.driver.query(`select "v" from "q08_kv" where "k" = 'snap'`)) as Array<{ v: number }>;
    assert.equal(committed[0].v, 11, "enclosing commit persists after streams closed");
  });

  test(`live q08 (${driverKind}): a stream used after its transaction settled is rejected, not executed`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const tx = ctx.db.transaction(async (scope) => {
      const stream = scope.select().from(kv).stream({ batchSize: 1 });
      const first = await stream.next();
      assert.equal(first.done, false);
      // Keep the stream alive past the callback: using it after settle must
      // fail without touching the released connection.
      return stream;
    });
    const stream = await tx;
    ctx.statements.count = 0;
    await assert.rejects(stream.next(), /settled/);
    assert.equal(ctx.statements.count, 0, "no statement may run on a settled scope");
    await stream.return();
    if (ctx.pool) await waitIdle(ctx.pool);
  });
}

// ---------------------------------------------------------------------------
// Concurrent consumers and pool reuse (Q08 exit: V13/V16)
// ---------------------------------------------------------------------------

for (const driverKind of ["postgres", "pg"] as const) {
  test(`live q08 (${driverKind}): concurrent owned streams interleave on separate connections with exact results`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const oracle = (await ctx.driver.query(`select "id" from "q08_jobs" where "queue" = 'bulk' order by "id"`)) as Array<{ id: number }>;
    const want = oracle.map((r) => r.id);
    assert.ok(want.length >= 100, "fixture: the bulk rows from the earlier stream test");
    const baseline = ctx.pool ? ctx.pool.totalCount : 0;

    // Three consumers (pool max is 4), each its own cursor/transaction,
    // pulled in lock-step so their FETCHes interleave on the wire.
    const streams = [7, 13, 29].map((batchSize) =>
      ctx.db.select({ id: jobs.id }).from(jobs).where(eq(jobs.queue, "bulk")).orderBy(asc(jobs.id)).stream({ batchSize }),
    );
    const got: number[][] = streams.map(() => []);
    let live = streams.length;
    const finished = streams.map(() => false);
    while (live > 0) {
      const results = await Promise.all(streams.map((s, i) => (finished[i] ? Promise.resolve(null) : s.next())));
      results.forEach((r, i) => {
        if (r === null) return;
        if (r.done) {
          finished[i] = true;
          live -= 1;
        } else {
          got[i].push(r.value.id);
        }
      });
    }
    for (const ids of got) assert.deepEqual(ids, want, "each consumer sees the full ordered result, no loss or duplication");
    if (ctx.pool) {
      await waitIdle(ctx.pool);
      assert.ok(ctx.pool.totalCount <= Math.max(baseline, 4), "never more connections than the pool allows");
    }

    // Two consumers of ONE builder: independent cursors, one exits early,
    // the other still completes.
    const q = ctx.db.select({ id: jobs.id }).from(jobs).where(eq(jobs.queue, "bulk")).orderBy(asc(jobs.id));
    const a = q.stream({ batchSize: 10 });
    const b = q.stream({ batchSize: 10 });
    assert.notEqual(a.explain().cursorName, b.explain().cursorName);
    const a1 = await a.next();
    const b1 = await b.next();
    assert.equal(a1.done === false && a1.value.id, want[0]);
    assert.equal(b1.done === false && b1.value.id, want[0]);
    await a.return();
    const rest: number[] = [b1.done === false ? b1.value.id : -1];
    for await (const row of b) rest.push(row.id);
    assert.deepEqual(rest, want);
    if (ctx.pool) await waitIdle(ctx.pool);
  });

  test(`live q08 (${driverKind}): two cursors inside one transaction interleave on its single connection`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const res = await ctx.db.transaction(async (tx) => {
      const s1 = tx.select({ k: kv.k }).from(kv).orderBy(asc(kv.k)).stream({ batchSize: 1 });
      const s2 = tx.select({ k: kv.k }).from(kv).orderBy(desc(kv.k)).stream({ batchSize: 2 });
      const x: string[] = [];
      const y: string[] = [];
      const [r1, r2] = await Promise.all([s1.next(), s2.next()]);
      if (!r1.done) x.push(r1.value.k);
      if (!r2.done) y.push(r2.value.k);
      for await (const row of s1) x.push(row.k);
      for await (const row of s2) y.push(row.k);
      return { x, y };
    });
    const oracle = ((await ctx.driver.query(`select "k" from "q08_kv" order by "k"`)) as Array<{ k: string }>).map((r) => r.k);
    assert.deepEqual(res.x, oracle);
    assert.deepEqual(res.y, [...oracle].reverse());
    if (ctx.pool) await waitIdle(ctx.pool);
  });

  test(`live q08 (${driverKind}): a stream failing mid-way rolls back and the pool is reused cleanly`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const baseline = ctx.pool ? ctx.pool.totalCount : 0;
    for (let round = 0; round < 6; round++) {
      // Division by zero surfaces as a server error (22012) on whichever
      // FETCH first evaluates a failing row (plan-dependent: a sort
      // evaluates every row on the first FETCH, an index scan later).
      const s = ctx.db
        .select({ id: jobs.id, q: sql`case when ${jobs.id} > (select min("id") + 3 from "q08_jobs" where "queue" = 'bulk') then 1 / 0 else 1 end` })
        .from(jobs)
        .where(eq(jobs.queue, "bulk"))
        .orderBy(asc(jobs.id))
        .stream({ batchSize: 2 });
      await assert.rejects(
        (async () => {
          for (;;) {
            const r = await s.next();
            if (r.done) break;
          }
        })(),
        (err: unknown) => err instanceof ServerSqlError && getSqlState(err) === "22012",
      );
    }
    if (ctx.pool) {
      await waitIdle(ctx.pool);
      assert.ok(ctx.pool.totalCount <= Math.max(baseline, 4));
    }
    // Every pooled connection serves work after the failure storm.
    const rounds = await Promise.all(Array.from({ length: 8 }, () => ctx.db.select({ n: count() }).from(kv)));
    assert.ok(rounds.every((r) => r[0].n === 3n));
  });
}
