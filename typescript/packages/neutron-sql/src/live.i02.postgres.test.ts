import assert from "node:assert/strict";
import test from "node:test";
import { after } from "node:test";
import pg from "pg";
import {
  CommitAmbiguityError,
  ConnectionFailedError,
  NeutronSqlError,
  QueryCanceledError,
  ServerSqlError,
  createDatabase,
  eq,
  getSqlState,
  integer,
  pgTable,
  serial,
  sql,
  text,
  wrapPgPool,
  wrapPostgresJs,
  type Driver,
  type PgPoolLike,
  type SqlEvent,
} from "./index.js";
import { TEST_URL, ensureLive, uniqueDbName } from "./live-harness.js";

// I02 live battery (V14 subset owned by this card): server-reaching
// cancellation via deadline AND AbortSignal, pooled-connection usability
// after cancellation, isolation/read-only/deferrable modes with observable
// semantics, savepoint nesting incl. rollback-to-middle, commit ambiguity
// (backend terminated during COMMIT — distinct state, never replayed),
// redacted default logs (canary password + parameter grep), retry bounds,
// and pool-leak checks after failure storms. Real drivers, real Postgres,
// both legs; killed-connection simulations go through the real network path
// (pg_terminate_backend on the live backend).

const kv = pgTable("i02_kv", {
  k: text("k").primaryKey(),
  v: integer("v").notNull(),
});

const notes = pgTable("i02_notes", {
  id: serial("id").primaryKey(),
  body: text("body").notNull(),
});

const ambig = pgTable("i02_ambig", {
  id: integer("id").primaryKey(),
  v: text("v").notNull(),
});

interface PgPoolMetrics {
  totalCount: number;
  idleCount: number;
}

interface I02Ctx {
  driverKind: "postgres" | "pg";
  dbUrl: string;
  db: Awaited<ReturnType<typeof createDatabase>>;
  driver: Driver;
  /** Raw underlying resource: pg Pool (with totalCount/idleCount) or
   *  postgres.js client. */
  raw: unknown;
  admin: pg.Pool;
  close(): Promise<void>;
}

const contexts: I02Ctx[] = [];

async function createCtx(driverKind: "postgres" | "pg"): Promise<I02Ctx> {
  const DB_NAME = uniqueDbName("i02");
  const admin = new pg.Pool({ connectionString: TEST_URL, max: 2 });
  await admin.query(`drop database if exists "${DB_NAME}"`);
  await admin.query(`create database "${DB_NAME}"`);

  const url = new URL(TEST_URL);
  url.pathname = `/${DB_NAME}`;
  // Borrowed wraps over a real driver resource we own, so tests can read
  // pool metrics (pg) and control teardown precisely.
  let raw: unknown;
  let driver: Driver;
  if (driverKind === "pg") {
    raw = new pg.Pool({ connectionString: url.toString(), max: 6 });
    driver = wrapPgPool(raw as unknown as PgPoolLike);
  } else {
    const postgres = (await import("postgres")) as unknown as { default: (u: string, o?: object) => import("./index.js").PostgresJsClient };
    raw = postgres.default(url.toString(), { max: 6 });
    driver = wrapPostgresJs(raw as import("./index.js").PostgresJsClient);
  }
  const db = await createDatabase({ driver, tables: { i02_kv: kv, i02_notes: notes, i02_ambig: ambig } });
  await driver.execute(`create table "i02_kv" ("k" text primary key, "v" integer not null)`);
  await driver.execute(`create table "i02_notes" ("id" serial primary key, "body" text not null)`);
  await driver.execute(`create table "i02_ambig" ("id" integer primary key, "v" text not null)`);
  await driver.execute(`insert into "i02_kv" ("k", "v") values ('pid-row', 0)`);

  const ctx: I02Ctx = {
    driverKind,
    dbUrl: url.toString(),
    db,
    driver,
    raw,
    admin,
    async close(): Promise<void> {
      await db.close();
      if (driverKind === "pg") await (raw as pg.Pool).end();
      else await (raw as { end(o?: { timeout?: number }): Promise<void> }).end({ timeout: 5 });
      await admin.query(`drop database if exists "${DB_NAME}"`);
      await admin.end();
    },
  };
  contexts.push(ctx);
  return ctx;
}

const setups: Record<string, Promise<I02Ctx> | undefined> = {};
async function ctxFor(driverKind: "postgres" | "pg"): Promise<I02Ctx | null> {
  if (!(await ensureLive(`live i02 (${driverKind})`))) return null;
  setups[driverKind] ??= createCtx(driverKind);
  return setups[driverKind]!;
}

after(async () => {
  for (const ctx of contexts) await ctx.close();
});

/** Minimal deferred (Node 20 target lib has no Promise.withResolvers). */
function defer<T>(): { promise: Promise<T>; resolve(value: T): void } {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

/** Wait until the pg pool has no checked-out clients (or time out). */
async function waitForQuiescence(ctx: I02Ctx, timeoutMs = 5000): Promise<void> {  if (ctx.driverKind !== "pg") return;
  const pool = ctx.raw as PgPoolMetrics;
  const deadline = Date.now() + timeoutMs;
  while (pool.totalCount !== pool.idleCount && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 25));
  }
  assert.equal(
    pool.totalCount,
    pool.idleCount,
    `pool leak: ${pool.totalCount - pool.idleCount} connection(s) still checked out after failure storm (total=${pool.totalCount} idle=${pool.idleCount})`,
  );
}

/** Functional no-leak oracle for both drivers: N concurrent queries over the
 * small pool all complete (a leaked/stuck pool would starve them). */
async function functionalPoolCheck(ctx: I02Ctx): Promise<void> {
  const rounds = await Promise.all(
    Array.from({ length: 12 }, () => ctx.driver.query<{ ok: number }>("select 1 as ok from pg_sleep(0.02)")),
  );
  assert.equal(rounds.every((r) => r.length === 1 && r[0].ok === 1), true);
}

/** postgres.js 3.4.8 upstream defect (reproduced on raw postgres.js, see
 * I02 evidence): a backend terminated while its connection is reserved can
 * deliver ONE stale 57P01/CONNECTION_CLOSED to the first query that later
 * touches that connection object. The error is client-side residue — the
 * statement never ran — so a retry is safe. This warms the pool until every
 * killed connection has flushed its stale error, asserting each retry
 * succeeds; the clean functional check then runs afterwards. The driver's
 * OWN begin() path crashes outright on the same kill pattern
 * (write-on-nulled-socket, reproduced) — the wrapper's pin machinery is
 * what keeps the process alive here. */
async function absorbPostgresJsStaleErrors(ctx: I02Ctx, expectedKills: number): Promise<void> {
  if (ctx.driverKind !== "postgres") return;
  // CONCURRENT warm-up: sequential queries would keep landing on the one
  // healthy connection; concurrency forces the pool to touch every
  // connection object, flushing each killed connection's stale error.
  const warm = Array.from({ length: 6 }, () =>
    (async () => {
      for (let attempt = 0; ; attempt++) {
        try {
          await ctx.driver.query("select 1 as ok");
          return;
        } catch (err) {
          const state = getSqlState(err);
          const kindOk = err instanceof ConnectionFailedError || state === "57P01";
          assert.ok(kindOk && attempt < 6, `unexpected post-storm error: ${String(err)}`);
        }
      }
    })(),
  );
  await Promise.all(warm);
  void expectedKills;
}

// ---------------------------------------------------------------------------
// Cancellation (deadline + AbortSignal) reaching the server
// ---------------------------------------------------------------------------

for (const driverKind of ["postgres", "pg"] as const) {
  test(`live i02 (${driverKind}): deadline cancels pg_sleep AT THE SERVER; pooled connection stays usable`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const started = Date.now();
    const err = await ctx.driver.query("select 1 as x from pg_sleep(4)", [], { deadlineMs: 200 }).then(
      () => null,
      (e: unknown) => e,
    );
    const elapsed = Date.now() - started;
    assert.ok(err instanceof QueryCanceledError, `expected QueryCanceledError, got ${String(err)}`);
    assert.equal(err.reason, "deadline");
    assert.equal(err.dispatched, true);
    assert.equal(err.sqlstate, "57014");
    assert.ok(err instanceof ServerSqlError, "QueryCanceledError is a ServerSqlError (SQLSTATE survives)");
    assert.equal(getSqlState(err), "57014");
    assert.ok(elapsed < 2500, `query should end near the deadline, took ${elapsed}ms`);
    assert.ok(elapsed >= 150, `cancel must have waited for the deadline, took ${elapsed}ms`);
    // The canceled connection returns to the pool CLEAN: a subsequent query
    // works and (pg) the pool has no stuck checkout.
    const usable = await ctx.driver.query<{ u: number }>("select 41 + 1 as u");
    assert.equal(usable[0].u, 42);
    await waitForQuiescence(ctx);
    await functionalPoolCheck(ctx);
  });

  test(`live i02 (${driverKind}): AbortSignal cancels mid-flight; connection usable after`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const controller = new AbortController();
    setTimeout(() => controller.abort(), 200);
    const started = Date.now();
    const err = await ctx.driver.query("select 1 as x from pg_sleep(4)", [], { signal: controller.signal }).then(
      () => null,
      (e: unknown) => e,
    );
    const elapsed = Date.now() - started;
    assert.ok(err instanceof QueryCanceledError, `expected QueryCanceledError, got ${String(err)}`);
    assert.equal(err.reason, "signal");
    assert.equal(err.sqlstate, "57014");
    assert.ok(elapsed < 2500, `query should end near the abort, took ${elapsed}ms`);
    const usable = await ctx.driver.execute("select 1");
    assert.equal(typeof usable, "number");
    await waitForQuiescence(ctx);
  });

  test(`live i02 (${driverKind}): pre-aborted signal rejects WITHOUT a server round trip`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const controller = new AbortController();
    controller.abort();
    const started = Date.now();
    const err = await ctx.driver.query("select 1 as x from pg_sleep(4)", [], { signal: controller.signal }).then(
      () => null,
      (e: unknown) => e,
    );
    assert.ok(err instanceof QueryCanceledError);
    assert.equal(err.reason, "signal");
    assert.equal(err.dispatched, false, "nothing was sent to the server");
    assert.ok(Date.now() - started < 500, "must reject immediately");
  });

  test(`live i02 (${driverKind}): invalid deadlineMs is rejected before touching a connection`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    for (const bad of [0, -5, Number.NaN, Number.POSITIVE_INFINITY]) {
      const err = await ctx.driver.query("select 1", [], { deadlineMs: bad }).then(
        () => null,
        (e: unknown) => e,
      );
      assert.ok(err instanceof NeutronSqlError, `deadlineMs ${String(bad)} must fail, got ${String(err)}`);
      assert.match(err.message, /deadlineMs/);
    }
  });

  test(`live i02 (${driverKind}): cancel mid-transaction rolls the transaction back and propagates`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    await ctx.driver.execute(`insert into "i02_notes" ("body") values ('before')`);
    const err = await ctx.driver
      .begin(async (tx) => {
        await tx.execute(`insert into "i02_notes" ("body") values ('inside')`);
        await tx.query("select 1 as x from pg_sleep(4)", [], { deadlineMs: 200 });
      })
      .then(
        () => null,
        (e: unknown) => e,
      );
    assert.ok(err instanceof QueryCanceledError, `expected the cancellation to propagate, got ${String(err)}`);
    const rows = await ctx.driver.query<{ body: string }>(`select "body" from "i02_notes" order by "id"`);
    assert.deepEqual(
      rows.map((r) => r.body),
      ["before"],
      "the transaction must be rolled back after the canceled statement",
    );
    await waitForQuiescence(ctx);
    await functionalPoolCheck(ctx);
  });

  // -------------------------------------------------------------------------
  // Isolation / read-only / deferrable — observable semantics
  // -------------------------------------------------------------------------

  test(`live i02 (${driverKind}): isolation levels map to what PG actually supports and are observable`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const cases: Array<["read-committed" | "repeatable-read" | "serializable", string]> = [
      ["read-committed", "read committed"],
      ["repeatable-read", "repeatable read"],
      ["serializable", "serializable"],
    ];
    for (const [level, pgName] of cases) {
      const seen: string | undefined = await ctx.driver.begin(async (tx) => {
        const rows = await tx.query<{ v: string }>("select current_setting('transaction_isolation') as v");
        return rows[0].v;
      }, { isolation: level });
      assert.equal(seen, pgName, `isolation ${level} must be applied by BEGIN`);
    }
    const ro = await ctx.driver.begin(async (tx) => (await tx.query<{ v: string }>("select current_setting('transaction_read_only') as v"))[0].v, { readOnly: true });
    assert.equal(ro, "on");
    const def = await ctx.driver.begin(
      async (tx) => (await tx.query<{ v: string }>("select current_setting('transaction_deferrable') as v"))[0].v,
      { isolation: "serializable", readOnly: true, deferrable: true },
    );
    assert.equal(def, "on");
  });

  test(`live i02 (${driverKind}): read-only transactions actually reject writes`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const err = await ctx.driver
      .begin(async (tx) => tx.execute(`insert into "i02_notes" ("body") values ('nope')`), { readOnly: true })
      .then(
        () => null,
        (e: unknown) => e,
      );
    assert.ok(err instanceof ServerSqlError, `expected a server error, got ${String(err)}`);
    assert.equal(err.sqlstate, "25006");
  });

  test(`live i02 (${driverKind}): unsupported mode combos fail BEFORE any SQL or connection use`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const events: SqlEvent[] = [];
    const err = await ctx.db
      .transaction(async () => "unreachable", { deferrable: true })
      .then(
        () => null,
        (e: unknown) => e,
      );
    assert.ok(err instanceof NeutronSqlError);
    assert.match(err.message, /deferrable transactions require/);
    void events;
  });

  // -------------------------------------------------------------------------
  // Savepoints
  // -------------------------------------------------------------------------

  test(`live i02 (${driverKind}): nested transactions are real savepoints — inner failure rolls back only the inner work`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    await ctx.driver.execute(`delete from "i02_notes"`);
    await ctx.db.transaction(async (tx) => {
      await tx.insert(notes).values({ body: "outer-1" });
      const inner = await tx
        .transaction(async (tx2) => {
          await tx2.insert(notes).values({ body: "inner-should-vanish" });
          throw new Error("inner failure");
        })
        .then(
          () => "unexpected",
          (e: unknown) => (e as Error).message,
        );
      assert.equal(inner, "inner failure");
      await tx.insert(notes).values({ body: "outer-2" });
    });
    const rows = await ctx.driver.query<{ body: string }>(`select "body" from "i02_notes" order by "id"`);
    assert.deepEqual(
      rows.map((r) => r.body),
      ["outer-1", "outer-2"],
    );
  });

  test(`live i02 (${driverKind}): explicit savepoints — rollback-to-middle keeps earlier work and the savepoint itself`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    await ctx.driver.execute(`delete from "i02_notes"`);
    await ctx.db.transaction(async (tx) => {
      const spA = await tx.savepoint("sp_a");
      await tx.insert(notes).values({ body: "after-a" });
      const spB = await tx.savepoint("sp_b");
      await tx.insert(notes).values({ body: "after-b" });
      const spC = await tx.savepoint("sp_c");
      await tx.insert(notes).values({ body: "after-c" });
      // Roll back to the MIDDLE savepoint: work after sp_b vanishes, work
      // after sp_a survives, and sp_b itself stays usable. sp_c was nested
      // inside the rolled-back region, so PG discarded it.
      await spB.rollbackTo();
      // Releasing the discarded savepoint fails — and the failure ABORTS
      // the transaction (subsequent commands are ignored). ROLLBACK TO
      // SAVEPOINT is exactly the recovery move: it works while aborted.
      await spC.release().then(
        () => assert.fail("released a savepoint nested inside a rolled-back region"),
        (e: unknown) => assert.match(String((e as Error).message), /does not exist|invalid savepoint|current transaction is aborted/i),
      );
      await spB.rollbackTo();
      await tx.insert(notes).values({ body: "after-b3" });
      await spB.release();
      await spA.release();
    });
    const rows = await ctx.driver.query<{ body: string }>(`select "body" from "i02_notes" order by "id"`);
    assert.deepEqual(
      rows.map((r) => r.body),
      ["after-a", "after-b3"],
    );
  });

  // -------------------------------------------------------------------------
  // Serializable conflicts + opt-in retry
  // -------------------------------------------------------------------------

  test(`live i02 (${driverKind}): serializable conflict surfaces as 40001 (observable semantics)`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    await ctx.driver.execute(`delete from "i02_kv"`);
    await ctx.driver.execute(`insert into "i02_kv" ("k", "v") values ('x', 0)`);
    let attempts = 0;
    const err = await ctx.db
      .transaction(
        async (tx) => {
          attempts++;
          await tx.select().from(kv).where(eq(kv.k, "x"));
          await ctx.driver.execute(`update "i02_kv" set "v" = "v" + 1 where "k" = 'x'`);
          await tx.update(kv).set({ v: 500 }).where(eq(kv.k, "x"));
        },
        { isolation: "serializable" },
      )
      .then(
        () => null,
        (e: unknown) => e,
      );
    assert.equal(attempts, 1, "no retry by default");
    assert.ok(err instanceof ServerSqlError, `expected a server error, got ${String(err)}`);
    assert.equal(err.sqlstate, "40001", "serialization failure must surface as SQLSTATE 40001");
    assert.equal(getSqlState(err), "40001");
    const final = await ctx.driver.query<{ v: number }>(`select "v" from "i02_kv" where "k" = 'x'`);
    assert.equal(final[0].v, 1, "only the interferer's write survived");
  });

  test(`live i02 (${driverKind}): opt-in retry recovers from a 40001 conflict`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    await ctx.driver.execute(`delete from "i02_kv"`);
    await ctx.driver.execute(`insert into "i02_kv" ("k", "v") values ('x', 0)`);
    let attempts = 0;
    const out = await ctx.db.transaction(
      async (tx) => {
        attempts++;
        await tx.select().from(kv).where(eq(kv.k, "x"));
        if (attempts === 1) {
          await ctx.driver.execute(`update "i02_kv" set "v" = "v" + 1 where "k" = 'x'`);
        }
        await tx.update(kv).set({ v: 900 + attempts }).where(eq(kv.k, "x"));
        return attempts;
      },
      { isolation: "serializable", retry: { maxAttempts: 3, idempotent: true, backoffMs: 0 } },
    );
    assert.equal(out, 2, "the second attempt should succeed");
    assert.equal(attempts, 2);
    const final = await ctx.driver.query<{ v: number }>(`select "v" from "i02_kv" where "k" = 'x'`);
    assert.equal(final[0].v, 902);
  });

  test(`live i02 (${driverKind}): retry bounds honored — persistent conflict exhausts attempts, then the error propagates`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    await ctx.driver.execute(`delete from "i02_kv"`);
    await ctx.driver.execute(`insert into "i02_kv" ("k", "v") values ('x', 0)`);
    let attempts = 0;
    const err = await ctx.db
      .transaction(
        async (tx) => {
          attempts++;
          await tx.select().from(kv).where(eq(kv.k, "x"));
          await ctx.driver.execute(`update "i02_kv" set "v" = "v" + 1 where "k" = 'x'`);
          await tx.update(kv).set({ v: 900 }).where(eq(kv.k, "x"));
        },
        { isolation: "serializable", retry: { maxAttempts: 3, idempotent: true, backoffMs: 0 } },
      )
      .then(
        () => null,
        (e: unknown) => e,
      );
    assert.equal(attempts, 3, "maxAttempts bounds total executions");
    assert.ok(err instanceof ServerSqlError);
    assert.equal(err.sqlstate, "40001");
  });

  test(`live i02 (${driverKind}): retry refuses without the idempotency assertion; non-retriable errors never retry`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const err = await ctx.db
      .transaction(async () => "unreachable", {
        isolation: "serializable",
        retry: { maxAttempts: 3 } as never,
      })
      .then(
        () => null,
        (e: unknown) => e,
      );
    assert.ok(err instanceof NeutronSqlError);
    assert.match(err.message, /idempotent: true/);

    await ctx.driver.execute(`insert into "i02_notes" ("id", "body") values (2, 'dup-anchor')`);
    let attempts = 0;
    const dupErr = await ctx.db
      .transaction(async (tx) => {
        attempts++;
        await tx.insert(notes).values({ id: 2, body: "dup" });
      }, { retry: { maxAttempts: 3, idempotent: true, backoffMs: 0 } })
      .then(
        () => null,
        (e: unknown) => e,
      );
    assert.equal(attempts, 1, "23505 is not a retryable class");
    assert.ok(dupErr instanceof ServerSqlError);
    assert.equal(dupErr.sqlstate, "23505");
  });

  // -------------------------------------------------------------------------
  // Commit ambiguity
  // -------------------------------------------------------------------------

  test(`live i02 (${driverKind}): connection killed during COMMIT is a DISTINCT ambiguous state, never replayed`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    await ctx.driver.execute(`create function "i02_slow_commit"() returns trigger as $$ begin perform pg_sleep(3); return new; end $$ language plpgsql`);
    await ctx.driver.execute(
      `create constraint trigger "i02_slow" after insert on "i02_ambig" deferrable initially deferred for each row execute function "i02_slow_commit"()`,
    );
    await ctx.driver.execute(`insert into "i02_kv" ("k", "v") values ('pid-row', 0) on conflict ("k") do nothing`);

    let attempts = 0;
    const fnDone = defer<number>();
    const txPromise = ctx.db.transaction(
      async (tx) => {
        attempts++;
        const pidRows = await tx.select({ pid: sql`pg_backend_pid()::int` }).from(kv);
        // The insert MUST go through the transaction's own connection so
        // the deferred trigger fires while THIS transaction commits.
        await tx.insert(ambig).values({ id: 1, v: "x" });
        fnDone.resolve(Number((pidRows[0] as { pid: number }).pid));
      },
      { retry: { maxAttempts: 3, idempotent: true, backoffMs: 0 } },
    );
    // attach the rejection handler before the await gap (the kill can
    // reject the transaction while the orchestrator waits)
    const settled = txPromise.then(
      () => null,
      (e: unknown) => e,
    );
    const pid = await fnDone.promise;
    // The callback has resolved; COMMIT is now sleeping inside the deferred
    // trigger. Terminate the backend through the real network path.
    await new Promise((r) => setTimeout(r, 300));
    await ctx.admin.query("select pg_terminate_backend($1)", [pid]);
    const err = await settled;
    assert.ok(err instanceof CommitAmbiguityError, `expected CommitAmbiguityError, got ${String(err)}`);
    assert.match(err.message, /outcome unknown/);
    assert.ok(err.cause instanceof Error, "the connection failure is preserved as cause");
    assert.ok(
      err.cause instanceof ConnectionFailedError || (err.cause instanceof ServerSqlError && /^57P\d\d$/.test(err.cause.sqlstate)),
      `cause must be the connection loss (got ${String(err.cause)})`,
    );
    assert.equal(attempts, 1, "an unknown commit outcome is NEVER replayed, even with retry armed");
    // The trigger's 3s sleep has been cut by the termination; give the
    // server a moment, then verify the row is absent (rolled back) and the
    // pool recovered.
    await new Promise((r) => setTimeout(r, 400));
    const rows = await ctx.driver.query<{ n: number }>(`select count(*)::int as n from "i02_ambig"`);
    assert.equal(rows[0].n, 0);
    const usable = await ctx.driver.query<{ u: number }>("select 41 + 1 as u");
    assert.equal(usable[0].u, 42);
    await waitForQuiescence(ctx);
  });

  // -------------------------------------------------------------------------
  // Observability: redaction + event kinds
  // -------------------------------------------------------------------------

  test(`live i02 (${driverKind}): default logs contain no parameter values and no connection password (canary grep)`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const canaryValue = `i02_param_canary_${process.pid}`;
    const canaryPassword = `i02_pw_canary_${process.pid}`;
    const lines: string[] = [];
    const originalLog = console.log;
    // Connect as a throwaway login role whose real password is the canary,
    // so the connection string carries a secret the logger must never emit
    // and authentication succeeds whether the server uses trust or
    // password auth.
    const canaryRole = `i02_canary_${process.pid}_${driverKind}`;
    await ctx.admin.query(`drop role if exists "${canaryRole}"`);
    await ctx.admin.query(`create role "${canaryRole}" login password '${canaryPassword}'`);
    await ctx.driver.execute(`grant usage on schema public to "${canaryRole}"`);
    await ctx.driver.execute(`grant select, insert on "i02_notes" to "${canaryRole}"`);
    await ctx.driver.execute(`grant usage on sequence "i02_notes_id_seq" to "${canaryRole}"`);
    const withPw = new URL(ctx.dbUrl);
    withPw.username = canaryRole;
    withPw.password = canaryPassword;
    let canaryRaw: unknown = null;
    let db: Awaited<ReturnType<typeof createDatabase>> | null = null;
    try {
      console.log = (...args: unknown[]) => lines.push(args.map(String).join(" "));
      let driver: Driver;
      if (driverKind === "pg") {
        canaryRaw = new pg.Pool({ connectionString: withPw.toString(), max: 2 });
        driver = wrapPgPool(canaryRaw as unknown as PgPoolLike);
      } else {
        const postgres = (await import("postgres")) as unknown as { default: (u: string, o?: object) => import("./index.js").PostgresJsClient };
        canaryRaw = postgres.default(withPw.toString(), { max: 2 });
        driver = wrapPostgresJs(canaryRaw as import("./index.js").PostgresJsClient);
      }
      db = await createDatabase({ driver, tables: { i02_notes: notes }, logger: true });
      await db.insert(notes).values({ body: canaryValue });
      await db.select().from(notes).where(eq(notes.body, canaryValue));
    } finally {
      console.log = originalLog;
      await db?.close();
      if (canaryRaw !== null) {
        if (driverKind === "pg") await (canaryRaw as pg.Pool).end();
        else await (canaryRaw as { end(o?: { timeout?: number }): Promise<void> }).end({ timeout: 5 });
      }
      await ctx.driver.execute(`drop owned by "${canaryRole}"`);
      await ctx.admin.query(`drop role if exists "${canaryRole}"`);
    }
    assert.ok(lines.length >= 3, `expected JSON lines from the default logger, got ${lines.length}`);
    const all = lines.join("\n");
    for (const line of lines) {
      assert.ok(line.startsWith("[neutron-sql] {"), `default logger emits JSON lines, got: ${line.slice(0, 60)}`);
    }
    assert.ok(!all.includes(canaryValue), "parameter values must be absent from default logs");
    assert.ok(!all.includes(canaryPassword), "connection-string passwords must never be logged");
    assert.ok(!/"params"/.test(all), "no params field in default output");
    // Statement identity is still observable: statement ids and kinds.
    const parsed = lines.map((l) => JSON.parse(l.slice("[neutron-sql] ".length))) as Array<Record<string, unknown>>;
    assert.ok(parsed.every((e) => typeof e.statementId === "string" && e.statementId.length === 16));
    assert.ok(parsed.some((e) => e.kind === "query-begin" && typeof e.sql === "string"));
    assert.ok(parsed.some((e) => e.kind === "query-end" && typeof e.durationMs === "number"));

    // Opt-in: NEUTRON_SQL_LOG_PARAMS=1 is the ONLY way parameters appear
    // (same events, env flipped; the docs warn about this mode).
    const previous = process.env.NEUTRON_SQL_LOG_PARAMS;
    const optLines: string[] = [];
    try {
      process.env.NEUTRON_SQL_LOG_PARAMS = "1";
      let optRaw: unknown;
      let driver2: Driver;
      if (driverKind === "pg") {
        optRaw = new pg.Pool({ connectionString: ctx.dbUrl, max: 2 });
        driver2 = wrapPgPool(optRaw as unknown as PgPoolLike);
      } else {
        const postgres = (await import("postgres")) as unknown as { default: (u: string, o?: object) => import("./index.js").PostgresJsClient };
        optRaw = postgres.default(ctx.dbUrl, { max: 2 });
        driver2 = wrapPostgresJs(optRaw as import("./index.js").PostgresJsClient);
      }
      const db2 = await createDatabase({ driver: driver2, tables: { i02_notes: notes }, logger: true });
      try {
        console.log = (...args: unknown[]) => optLines.push(args.map(String).join(" "));
        await db2.select().from(notes).where(eq(notes.body, canaryValue));
      } finally {
        console.log = originalLog;
        await db2.close();
        if (driverKind === "pg") await (optRaw as pg.Pool).end();
        else await (optRaw as { end(o?: { timeout?: number }): Promise<void> }).end({ timeout: 5 });
      }
    } finally {
      if (previous === undefined) delete process.env.NEUTRON_SQL_LOG_PARAMS;
      else process.env.NEUTRON_SQL_LOG_PARAMS = previous;
    }
    const optAll = optLines.join("\n");
    assert.ok(optAll.includes(canaryValue), "the opt-in mode is the ONLY way parameters appear");
  });

  test(`live i02 (${driverKind}): structured events cover query/tx/savepoint/cancel kinds with durations and ids`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;
    const events: SqlEvent[] = [];
    const db = await createDatabase({
      driver: ctx.driver,
      tables: { i02_notes: notes, i02_kv: kv },
      logger: (e) => events.push(e),
    });
    await ctx.driver.execute(`insert into "i02_kv" ("k", "v") values ('event-row', 1) on conflict ("k") do update set "v" = 1`);
    await db.transaction(async (tx) => {
      await tx.insert(notes).values({ body: "events" });
      await tx.transaction(async (tx2) => {
        await tx2.insert(notes).values({ body: "inner-events" });
        throw new Error("rollback the savepoint");
      }).then(
        () => assert.fail("inner should reject"),
        (e: unknown) => void e,
      );
    });
    // A builder-executed statement with a deadline: query-begin -> cancel
    // dispatched -> query-error, all through the logger seam.
    const cancelErr = await db
      .select({ slow: sql`(select 1 from pg_sleep(2))` })
      .from(kv)
      .execute({ deadlineMs: 150 })
      .then(
        () => null,
        (e: unknown) => e,
      );
    assert.ok(cancelErr instanceof QueryCanceledError);
    // A builder-executed failing statement: query-error.
    await ctx.driver.execute(`insert into "i02_notes" ("id", "body") values (3, 'events-anchor')`);
    await db.insert(notes).values({ id: 3, body: "conflict" }).then(
      () => assert.fail("expected the duplicate to fail"),
      (e: unknown) => assert.ok(e instanceof ServerSqlError),
    );
    const kinds = new Set(events.map((e) => e.kind));
    for (const expected of ["query-begin", "query-end", "query-error", "cancel", "tx-begin", "tx-commit", "savepoint"]) {
      assert.ok(kinds.has(expected as SqlEvent["kind"]), `event kind ${expected} missing (saw ${[...kinds].join(",")})`);
    }
    const spEvents = events.filter((e) => e.kind === "savepoint");
    assert.deepEqual(spEvents.map((e) => e.savepointAction), ["create", "rollback-to", "release"]);
    const endEvents = events.filter((e) => e.kind === "query-end");
    assert.ok(endEvents.every((e) => typeof e.durationMs === "number" && e.statementId.length === 16));
    const txBegin = events.find((e) => e.kind === "tx-begin");
    assert.ok(txBegin?.txId);
    const commit = events.find((e) => e.kind === "tx-commit");
    assert.equal(commit?.txId, txBegin?.txId);
    assert.equal(typeof commit?.durationMs, "number");
    const cancel = events.find((e) => e.kind === "cancel");
    assert.equal(cancel?.cancelReason, "deadline");
    const cancelQueryError = events.find((e) => e.kind === "query-error" && e.error?.sqlstate === "57014");
    assert.ok(cancelQueryError, "the canceled statement emits a query-error with its SQLSTATE");
    const dupQueryError = events.find((e) => e.kind === "query-error" && e.error?.sqlstate === "23505");
    assert.ok(dupQueryError, "the failed insert emits a query-error with its SQLSTATE");
  });

  // -------------------------------------------------------------------------
  // Failure storms + pool leak checks
  // -------------------------------------------------------------------------

  test(`live i02 (${driverKind}): no connection leaks after begin/rollback/cancel failure storms`, async () => {
    const ctx = await ctxFor(driverKind);
    if (!ctx) return;

    // Storm A: SQL errors inside transactions (rollback path). Each attempt
    // inserts an explicit duplicate primary key -> real 23505 from the
    // server, then the transaction rolls back.
    await ctx.driver.execute(`insert into "i02_notes" ("id", "body") values (1, 'kept-anchor-a')`);
    for (let i = 0; i < 5; i++) {
      const err: unknown = await ctx.db.transaction(async (tx) => {
        await tx.insert(notes).values({ body: `stormA-${i}-before` });
        await tx.insert(notes).values({ id: 1, body: `stormA-${i}-conflict` });
        await tx.insert(notes).values({ body: `stormA-${i}-after` });
      }).then(
        () => null,
        (e: unknown) => e,
      );
      assert.ok(err instanceof ServerSqlError, `storm A must fail with a server error, got ${String(err)}`);
      assert.equal(err.sqlstate, "23505");
    }
    await waitForQuiescence(ctx);

    // Storm B: backend killed mid-transaction (dead connection through the
    // real network path; rollback must fail; pool must discard the client).
    for (let i = 0; i < 5; i++) {
      const fnDone = defer<number>();
      const txPromise = ctx.driver.begin(async (tx) => {
        const pidRows = await tx.query<{ pid: number }>("select pg_backend_pid() as pid");
        await tx.execute(`insert into "i02_notes" ("body") values ('stormB')`);
        fnDone.resolve(pidRows[0].pid);
        await tx.query("select 1 as x from pg_sleep(4)"); // killed during this
      });
      // attach the rejection handler BEFORE any await gap (the kill below
      // can reject the transaction while we are waiting on fnDone)
      const settled = txPromise.then(
        () => null,
        (e: unknown) => e,
      );
      const pid = await fnDone.promise;
      await ctx.admin.query("select pg_terminate_backend($1)", [pid]);
      const err = await settled;
      assert.ok(err instanceof Error, `storm B iteration ${i} must fail, got ${String(err)}`);
      // the process must survive the unhandled-error hazard (the pin's
      // error listener owns the dying socket)
      // Let the driver's close/reconnect bookkeeping settle before the
      // next iteration hammers the pool again.
      await new Promise((r) => setTimeout(r, 150));
    }
    await waitForQuiescence(ctx);
    await absorbPostgresJsStaleErrors(ctx, 5);
    await functionalPoolCheck(ctx);

    // Storm C: canceled queries (deadline) at the pool level.
    for (let i = 0; i < 5; i++) {
      const err = await ctx.driver.query("select 1 as x from pg_sleep(2)", [], { deadlineMs: 80 }).then(
        () => null,
        (e: unknown) => e,
      );
      assert.ok(err instanceof QueryCanceledError);
    }
    await waitForQuiescence(ctx);
    await functionalPoolCheck(ctx);

    // Durable state: storm A rolled everything back; B rolled back; the
    // pool still serves real queries.
    const rows = await ctx.driver.query<{ body: string }>(`select "body" from "i02_notes"`);
    assert.ok(rows.every((r) => !r.body.includes("stormA-")), "storm A transactions rolled back");
    assert.ok(rows.every((r) => r.body !== "stormB"), "storm B transactions rolled back");
  });
}

// pg-only: exact pool-metric leak assertion after the whole battery.
test("live i02 (pg): pool fully quiescent after the battery (no stuck checkouts)", async () => {
  const ctx = await ctxFor("pg");
  if (!ctx) return;
  await ctx.driver.query("select 1 as ok");
  await waitForQuiescence(ctx);
  const pool = ctx.raw as PgPoolMetrics;
  assert.ok(pool.totalCount >= 1);
  assert.equal(pool.totalCount, pool.idleCount);
});
