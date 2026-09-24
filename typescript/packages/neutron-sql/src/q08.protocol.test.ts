import assert from "node:assert/strict";
import test from "node:test";
import {
  QueryCanceledError,
  createDatabase,
  eq,
  integer,
  makeLifecycle,
  pgTable,
  serial,
  text,
  type Driver,
  type PinnedExecutor,
} from "./index.js";

// ---------------------------------------------------------------------------
// Q08 — stream / batch protocol against a recording fake adapter (unit leg).
// No database: the fake pins "connections", records every statement per pin
// and serves FETCH FORWARD n from a fixed row set. Pins the lifecycle
// contract independently of Postgres: exact statement sequences, rollback on
// early exit, exactly-once release, one pin per concurrent consumer, no
// statements after a scope settles, fail-closed adapters. Live twins (real
// drivers, pool metrics) are in live.q08.postgres.test.ts.
// ---------------------------------------------------------------------------

const items = pgTable("items", {
  id: serial("id").primaryKey(),
  n: integer("n").notNull(),
  label: text("label"),
});

interface FakePin {
  readonly id: number;
  readonly log: string[];
  released: number;
  releasedWith: unknown;
}

function fakeDriver(rowCount: number, opts: { pinnable?: boolean; failFetchAt?: number } = {}) {
  const pins: FakePin[] = [];
  const rows = Array.from({ length: rowCount }, (_, i) => ({ id: i + 1, n: i * 10, label: `r${i + 1}` }));
  const rootLog: string[] = [];
  const cursorPos = new Map<string, number>();
  let fetches = 0;

  const serve = (log: string[], sql: string): Record<string, unknown>[] => {
    log.push(sql);
    const m = /^fetch forward (\d+) from "([^"]+)"$/.exec(sql);
    if (m) {
      fetches += 1;
      if (opts.failFetchAt !== undefined && fetches === opts.failFetchAt) throw new Error("fake: fetch failed");
      const pos = cursorPos.get(m[2]) ?? 0;
      const batch = rows.slice(pos, pos + Number(m[1]));
      cursorPos.set(m[2], pos + batch.length);
      return batch.map((r) => ({ ...r }));
    }
    if (sql.startsWith("select version()")) return [{ version: "PostgreSQL 17.11 on fake" }];
    if (sql.startsWith("select")) return rows.map((r) => ({ ...r }));
    return [];
  };

  const pin = async (): Promise<PinnedExecutor> => {
    const p: FakePin = { id: pins.length + 1, log: [], released: 0, releasedWith: undefined };
    pins.push(p);
    return {
      async query<T>(sql: string): Promise<T[]> {
        return serve(p.log, sql) as T[];
      },
      async execute(sql: string): Promise<number> {
        serve(p.log, sql);
        return 1;
      },
      release(err?: unknown): void {
        p.released += 1;
        p.releasedWith = err;
      },
    };
  };

  const driver: Driver = {
    async query<T>(sql: string): Promise<T[]> {
      return serve(rootLog, sql) as T[];
    },
    async execute(sql: string): Promise<number> {
      serve(rootLog, sql);
      return 1;
    },
    async begin<T>(fn: (tx: Driver) => Promise<T>): Promise<T> {
      return fn(driver);
    },
    close: () => driver.lifecycle.terminate(),
    lifecycle: makeLifecycle("borrowed", async () => {}),
  };
  if (opts.pinnable !== false) driver.pin = pin;
  return { driver, pins, rootLog };
}

const DECLARE = /^declare "neutron_cursor_\d+" no scroll cursor for select /;

test("stream protocol: exhaustion = BEGIN, DECLARE, FETCH… CLOSE, COMMIT on one pin, released once", async () => {
  const { driver, pins } = fakeDriver(5);
  const db = await createDatabase({ driver });
  const got: number[] = [];
  for await (const row of db.select().from(items).stream({ batchSize: 2 })) got.push(row.id);
  assert.deepEqual(got, [1, 2, 3, 4, 5]);
  assert.equal(pins.length, 1);
  const log = pins[0].log;
  assert.equal(log[0], "begin");
  assert.match(log[1], DECLARE);
  assert.deepEqual(log.slice(2, 5).map((s) => s.replace(/"neutron_cursor_\d+"/, "C")), ["fetch forward 2 from C", "fetch forward 2 from C", "fetch forward 2 from C"]);
  assert.match(log[5], /^close "neutron_cursor_\d+"$/);
  assert.equal(log[6], "commit");
  assert.equal(log.length, 7);
  assert.equal(pins[0].released, 1);
  assert.equal(pins[0].releasedWith, undefined);
});

test("stream protocol: exact multiple of batchSize ends with an empty FETCH, never an empty batch", async () => {
  const { driver, pins } = fakeDriver(4);
  const db = await createDatabase({ driver });
  const batches: number[] = [];
  for await (const b of db.select().from(items).streamBatches({ batchSize: 2 })) batches.push(b.length);
  assert.deepEqual(batches, [2, 2]);
  assert.equal(pins[0].log.filter((s) => s.startsWith("fetch")).length, 3);
  assert.equal(pins[0].log.at(-1), "commit");
});

test("stream protocol: early exit rolls back and releases before the loop continues", async () => {
  const { driver, pins } = fakeDriver(50);
  const db = await createDatabase({ driver });
  let seen = 0;
  for await (const row of db.select().from(items).stream({ batchSize: 10 })) {
    void row;
    if (++seen === 3) break;
  }
  const log = pins[0].log;
  assert.equal(log.filter((s) => s.startsWith("fetch")).length, 1, "no fetch beyond what the consumer pulled");
  assert.equal(log.at(-1), "rollback");
  assert.ok(!log.includes("commit"));
  assert.equal(pins[0].released, 1, "released exactly once, synchronously with break");
});

test("stream protocol: nothing is pinned until the first pull; return() before it is a no-op", async () => {
  const { driver, pins } = fakeDriver(3);
  const db = await createDatabase({ driver });
  const s = db.select().from(items).stream();
  assert.equal(pins.length, 0);
  assert.deepEqual(await s.return(), { done: true, value: undefined });
  assert.equal(pins.length, 0);
  assert.deepEqual(await s.next(), { done: true, value: undefined });
  assert.equal(pins.length, 0);
});

test("stream protocol: concurrent consumers get their own pins and cursors", async () => {
  const { driver, pins } = fakeDriver(6);
  const db = await createDatabase({ driver });
  const q = db.select().from(items);
  const a = q.stream({ batchSize: 4 });
  const b = q.stream({ batchSize: 4 });
  const [a1, b1] = await Promise.all([a.next(), b.next()]);
  assert.equal(a1.done === false && a1.value.id, 1);
  assert.equal(b1.done === false && b1.value.id, 1);
  assert.equal(pins.length, 2);
  assert.notEqual(pins[0].log[1], pins[1].log[1], "distinct cursor names");
  await a.return();
  const rest: number[] = [];
  for await (const r of b) rest.push(r.id);
  assert.deepEqual(rest, [2, 3, 4, 5, 6]);
  assert.equal(pins[0].log.at(-1), "rollback");
  assert.equal(pins[1].log.at(-1), "commit");
  assert.deepEqual(pins.map((p) => p.released), [1, 1]);
});

test("stream protocol: concurrent next() on ONE stream is serialized (no row lost or duplicated)", async () => {
  const { driver } = fakeDriver(7);
  const db = await createDatabase({ driver });
  const s = db.select().from(items).stream({ batchSize: 3 });
  const results = await Promise.all(Array.from({ length: 9 }, () => s.next()));
  const ids = results.filter((r) => !r.done).map((r) => (r.value as { id: number }).id);
  assert.deepEqual(ids, [1, 2, 3, 4, 5, 6, 7]);
  assert.equal(results.filter((r) => r.done).length, 2);
});

test("stream protocol: a failing FETCH rolls back, releases and surfaces the error once", async () => {
  const { driver, pins } = fakeDriver(10, { failFetchAt: 2 });
  const db = await createDatabase({ driver });
  const s = db.select().from(items).stream({ batchSize: 3 });
  for (let i = 0; i < 3; i++) assert.equal((await s.next()).done, false);
  await assert.rejects(s.next(), /fake: fetch failed/);
  assert.equal(pins[0].log.at(-1), "rollback");
  assert.equal(pins[0].released, 1);
  assert.deepEqual(await s.next(), { done: true, value: undefined });
});

test("stream protocol: transaction modes render into the owned BEGIN", async () => {
  const { driver, pins } = fakeDriver(1);
  const db = await createDatabase({ driver });
  for await (const r of db.select().from(items).stream({ isolation: "repeatable-read", readOnly: true })) void r;
  assert.equal(pins[0].log[0], "begin isolation level repeatable read read only");
});

test("stream protocol: pre-aborted signal rejects without pinning; abort while idle rolls back", async () => {
  const { driver, pins } = fakeDriver(20);
  const db = await createDatabase({ driver });
  const pre = new AbortController();
  pre.abort();
  const s0 = db.select().from(items).stream({ signal: pre.signal });
  await assert.rejects(s0.next(), (e: unknown) => e instanceof QueryCanceledError && e.dispatched === false);
  assert.equal(pins.length, 0);

  const ctl = new AbortController();
  const s = db.select().from(items).stream({ batchSize: 5, signal: ctl.signal });
  assert.equal((await s.next()).done, false);
  ctl.abort();
  await assert.rejects(s.next(), (e: unknown) => e instanceof QueryCanceledError);
  assert.equal(pins[0].log.at(-1), "rollback");
  assert.equal(pins[0].released, 1);
});

test("stream protocol: inside db.transaction the stream never issues BEGIN/COMMIT and closes on early exit", async () => {
  const { driver, pins } = fakeDriver(10);
  const db = await createDatabase({ driver });
  await db.transaction(async (tx) => {
    let n = 0;
    for await (const r of tx.select().from(items).stream({ batchSize: 4 })) {
      void r;
      if (++n === 2) break;
    }
  });
  assert.equal(pins.length, 1, "the stream rides the transaction's pin");
  const log = pins[0].log.map((s) => s.replace(/"neutron_cursor_\d+"/, "C"));
  assert.equal(log[0], "begin");
  assert.match(log[1], /^declare C no scroll cursor for select /);
  assert.deepEqual(log.slice(2), ["fetch forward 4 from C", "close C", "commit"]);
});

test("stream protocol: a stream kept past its transaction runs nothing afterwards", async () => {
  const { driver, pins } = fakeDriver(10);
  const db = await createDatabase({ driver });
  const s = await db.transaction(async (tx) => {
    const stream = tx.select().from(items).stream({ batchSize: 2 });
    await stream.next();
    await stream.next();
    return stream;
  });
  const before = pins[0].log.length;
  await assert.rejects(s.next(), /settled/);
  await s.return();
  assert.equal(pins[0].log.length, before, "no statement after settle");
});

test("stream/batch protocol: adapters without pin fail clearly before any statement", async () => {
  const { driver, rootLog } = fakeDriver(3, { pinnable: false });
  const db = await createDatabase({ driver });
  assert.throws(() => db.select().from(items).stream(), /pinnable adapter/);
  await assert.rejects(db.batch([db.select().from(items)]).execute(), /pinnable adapter/);
  assert.deepEqual(rootLog, []);
});

test("batch protocol: exactly the planned statements, one pin, repeatable-read default", async () => {
  const { driver, pins } = fakeDriver(3);
  const db = await createDatabase({ driver });
  const plan = db.batch([
    db.select().from(items),
    db.update(items).set({ n: 5 }).where(eq(items.id, 1)),
    db.delete(items).where(eq(items.id, 2)).returning(),
  ]);
  const explained = plan.explain();
  assert.equal(explained.statementCount, 3);
  const [rows, count, deleted] = await plan;
  assert.equal(rows.length, 3);
  assert.equal(count, 1);
  assert.ok(Array.isArray(deleted));
  assert.equal(pins.length, 1);
  const log = pins[0].log;
  assert.equal(log[0], "begin isolation level repeatable read");
  assert.deepEqual(log.slice(1, 4), explained.statements.map((s) => s.sql));
  assert.equal(log[4], "commit");
  assert.equal(log.length, 5);
  assert.equal(pins[0].released, 1);
});

test("batch protocol: inside db.transaction the batch joins the scope (no BEGIN of its own)", async () => {
  const { driver, pins } = fakeDriver(3);
  const db = await createDatabase({ driver });
  await db.transaction(async (tx) => {
    const plan = tx.batch([tx.select().from(items), tx.select().from(items)]);
    assert.equal(plan.explain().transaction.ownership, "enclosing");
    await assert.rejects(tx.batch([tx.select().from(items)], { isolation: "serializable" }).execute(), /enclosing db.transaction/);
    await plan;
  });
  assert.equal(pins.length, 1);
  assert.deepEqual(
    pins[0].log.map((s) => (s.startsWith("select") ? "select" : s)),
    ["begin", "select", "select", "commit"],
  );
});
