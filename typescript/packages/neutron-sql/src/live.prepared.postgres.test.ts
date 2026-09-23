import assert from "node:assert/strict";
import test from "node:test";
import {
  createDatabase,
  preparedStatement,
  pgStatementName,
  wrapPgPool,
  integer,
  pgTable,
  schemaToDDL,
  serial,
  text,
  type Driver,
  type NeutronDatabase,
  type PreparedStatement,
} from "./index.js";
import { TEST_URL, ensureLive, uniqueDbName } from "./live-harness.js";

// Live V13 suite (Q04): connection-scoped prepared execution against REAL
// PostgreSQL —
// - wrapper correctness on both bundled drivers (params, repeats, counts,
//   zero-parameter statements),
// - deterministic name derivation (pg) and the server-side existence proof
//   through pg_prepared_statements on a PINNED connection,
// - scope correctness: two connections, same name, different SQL must not
//   clash (per-session namespaces — pinned with raw clients),
// - the one-connection hazard (same name, different text) that makes
//   SQL-derived names mandatory — pg's own guard pinned live,
// - statement release on connection close (server deallocates at session
//   end; pg_stat_activity proof),
// - rollback/abort survival (verified live on PG 17): named statements
//   outlive ROLLBACK and ABORT — the session is their only lifetime.
//   postgres.js's FetchPreparedStatement retry exists for environments
//   where statements DO vanish (e.g. transaction poolers), not for PG
//   itself.
// Live databases are q04_-prefixed throwaways; the shared admin DB's
// _neutron_migrations is never touched.

const DB_NAME = uniqueDbName("q04_prepared");

const notes = pgTable("q04_notes", {
  id: serial("id").primaryKey(),
  body: text("body").notNull(),
  hits: integer("hits").notNull().default(0),
});

interface SuiteFixture {
  db: NeutronDatabase;
  url: string;
}

async function withSuite(driverKind: "postgres" | "pg", fn: (fx: SuiteFixture) => Promise<void>): Promise<void> {
  if (!(await ensureLive(`live prepared (${driverKind})`))) {
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
  const db = await createDatabase({ url: url.toString(), driverOptions: { driver: driverKind }, tables: { notes } });
  try {
    for (const stmt of schemaToDDL([notes])) {
      await db.driver.execute(stmt);
    }
    await fn({ db, url: url.toString() });
  } finally {
    await db.close();
    const cleanup = new Pool({ connectionString: TEST_URL, max: 1 });
    await cleanup.query(`drop database if exists "${DB_NAME}"`);
    await cleanup.end();
  }
}

test(`live prepared: wrapper prepared statements execute with params, repeats, counts and zero parameters`, async (t) => {
  for (const driverKind of ["postgres", "pg"] as const) {
    await t.test(driverKind, async () => {
      await withSuite(driverKind, async ({ db }) => {
        await db.insert(notes).values([
          { body: "a" },
          { body: "b" },
          { body: "c" },
        ]);
        const stmt = db.driver.prepare!("select id, body, hits from q04_notes where body = $1 order by id");
        const r1 = await stmt.query<{ id: number; body: string }>(["b"]);
        assert.deepEqual(r1.map((r) => r.body), ["b"]);
        const r2 = await stmt.query<{ id: number; body: string }>(["a"]);
        assert.deepEqual(r2.map((r) => r.body), ["a"], "second execution reuses the parsed statement");
        if (driverKind === "pg") {
          assert.ok(stmt.name?.startsWith("nsqp_"), "pg names statements deterministically");
          assert.ok((stmt.name?.length ?? 0) <= 63, "within PostgreSQL's 63-byte name limit");
          assert.equal(stmt.name, pgStatementName(stmt.sql), "the name is the documented sha256 derivation");
          assert.notEqual(stmt.name, pgStatementName(stmt.sql + " "));
        } else {
          assert.equal(stmt.name, undefined, "postgres.js owns per-connection naming internally");
        }
        const zero = db.driver.prepare!("select count(*)::int as n from q04_notes");
        const zr = await zero.query<{ n: number }>();
        assert.equal(zr[0].n, 3, "zero-parameter statements take the extended protocol path");
        const upd = db.driver.prepare!("update q04_notes set hits = hits + $1 where body = $2");
        assert.equal(await upd.execute([5, "b"]), 1, "execute resolves the affected row count");
        assert.equal(await upd.execute([2, "b"]), 1);
        const after = await stmt.query<{ id: number; body: string; hits: number }>(["b"]);
        assert.equal(after[0].hits, 7);
      });
    });
  }
});

test(`live prepared: server-side existence and connection scope on a pinned connection (pg)`, async (t) => {
  for (const driverKind of ["postgres", "pg"] as const) {
    await t.test(driverKind, async () => {
      await withSuite(driverKind, async ({ db }) => {
        // begin() pins one connection on both drivers — whatever prepares
        // inside must exist on THAT session's pg_prepared_statements.
        await db.driver.begin(async (tx) => {
          const stmt = tx.prepare!("select $1::int as v");
          const rows = await stmt.query<{ v: number }>([41]);
          assert.equal(rows[0].v, 41);
          const mine = await tx.query<{ n: number }>("select count(*)::int as n from pg_prepared_statements where name = $1", [stmt.name ?? "%"]);
          if (driverKind === "pg") {
            assert.equal(mine[0].n, 1, "the deterministic nsqp_ name exists server-side on the pinned session");
          } else {
            assert.ok(stmt.name === undefined && mine[0].n >= 0, "postgres.js naming is driver-internal (documented)");
          }
        });
      });
    });
  }
});

test(`live prepared: two connections, same name, different SQL do not clash (server sessions are independent namespaces)`, async (t) => {
  await t.test("raw pg clients, explicit shared name", async () => {
    if (!(await ensureLive("live prepared (raw pg)"))) return;
    const { Pool } = (await import("pg")) as unknown as {
      Pool: new (o: object) => { query: (s: string, p?: unknown[]) => Promise<unknown>; end: () => Promise<void> };
    };
    const admin = new Pool({ connectionString: TEST_URL, max: 1 });
    await admin.query(`drop database if exists "${DB_NAME}"`);
    await admin.query(`create database "${DB_NAME}"`);
    await admin.end();
    const url = new URL(TEST_URL);
    url.pathname = `/${DB_NAME}`;
    const { Client } = (await import("pg")) as unknown as {
      Client: new (o: object) => { query: (c: object) => Promise<{ rows: Array<Record<string, unknown>> }>; end: () => Promise<void> };
    };
    const a = new Client({ connectionString: url.toString() });
    const b = new Client({ connectionString: url.toString() });
    await (a as unknown as { connect(): Promise<void> }).connect();
    await (b as unknown as { connect(): Promise<void> }).connect();
    try {
      // One shared name, two DIFFERENT statements, two sessions: each session
      // resolves the name to its own parse — the scope fact V13 pins.
      const ra = await a.query({ name: "q04_clash", text: "select 41 as v" });
      const rb = await b.query({ name: "q04_clash", text: "select 42 as v" });
      assert.equal(ra.rows[0].v, 41);
      assert.equal(rb.rows[0].v, 42);
      const repeat = await b.query({ name: "q04_clash", text: "select 42 as v" });
      assert.equal(repeat.rows[0].v, 42, "repeated binds skip Parse on the same session");
    } finally {
      await a.end();
      await b.end();
      const cleanup = new Pool({ connectionString: TEST_URL, max: 1 });
      await cleanup.query(`drop database if exists "${DB_NAME}"`);
      await cleanup.end();
    }
  });

  await t.test("wrapped pools, same derived name (same SQL) on separate connections", async () => {
    if (!(await ensureLive("live prepared (wrapped pools)"))) return;
    const { Pool } = (await import("pg")) as unknown as {
      Pool: new (o: object) => { query: (s: string, p?: unknown[]) => Promise<unknown>; end: () => Promise<void> };
    };
    const admin = new Pool({ connectionString: TEST_URL, max: 1 });
    await admin.query(`drop database if exists "${DB_NAME}"`);
    await admin.query(`create database "${DB_NAME}"`);
    await admin.end();
    const url = new URL(TEST_URL);
    url.pathname = `/${DB_NAME}`;
    const poolA = new Pool({ connectionString: url.toString(), max: 1 });
    const poolB = new Pool({ connectionString: url.toString(), max: 1 });
    const driverA: Driver = wrapPgPool(poolA as never, { ownership: "owned" });
    const driverB: Driver = wrapPgPool(poolB as never, { ownership: "owned" });
    const sqlText = "select 7::int as v";
    const sa = driverA.prepare!(sqlText);
    const sb = driverB.prepare!(sqlText);
    assert.equal(sa.name, sb.name, "same SQL derives the same name on every connection");
    assert.equal((await sa.query())[0].v, 7);
    assert.equal((await sb.query())[0].v, 7, "each connection parsed the same name on its own session — no clash");
    await driverA.close();
    await driverB.close();
    const cleanup = new Pool({ connectionString: TEST_URL, max: 1 });
    await cleanup.query(`drop database if exists "${DB_NAME}"`);
    await cleanup.end();
  });
});

test(`live prepared: the same name with different SQL on ONE connection is rejected (why hash names are mandatory)`, async (t) => {
  await t.test("raw pg client guard", async () => {
    if (!(await ensureLive("live prepared (raw guard)"))) return;
    const { Pool, Client } = (await import("pg")) as unknown as {
      Pool: new (o: object) => { query: (s: string, p?: unknown[]) => Promise<unknown>; end: () => Promise<void> };
      Client: new (o: object) => { query: (c: object) => Promise<{ rows: Array<Record<string, unknown>> }>; end: () => Promise<void> };
    };
    const admin = new Pool({ connectionString: TEST_URL, max: 1 });
    await admin.query(`drop database if exists "${DB_NAME}"`);
    await admin.query(`create database "${DB_NAME}"`);
    await admin.end();
    const url = new URL(TEST_URL);
    url.pathname = `/${DB_NAME}`;
    const c = new Client({ connectionString: url.toString() });
    await (c as unknown as { connect(): Promise<void> }).connect();
    try {
      await c.query({ name: "q04_reuse", text: "select 1 as v" });
      await assert.rejects(
        () => c.query({ name: "q04_reuse", text: "select 2 as v" }),
        /Prepared statements must be unique/,
        "pg's client-side guard — our sha256-derived names make this unreachable through the wrapper",
      );
    } finally {
      await c.end();
      const cleanup = new Pool({ connectionString: TEST_URL, max: 1 });
      await cleanup.query(`drop database if exists "${DB_NAME}"`);
      await cleanup.end();
    }
  });
});

test(`live prepared: closing a connection releases its server-side statements (session end deallocates)`, async (t) => {
  await t.test("raw pg client lifecycle", async () => {
    if (!(await ensureLive("live prepared (close release)"))) return;
    const { Pool, Client } = (await import("pg")) as unknown as {
      Pool: new (o: object) => { query: (s: string, p?: unknown[]) => Promise<{ rows: Array<Record<string, unknown>> }>; end: () => Promise<void> };
      Client: new (o: object) => { query: (c: object | string) => Promise<{ rows: Array<Record<string, unknown>> }>; end: () => Promise<void> };
    };
    const admin = new Pool({ connectionString: TEST_URL, max: 1 });
    await admin.query(`drop database if exists "${DB_NAME}"`);
    await admin.query(`create database "${DB_NAME}"`);
    await admin.end();
    const url = new URL(TEST_URL);
    url.pathname = `/${DB_NAME}`;
    const c = new Client({ connectionString: url.toString() });
    await (c as unknown as { connect(): Promise<void> }).connect();
    const pidRows = await c.query("select pg_backend_pid() as pid");
    const pid = pidRows.rows[0].pid as number;
    await c.query({ name: "q04_close_probe", text: "select 1 as v" });
    const mine = await c.query("select count(*)::int as n from pg_prepared_statements where name = 'q04_close_probe'");
    assert.equal(mine.rows[0].n, 1, "statement exists on the session");
    await c.end();
    // The session is gone -> PostgreSQL deallocated its statements (no
    // DEALLOCATE was ever sent; session end is the release mechanism).
    const checker = new Pool({ connectionString: url.toString(), max: 1 });
    const gone = await checker.query("select count(*)::int as n from pg_stat_activity where pid = $1", [pid]);
    assert.equal(gone.rows[0].n, 0, "the backend session terminated");
    await checker.end();
    const cleanup = new Pool({ connectionString: TEST_URL, max: 1 });
    await cleanup.query(`drop database if exists "${DB_NAME}"`);
    await cleanup.end();
  });
});

test(`live prepared: statements survive transaction rollback and abort on PG 17 (session lifetime, verified live)`, async (t) => {
  await t.test("pg: named statements outlive rollback and abort on the same session", async () => {
    if (!(await ensureLive("live prepared (pg rollback)"))) return;
    const { Pool, Client } = (await import("pg")) as unknown as {
      Pool: new (o: object) => { query: (s: string, p?: unknown[]) => Promise<unknown>; end: () => Promise<void> };
      Client: new (o: object) => { query: (c: object | string) => Promise<{ rows: Array<Record<string, unknown>> }>; end: () => Promise<void> };
    };
    const admin = new Pool({ connectionString: TEST_URL, max: 1 });
    await admin.query(`drop database if exists "${DB_NAME}"`);
    await admin.query(`create database "${DB_NAME}"`);
    await admin.end();
    const url = new URL(TEST_URL);
    url.pathname = `/${DB_NAME}`;
    const c = new Client({ connectionString: url.toString() });
    await (c as unknown as { connect(): Promise<void> }).connect();
    try {
      await c.query("begin");
      await c.query({ name: "q04_rb", text: "select 9::int as v" });
      const inTx = await c.query("select count(*)::int as n from pg_prepared_statements where name = 'q04_rb'");
      assert.equal(inTx.rows[0].n, 1);
      await c.query("rollback");
      // PostgreSQL 17: the statement survives (session lifetime). postgres.js's
      // FetchPreparedStatement retry path exists for environments where
      // statements DO vanish (transaction poolers); PG itself keeps them.
      const survived = await c.query({ name: "q04_rb", text: "select 9::int as v" });
      assert.equal(survived.rows[0].v, 9, "re-bound without re-Parse after rollback");
      // ... and it survives an ABORTED transaction too.
      await c.query("begin");
      try {
        await c.query("select 1/0");
      } catch {
        // expected
      }
      await c.query("rollback");
      const afterAbort = await c.query({ name: "q04_rb", text: "select 9::int as v" });
      assert.equal(afterAbort.rows[0].v, 9, "also re-bound after an aborted transaction");
    } finally {
      await c.end();
      const cleanup = new Pool({ connectionString: TEST_URL, max: 1 });
      await cleanup.query(`drop database if exists "${DB_NAME}"`);
      await cleanup.end();
    }
  });

  await t.test("postgres.js: signature-cached statements re-execute after rollback", async () => {
    if (!(await ensureLive("live prepared (postgres rollback)"))) return;
    const { Pool } = (await import("pg")) as unknown as {
      Pool: new (o: object) => { query: (s: string, p?: unknown[]) => Promise<unknown>; end: () => Promise<void> };
    };
    const admin = new Pool({ connectionString: TEST_URL, max: 1 });
    await admin.query(`drop database if exists "${DB_NAME}"`);
    await admin.query(`create database "${DB_NAME}"`);
    await admin.end();
    const url = new URL(TEST_URL);
    url.pathname = `/${DB_NAME}`;
    const mod = (await import("postgres")) as unknown as {
      default: (url: string, opts?: object) => import("./index.js").PostgresJsClient & {
        reserve?: () => Promise<{
          unsafe: (s: string, p?: unknown[], o?: object) => Promise<Array<Record<string, unknown>>>;
          release: () => void;
        }>;
      };
    };
    const client = mod.default(url.toString(), { max: 1 });
    const reserved = await client.reserve!();
    try {
      const text = "select 9::int as v";
      await reserved.unsafe("begin");
      await reserved.unsafe(text, [], { prepare: true, simple: false });
      await reserved.unsafe("rollback");
      const again = await reserved.unsafe(text, [], { prepare: true, simple: false });
      assert.equal(again[0].v, 9, "the per-connection signature cache re-bound the statement after rollback");
    } finally {
      reserved.release();
      await client.end({ timeout: 5 });
      const cleanup = new Pool({ connectionString: TEST_URL, max: 1 });
      await cleanup.query(`drop database if exists "${DB_NAME}"`);
      await cleanup.end();
    }
  });
});

test(`live prepared: tx-scoped prepare after the transaction ends follows the plain tx.query posture (documented lease)`, async (t) => {
  // Review-1 MINOR-3: the tx-scoped prepare closes over the pooled client,
  // which begin() releases in its finally — the pin OUTLIVES the transaction.
  // That is the same posture plain tx.query already has (I01): use after the
  // transaction ends still executes, on the released pooled client, and is
  // UNSUPPORTED. Pinned so the documented behavior cannot drift silently.
  await t.test("pg: escaped statement and tx both execute after end (unsupported lease)", async () => {
    await withSuite("pg", async ({ db }) => {
      let escaped: PreparedStatement | undefined;
      let escapedTx: Driver | undefined;
      await db.driver.begin(async (tx) => {
        escaped = tx.prepare!("select 13::int as v");
        escapedTx = tx;
        assert.equal((await escaped.query<{ v: number }>())[0].v, 13, "in-tx execution on the pinned connection");
      });
      assert.ok(escaped && escapedTx);
      const rows = await escaped!.query<{ v: number }>();
      assert.equal(rows[0].v, 13, "use-after-end still executes on the released pooled client (documented, unsupported)");
      const plain = await escapedTx!.query<{ v: number }>("select 1 as v");
      assert.equal(plain[0].v, 1, "plain tx.query after end behaves identically — the I01 posture");
    });
  });
});

test(`live prepared: adapters without prepare fail closed through the helper`, async () => {
  const bare: Driver = {
    query: async () => [],
    execute: async () => 0,
    begin: async () => {
      throw new Error("no tx");
    },
    close: async () => {},
    lifecycle: { ownership: "borrowed", terminated: false, terminate: async () => {} },
  };
  assert.throws(() => preparedStatement(bare, "select 1"), /does not advertise support/);
  const db = await createDatabase({ driver: bare, tables: { notes } });
  assert.equal(typeof db.driver.prepare, "undefined");
  await db.close();
});

test(`live prepared: borrowed wrapped pools keep working after a prepared-statement user closes its wrapper`, async () => {
  // V14-adjacent ownership pin: prepared execution must not change the
  // borrowed/owned contract.
  if (!(await ensureLive("live prepared (ownership)"))) return;
  const { Pool } = (await import("pg")) as unknown as {
    Pool: new (o: object) => { query: (s: string, p?: unknown[]) => Promise<{ rows: Array<Record<string, unknown>> }>; end: () => Promise<void> };
  };
  const pool = new Pool({ connectionString: TEST_URL, max: 1 });
  const wrapper = wrapPgPool(pool as never); // borrowed
  const stmt = wrapper.prepare!("select 1::int as v");
  assert.equal((await stmt.query())[0].v, 1);
  await wrapper.close(); // borrowed: never touches the pool
  const stillAlive = await pool.query("select 2::int as v");
  assert.equal(stillAlive.rows[0].v, 2, "the injected pool survives wrapper close");
  await pool.end();
});
