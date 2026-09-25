import test, { after } from "node:test";
import assert from "node:assert/strict";
import { TEST_URL, ensureLive, uniqueDbName } from "./live-harness.js";
import { pgListener, postgresJsListener, notifyStatement, type PgNotification, type ListenEvent } from "./listen-notify.js";

// ---------------------------------------------------------------------------
// X05 live coverage for PostgreSQL LISTEN/NOTIFY through /listen-notify:
// real delivery semantics (commit-time delivery, rollback drops
// notifications, per-session ordering, disconnect = gap with NO automatic
// reconnect), the dedicated-connection contract (no pool checkout, no leaked
// backend sessions), cancellation (AbortSignal), and the postgres.js native
// listener. All against a throwaway x05_-prefixed database.
// ---------------------------------------------------------------------------

if (!(await ensureLive("live x05 listen-notify"))) {
  // Skips are visible per-suite; required-live mode already threw.
} else {
  const { Client } = (await import("pg")) as unknown as {
    Client: new (o: { connectionString: string }) => import("./listen-notify.js").PgNotifyClient & {
      connect: () => Promise<void>;
    };
  };
  const admin = new Client({ connectionString: TEST_URL });
  await admin.connect();
  const dbName = uniqueDbName("x05rev2_listen");
  await admin.query(`drop database if exists "${dbName}"`);
  await admin.query(`create database "${dbName}"`);
  const dbUrl = new URL(TEST_URL);
  dbUrl.pathname = `/${dbName}`;

  const sessionCount = async (): Promise<number> => {
    const r = (await (admin.query as unknown as (s: string, p: unknown[]) => Promise<{ rows: { n: number }[] }>)(
      "SELECT count(*)::int AS n FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
      [dbName],
    ));
    return r.rows[0].n;
  };
  const until = async (fn: () => boolean | Promise<boolean>, ms = 5000): Promise<boolean> => {
    for (let i = 0; i < ms / 50; i++) {
      if (await fn()) return true;
      await new Promise((r) => setTimeout(r, 50));
    }
    return await fn();
  };

  test("x05 listen-notify: round-trip, case preservation, ordering, escaping (owned pg)", async () => {
    const events: ListenEvent[] = [];
    const listener = await pgListener({ url: dbUrl.toString(), logger: (e) => events.push(e) });
    try {
      const got: PgNotification[] = [];
      await listener.listen(["X05_Case", "plain"], (n) => got.push(n));
      const other = new Client({ connectionString: dbUrl.toString() });
      await other.connect();
      try {
        await other.query(notifyStatement("X05_Case", "it's a 'test'"));
        await other.query(notifyStatement("plain"));
        await other.query(notifyStatement("plain", "second"));
      } finally {
        await other.end();
      }
      assert.ok(await until(() => got.length >= 3), `notifications arrived: ${JSON.stringify(got)}`);
      const cased = got.find((n) => n.channel === "X05_Case");
      assert.ok(cased, "case-preserved channel name round-trips");
      assert.equal(cased!.payload, "it's a 'test'");
      assert.deepEqual(
        got.filter((n) => n.channel === "plain").map((n) => n.payload),
        ["", "second"],
        "empty payload renders as '' and per-session order is commit order",
      );
      assert.equal(typeof got[0].processId, "number");
      assert.deepEqual(
        events.filter((e) => e.kind === "listen-open").map((e) => (e as { channel: string }).channel),
        ["X05_Case", "plain"],
      );
      // Lifecycle events never carry payloads.
      assert.ok(!JSON.stringify(events).includes("it's a 'test'"));
    } finally {
      await listener.close();
    }
    // close() ended the OWNED connection: no backend session leaks.
    assert.ok(await until(async () => (await sessionCount()) === 0), "owned listener connection must not leak a backend session after close()");
  });

  test("x05 listen-notify: notifications fire at COMMIT; a rolled-back NOTIFY is never delivered", async () => {
    const listener = await pgListener({ url: dbUrl.toString() });
    try {
      const got: string[] = [];
      await listener.listen("txch", (n) => got.push(n.payload));
      const other = new Client({ connectionString: dbUrl.toString() });
      await other.connect();
      try {
        await other.query("BEGIN");
        await other.query(notifyStatement("txch", "rolled-back"));
        await other.query("ROLLBACK");
        await other.query(notifyStatement("txch", "committed"));
      } finally {
        await other.end();
      }
      assert.ok(await until(() => got.length >= 1));
      await new Promise((r) => setTimeout(r, 150));
      assert.deepEqual(got, ["committed"], "PostgreSQL drops notifications of rolled-back transactions");
    } finally {
      await listener.close();
    }
  });

  test("x05 listen-notify: unlisten stops delivery; self-notify delivers; close is idempotent", async () => {
    const listener = await pgListener({ url: dbUrl.toString() });
    const got: PgNotification[] = [];
    await listener.listen(["keep", "drop"], (n) => got.push(n));
    await listener.unlisten("drop");
    await listener.notify("keep", "kept-1");
    await listener.notify("drop", "must-not-arrive");
    assert.ok(await until(() => got.length >= 1));
    await new Promise((r) => setTimeout(r, 100));
    // A listening session receives its own notifications in PostgreSQL.
    assert.deepEqual(got.map((n) => [n.channel, n.payload]), [["keep", "kept-1"]]);
    await listener.close();
    await listener.close();
    await assert.rejects(() => listener.notify("keep", "x"), /closed/);
  });

  test("x05 listen-notify: AbortSignal cancels the listener and releases its connection (I02)", async () => {
    const controller = new AbortController();
    const listener = await pgListener({ url: dbUrl.toString(), signal: controller.signal });
    await listener.listen("cancelch", () => {});
    assert.equal(await sessionCount(), 1, "the listener holds exactly one dedicated session");
    controller.abort();
    await new Promise((r) => setTimeout(r, 200));
    await assert.rejects(() => listener.notify("cancelch", "x"), /closed/);
    assert.ok(await until(async () => (await sessionCount()) === 0), "abort must release the dedicated connection — no leak, no pool poison");
  });

  test("x05 listen-notify: graceful close emits listener-closed, never listener-disconnected", async () => {
    const events: ListenEvent[] = [];
    let onDisconnect = 0;
    const listener = await pgListener({ url: dbUrl.toString(), logger: (e) => events.push(e) });
    await listener.listen("closech", () => {}, () => {
      onDisconnect++;
    });
    await listener.notify("closech", "last");
    await listener.close();
    assert.ok(events.some((e) => e.kind === "listen-open"), "sanity: listener opened");
    assert.ok(
      !events.some((e) => e.kind === "listener-disconnected"),
      "a deliberate close must not signal a disconnect gap",
    );
    assert.equal(onDisconnect, 0, "onDisconnect fires only when the dedicated connection dies");
    assert.equal(events.at(-1)?.kind, "listener-closed");
  });

  test("x05 listen-notify: server-side termination fires onDisconnect; NO automatic reconnect (gap by design)", async () => {
    const listener = await pgListener({ url: dbUrl.toString() });
    let disconnected = false;
    let pid = 0;
    await listener.listen("terminv", (n) => {
      pid = n.processId;
    });
    await listener.notify("terminv", "self");
    assert.ok(await until(() => pid > 0), "self-notification identifies the listener's backend pid");
    await listener.listen("terminv2", () => {}, () => {
      disconnected = true;
    });
    const other = new Client({ connectionString: dbUrl.toString() });
    await other.connect();
    try {
      await (other.query as unknown as (s: string, p: unknown[]) => Promise<unknown>)("SELECT pg_terminate_backend($1)", [pid]);
    } finally {
      await other.end();
    }
    assert.ok(await until(() => disconnected), "onDisconnect fires on server-side termination");
    // The documented contract: the owned listener does NOT reconnect. A
    // notify after the death fails (or is lost), never silently pretends.
    // The connection is dead, so closing must still be safe.
    await listener.close();
    assert.ok(await until(async () => (await sessionCount()) === 0), "terminated listener's connection is cleaned up");
  });

  test("x05 listen-notify: borrowed pg client stays open across listener close", async () => {
    const client = new Client({ connectionString: dbUrl.toString() });
    await client.connect();
    const listener = await pgListener({ client });
    const got: PgNotification[] = [];
    await listener.listen("borrowed", (n) => got.push(n));
    await listener.notify("borrowed", "one");
    assert.ok(await until(() => got.length >= 1));
    await listener.close();
    const r = (await client.query("SELECT 1 AS one")) as { rows: { one: number }[] };
    assert.equal(r.rows[0].one, 1, "the owner's client still works after the listener closed");
    await client.end();
  });

  test("x05 listen-notify: postgres.js native listener round-trip + unlisten", async () => {
    const postgres = (await import("postgres")).default as unknown as (
      u: string,
    ) => import("./listen-notify.js").PostgresJsListenClient & { end: (o?: { timeout?: number }) => Promise<void> };
    const sql = postgres(dbUrl.toString());
    try {
      const listener = await postgresJsListener({ client: sql });
      const got: [string, string][] = [];
      await listener.listen("jschan", (payload, channel) => got.push([channel, payload]));
      const other = new Client({ connectionString: dbUrl.toString() });
      await other.connect();
      try {
        await other.query(notifyStatement("jschan", "from-pg"));
      } finally {
        await other.end();
      }
      assert.ok(await until(() => got.length >= 1), "cross-driver delivery (pg NOTIFY -> postgres.js LISTEN)");
      assert.deepEqual(got, [["jschan", "from-pg"]]);
      await listener.unlisten("jschan");
      assert.deepEqual(listener.channels, []);
      await listener.close();
    } finally {
      await sql.end({ timeout: 5 });
    }
  });

  test("x05 listen-notify: oversized payload is rejected client-side before any connection", async () => {
    await assert.throws(() => notifyStatement("ch", "x".repeat(8001)), /8000-byte limit/);
  });

  after(async () => {
    await admin.query(`drop database if exists "${dbName}"`);
    await admin.end();
  });
}
