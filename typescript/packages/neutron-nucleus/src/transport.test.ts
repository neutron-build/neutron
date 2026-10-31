import assert from "node:assert/strict";
import { describe, it, afterEach } from "node:test";

import {
  HttpTransport,
  MobileTransport,
  EmbeddedTransport,
  PgTransport,
  createTransport,
  withKV,
  NucleusConnectionError,
  NucleusError,
  NucleusQueryError,
} from "./index.js";

// =========================================================================
// Global state helpers — save/restore around mocks
// =========================================================================

const origFetch = globalThis.fetch;
const origNavigator = Object.getOwnPropertyDescriptor(globalThis, "navigator");
const origWindow = Object.getOwnPropertyDescriptor(globalThis, "window");

function restoreGlobals(): void {
  globalThis.fetch = origFetch;
  if (origNavigator) {
    Object.defineProperty(globalThis, "navigator", origNavigator);
  } else {
    delete (globalThis as Record<string, unknown>).navigator;
  }
  if (origWindow) {
    Object.defineProperty(globalThis, "window", origWindow);
  } else {
    delete (globalThis as Record<string, unknown>).window;
  }
}

// =========================================================================
// createTransport() auto-detection
// =========================================================================

describe("createTransport", () => {
  afterEach(() => {
    restoreGlobals();
  });

  it("returns HttpTransport by default", () => {
    // Ensure no Tauri or RN globals are set
    delete (globalThis as Record<string, unknown>).window;
    delete (globalThis as Record<string, unknown>).navigator;
    const transport = createTransport({ url: "http://localhost:5432" });
    assert(transport instanceof HttpTransport);
  });

  it("returns MobileTransport when React Native detected", () => {
    // Remove window so Tauri check doesn't fire
    delete (globalThis as Record<string, unknown>).window;
    Object.defineProperty(globalThis, "navigator", {
      value: { product: "ReactNative", onLine: true },
      configurable: true,
    });
    const transport = createTransport({ url: "http://localhost:5432" });
    assert(transport instanceof MobileTransport);
  });

  it("returns EmbeddedTransport when Tauri detected", () => {
    (globalThis as Record<string, unknown>).window = {
      __TAURI_INTERNALS__: {
        invoke: async () => ({}),
      },
      addEventListener: () => {},
    };
    const transport = createTransport({ url: "" });
    assert(transport instanceof EmbeddedTransport);
  });

  it("returns PgTransport for a postgres:// URL in Node (the canonical path)", () => {
    delete (globalThis as Record<string, unknown>).window;
    delete (globalThis as Record<string, unknown>).navigator;
    const transport = createTransport({ url: "postgres://nucleus@localhost:5432/nucleus" });
    assert(transport instanceof PgTransport);
  });

  it("returns PgTransport for postgresql:// too", () => {
    delete (globalThis as Record<string, unknown>).window;
    delete (globalThis as Record<string, unknown>).navigator;
    const transport = createTransport({ url: "postgresql://localhost:5432/db" });
    assert(transport instanceof PgTransport);
  });

  it("still returns HttpTransport for an http:// gateway URL", () => {
    delete (globalThis as Record<string, unknown>).window;
    delete (globalThis as Record<string, unknown>).navigator;
    const transport = createTransport({ url: "http://gateway.example.com" });
    assert(transport instanceof HttpTransport);
  });

  it("prefers EmbeddedTransport over MobileTransport when both present", () => {
    Object.defineProperty(globalThis, "navigator", {
      value: { product: "ReactNative", onLine: true },
      configurable: true,
    });
    (globalThis as Record<string, unknown>).window = {
      __TAURI_INTERNALS__: {
        invoke: async () => ({}),
      },
      addEventListener: () => {},
    };
    const transport = createTransport({ url: "http://localhost:5432" });
    assert(transport instanceof EmbeddedTransport);
  });
});

// =========================================================================
// Helper — create a fake fetch that records calls
// =========================================================================

interface FetchCall {
  url: string;
  init?: RequestInit;
}

function makeFakeFetch(
  responses: Array<{ ok: boolean; status: number; body: unknown } | "network-error">,
): { fetch: typeof globalThis.fetch; calls: FetchCall[] } {
  const calls: FetchCall[] = [];
  let idx = 0;
  const fakeFetch = async (input: string | URL | Request, init?: RequestInit): Promise<Response> => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
    calls.push({ url, init });
    const entry = responses[Math.min(idx, responses.length - 1)];
    idx++;
    if (entry === "network-error") {
      throw new TypeError("fetch failed");
    }
    return {
      ok: entry.ok,
      status: entry.status,
      json: async () => entry.body,
      text: async () => (typeof entry.body === "string" ? entry.body : JSON.stringify(entry.body)),
    } as unknown as Response;
  };
  return { fetch: fakeFetch as typeof globalThis.fetch, calls };
}

// =========================================================================
// MobileTransport — retry logic
// =========================================================================

describe("MobileTransport", () => {
  afterEach(() => {
    restoreGlobals();
  });

  it("retries on network failure then succeeds", async () => {
    const { fetch: fakeFetch, calls } = makeFakeFetch([
      "network-error",
      "network-error",
      { ok: true, status: 200, body: { ok: true, data: [{ id: 1 }], rowCount: 1 } },
    ]);
    globalThis.fetch = fakeFetch;

    // Minimal retryDelay so the test is fast
    const transport = new MobileTransport({
      url: "http://localhost:5432",
      maxRetries: 3,
      retryDelay: 1,
      cacheEnabled: false,
      offlineQueueEnabled: false,
    });

    const result = await transport.query("SELECT * FROM users", [], { readOnly: true, cache: true });
    assert.equal(result.rows.length, 1);
    assert.equal(calls.length, 3);
  });

  it("does not retry on 4xx errors", async () => {
    const { fetch: fakeFetch, calls } = makeFakeFetch([
      { ok: false, status: 400, body: "Bad request" },
    ]);
    globalThis.fetch = fakeFetch;

    const transport = new MobileTransport({
      url: "http://localhost:5432",
      maxRetries: 3,
      retryDelay: 1,
      cacheEnabled: false,
      offlineQueueEnabled: false,
    });

    await assert.rejects(() => transport.query("SELECT bad"), NucleusQueryError);
    // Only 1 fetch call — no retry for 4xx
    assert.equal(calls.length, 1);
  });

  it("caches explicitly opted-in pure reads", async () => {
    const { fetch: fakeFetch, calls } = makeFakeFetch([
      { ok: true, status: 200, body: { ok: true, data: [{ id: 1 }], rowCount: 1 } },
    ]);
    globalThis.fetch = fakeFetch;

    const transport = new MobileTransport({
      url: "http://localhost:5432",
      maxRetries: 0,
      retryDelay: 1,
      cacheEnabled: true,
      cacheTTL: 60_000,
      offlineQueueEnabled: false,
    });

    const result1 = await transport.query("SELECT * FROM users", [], { readOnly: true, cache: true });
    const result2 = await transport.query("SELECT * FROM users", [], { readOnly: true, cache: true });
    // Fetch called only once — second call served from cache
    assert.equal(calls.length, 1);
    assert.deepEqual(result1, result2);
  });

  it("does not cache non-SELECT queries", async () => {
    const { fetch: fakeFetch, calls } = makeFakeFetch([
      { ok: true, status: 200, body: { ok: true, affected: 1 } },
      { ok: true, status: 200, body: { ok: true, affected: 1 } },
    ]);
    globalThis.fetch = fakeFetch;

    const transport = new MobileTransport({
      url: "http://localhost:5432",
      maxRetries: 0,
      retryDelay: 1,
      cacheEnabled: true,
      offlineQueueEnabled: false,
    });

    await transport.execute("INSERT INTO users (name) VALUES ('a')");
    await transport.execute("INSERT INTO users (name) VALUES ('b')");
    assert.equal(calls.length, 2);
  });

  it("invalidates cache by pattern", async () => {
    const { fetch: fakeFetch, calls } = makeFakeFetch([
      { ok: true, status: 200, body: { ok: true, data: [{ id: 1 }], rowCount: 1 } },
      { ok: true, status: 200, body: { ok: true, data: [{ id: 2 }], rowCount: 1 } },
    ]);
    globalThis.fetch = fakeFetch;

    const transport = new MobileTransport({
      url: "http://localhost:5432",
      maxRetries: 0,
      retryDelay: 1,
      cacheEnabled: true,
      cacheTTL: 60_000,
      offlineQueueEnabled: false,
    });

    // First call populates cache
    const result1 = await transport.query("SELECT * FROM users", [], { readOnly: true, cache: true });
    assert.equal(calls.length, 1);

    // Invalidate cache entries containing "users"
    transport.invalidateCache("users");

    // Second call should hit the server again
    const result2 = await transport.query("SELECT * FROM users", [], { readOnly: true, cache: true });
    assert.equal(calls.length, 2);
    assert.equal((result2.rows[0] as Record<string, unknown>).id, 2);
  });

  it("invalidateCache() with no arg clears all entries", async () => {
    const { fetch: fakeFetch, calls } = makeFakeFetch([
      { ok: true, status: 200, body: { ok: true, data: [{ id: 1 }], rowCount: 1 } },
      { ok: true, status: 200, body: { ok: true, data: [{ id: 10 }], rowCount: 1 } },
    ]);
    globalThis.fetch = fakeFetch;

    const transport = new MobileTransport({
      url: "http://localhost:5432",
      maxRetries: 0,
      retryDelay: 1,
      cacheEnabled: true,
      cacheTTL: 60_000,
      offlineQueueEnabled: false,
    });

    await transport.query("SELECT * FROM users", [], { readOnly: true, cache: true });
    assert.equal(calls.length, 1);

    transport.invalidateCache();

    const result2 = await transport.query("SELECT * FROM users", [], { readOnly: true, cache: true });
    assert.equal(calls.length, 2);
    assert.equal((result2.rows[0] as Record<string, unknown>).id, 10);
  });
});

// =========================================================================
// MobileTransport — offline queue
// =========================================================================

describe("MobileTransport offline queue", () => {
  afterEach(() => {
    restoreGlobals();
  });

  it("queues writes when offline", async () => {
    const { fetch: fakeFetch } = makeFakeFetch([]);
    globalThis.fetch = fakeFetch;

    // Set navigator.onLine = false so the constructor sees offline
    Object.defineProperty(globalThis, "navigator", {
      value: { onLine: false },
      configurable: true,
    });

    const transport = new MobileTransport({
      url: "http://localhost:5432",
      maxRetries: 0,
      retryDelay: 1,
      cacheEnabled: false,
      offlineQueueEnabled: true,
      maxQueueSize: 10,
    });

    // execute() should not throw — it queues the write
    const promise = transport.execute("INSERT INTO users (name) VALUES ('offline')");
    // The promise is pending (queued), not resolved yet; it settles on close
    // ("transport closed") — handle it so the eventual rejection never
    // surfaces as an unhandledRejection after the test ends.
    promise.catch(() => {});
    assert.equal(transport.queueSize, 1);

    // We cannot await the promise or it will hang — it resolves only on flush
    // Just verify the queue grew. Clean up by closing transport.
    await transport.close();
  });

  it("flushes queue on reconnect", async () => {
    // We need to simulate the window 'online' event listener.
    // MobileTransport registers a listener in its constructor.
    const listeners: Record<string, Array<() => void>> = {};
    (globalThis as Record<string, unknown>).window = {
      addEventListener: (event: string, handler: () => void) => {
        if (!listeners[event]) listeners[event] = [];
        listeners[event].push(handler);
      },
    };

    const { fetch: fakeFetch, calls } = makeFakeFetch([
      { ok: true, status: 200, body: { ok: true, affected: 1 } },
      { ok: true, status: 200, body: { ok: true, affected: 1 } },
    ]);
    globalThis.fetch = fakeFetch;

    Object.defineProperty(globalThis, "navigator", {
      value: { onLine: false },
      configurable: true,
    });

    const transport = new MobileTransport({
      url: "http://localhost:5432",
      maxRetries: 0,
      retryDelay: 1,
      cacheEnabled: false,
      offlineQueueEnabled: true,
      maxQueueSize: 10,
    });

    // Queue two writes while offline
    const p1 = transport.execute("INSERT INTO a VALUES (1)");
    const p2 = transport.execute("INSERT INTO b VALUES (2)");
    assert.equal(transport.queueSize, 2);

    // Simulate coming back online — fire the 'online' handler
    assert.ok(listeners.online, "online listener should be registered");
    for (const handler of listeners.online) handler();

    // The queued writes should now resolve
    const [r1, r2] = await Promise.all([p1, p2]);
    assert.equal(r1, 1);
    assert.equal(r2, 1);
    assert.equal(transport.queueSize, 0);
    assert.equal(calls.length, 2);
  });

  it("rejects when offline queue is full", async () => {
    Object.defineProperty(globalThis, "navigator", {
      value: { onLine: false },
      configurable: true,
    });

    const transport = new MobileTransport({
      url: "http://localhost:5432",
      maxRetries: 0,
      retryDelay: 1,
      cacheEnabled: false,
      offlineQueueEnabled: true,
      maxQueueSize: 2,
    });

    // Fill the queue. The queued promises only settle on close(); attach
    // catch handlers so their eventual "transport closed" rejection does
    // not surface as an unhandled rejection after the test ends.
    transport.execute("INSERT INTO a VALUES (1)").catch(() => {});
    transport.execute("INSERT INTO b VALUES (2)").catch(() => {});
    assert.equal(transport.queueSize, 2);

    // Third write should reject
    await assert.rejects(
      () => transport.execute("INSERT INTO c VALUES (3)"),
      NucleusConnectionError,
    );

    await transport.close();
  });
});

// =========================================================================
// HttpTransport — timeout
// =========================================================================

describe("HttpTransport", () => {
  afterEach(() => {
    restoreGlobals();
  });

  it("aborts request after timeout", async () => {
    // Create a fetch that never resolves (simulates a slow server)
    globalThis.fetch = ((_input: string | URL | Request, init?: RequestInit): Promise<Response> => {
      return new Promise((_resolve, reject) => {
        // Listen for abort signal
        const signal = init?.signal;
        if (signal) {
          signal.addEventListener("abort", () => {
            reject(new DOMException("The operation was aborted.", "AbortError"));
          });
        }
        // Never resolve — let the timeout fire
      });
    }) as typeof globalThis.fetch;

    const transport = new HttpTransport("http://localhost:5432", {}, 50);

    // The configured deadline has a distinct timeout outcome
    await assert.rejects(
      () => transport.query("SELECT 1"),
      (err: unknown) => {
        assert.equal((err as Error).name, "TimeoutError");
        return true;
      },
    );
  });

  it("completes normally when response arrives before timeout", async () => {
    const { fetch: fakeFetch } = makeFakeFetch([
      { ok: true, status: 200, body: { ok: true, data: [{ val: 42 }], rowCount: 1 } },
    ]);
    globalThis.fetch = fakeFetch;

    const transport = new HttpTransport("http://localhost:5432", {}, 5000);
    const result = await transport.query("SELECT 42 AS val");
    assert.equal(result.rows.length, 1);
    assert.equal((result.rows[0] as Record<string, unknown>).val, 42);
  });
});

// =========================================================================
// EmbeddedTransport — basic smoke test
// =========================================================================

describe("EmbeddedTransport", () => {
  afterEach(() => {
    restoreGlobals();
  });

  it("routes query through Tauri invoke", async () => {
    const invokeCalls: Array<{ cmd: string; args: Record<string, unknown> }> = [];
    (globalThis as Record<string, unknown>).window = {
      __TAURI_INTERNALS__: {
        invoke: async (cmd: string, args: Record<string, unknown>) => {
          invokeCalls.push({ cmd, args });
          return { rows: [{ id: 1 }], rowCount: 1 };
        },
      },
      addEventListener: () => {},
    };

    const transport = new EmbeddedTransport();
    const result = await transport.query("SELECT * FROM users");
    assert.equal(result.rows.length, 1);
    assert.equal(invokeCalls.length, 1);
    assert.equal(invokeCalls[0].cmd, "nucleus_query");
    assert.equal(invokeCalls[0].args.sql, "SELECT * FROM users");
  });

  it("routes execute through Tauri invoke", async () => {
    (globalThis as Record<string, unknown>).window = {
      __TAURI_INTERNALS__: {
        invoke: async () => ({ affected: 3 }),
      },
      addEventListener: () => {},
    };

    const transport = new EmbeddedTransport();
    const affected = await transport.execute("DELETE FROM users WHERE active = false");
    assert.equal(affected, 3);
  });
});

// =========================================================================
// PgTransactionTransport — pooled-client release on failed COMMIT/ROLLBACK
// =========================================================================
//
// The pool is max: 8. Before the try/finally, a failed COMMIT or ROLLBACK
// never released its client — 8 failed commits permanently deadlocked the
// app. beginTransaction DID release on error; the asymmetry was the miss.
// These are the first behavioral tests for the Pg transaction transport
// (previously only an instanceof check existed).

import { PgTransactionTransport } from "./index.js";

interface FakeClientLog {
  queries: string[];
  releases: Array<Error | undefined>;
}

function makeFakeClient(failOn?: string): { client: any; log: FakeClientLog } {
  const log: FakeClientLog = { queries: [], releases: [] };
  const client = {
    async query(sql: string) {
      log.queries.push(sql);
      if (sql === failOn) throw new Error(`${sql} failed (connection died)`);
      return { rows: [], rowCount: 0 };
    },
    release(err?: Error) {
      log.releases.push(err);
    },
  };
  return { client, log };
}

describe("PgTransactionTransport pooled-client release", () => {
  it("releases the client (with the error) when COMMIT fails", async () => {
    const { client, log } = makeFakeClient("COMMIT");
    const tx = new PgTransactionTransport(client);

    await assert.rejects(tx.commit(), /COMMIT failed/);
    assert.equal(log.releases.length, 1, "client must be released exactly once");
    assert.ok(log.releases[0] instanceof Error, "failed COMMIT must release(err) so the pool destroys the connection");

    // The transport is finished; further use fails loudly, not by leaking.
    await assert.rejects(tx.query("SELECT 1"), /already finished/);
  });

  it("releases the client (with the error) when ROLLBACK fails", async () => {
    const { client, log } = makeFakeClient("ROLLBACK");
    const tx = new PgTransactionTransport(client);

    await assert.rejects(tx.rollback(), /ROLLBACK failed/);
    assert.equal(log.releases.length, 1);
    assert.ok(log.releases[0] instanceof Error);
  });

  it("releases the client cleanly on successful COMMIT", async () => {
    const { client, log } = makeFakeClient();
    const tx = new PgTransactionTransport(client);

    await tx.commit();
    assert.equal(log.releases.length, 1);
    assert.equal(log.releases[0], undefined);
  });

  it("eight failed commits do not exhaust the pool (every client released)", async () => {
    // The deadlock shape: pool max is 8. Each failed transaction must hand
    // its client back, so the ninth connect() still succeeds.
    const released: Array<Error | undefined> = [];
    let live = 0;
    let peak = 0;
    const pool = {
      async connect() {
        live += 1;
        peak = Math.max(peak, live);
        return {
          async query(sql: string) {
            if (sql === "COMMIT") throw new Error("commit failed");
            return { rows: [], rowCount: 0 };
          },
          release(err?: Error) {
            released.push(err);
            live -= 1;
          },
        };
      },
    };
    for (let i = 0; i < 9; i++) {
      const client = await pool.connect();
      const tx = new PgTransactionTransport(client);
      await assert.rejects(tx.commit(), /commit failed/);
    }
    assert.equal(released.length, 9, "all nine failed commits released their client");
    assert.equal(live, 0, "no client stays checked out");
    assert.ok(peak <= 1, "clients are not held across transactions");
  });
});

// =========================================================================
// PgTransport — lazy pool creation
// =========================================================================
//
// getPool() used to be check-then-await-then-assign: two concurrent first
// queries both saw `pool === null`, both awaited loadPg(), and both
// constructed a Pool — the loser was orphaned and its connections never
// .end()ed. The memoization must be synchronous (assign the promise before
// the first await).

describe("PgTransport lazy pool creation", () => {
  it("constructs exactly one pool for concurrent first queries", async () => {
    // Patch the shared pg module exports (CJS exports object is mutable; the
    // transport resolves `import('pg')` lazily, so nothing has cached a Pool
    // constructor before this point in the process).
    const mod = (await import("pg")) as unknown as { default?: Record<string, unknown> };
    const pgExports = (mod.default ?? (mod as unknown as Record<string, unknown>)) as {
      Pool?: unknown;
    };
    const RealPool = pgExports.Pool;
    let constructed = 0;
    let ended = 0;
    class CountingPool {
      constructor(_cfg: unknown) {
        constructed++;
        return {
          on: () => {},
          query: async () => ({ rows: [{ result: 1 }], rowCount: 1 }),
          end: async () => {
            ended++;
          },
          connect: async () => {
            throw new Error("connect not used by this test");
          },
        };
      }
    }
    pgExports.Pool = CountingPool;
    try {
      const transport = new PgTransport("postgres://nucleus@localhost:5432/nucleus");
      const [a, b] = await Promise.all([
        transport.query("SELECT 1 AS a"),
        transport.query("SELECT 2 AS b"),
      ]);
      assert.equal(a.rows.length, 1);
      assert.equal(b.rows.length, 1);
      assert.equal(constructed, 1, "concurrent first queries must share one pool (no orphan)");

      await transport.close();
      assert.equal(ended, 1, "close() ends the pool");
    } finally {
      pgExports.Pool = RealPool;
    }
  });
});

// =========================================================================
// PgTransport — headers/timeout honored or rejected at construction
// =========================================================================
//
// createTransport() silently dropped headers/timeout for postgres:// URLs
// (they were only honored on the HTTP paths). Ratified contract: timeout is
// honored by mapping it onto the pool's native statement/query/connection
// timeouts; headers are REJECTED at construction — the PostgreSQL wire
// protocol has no HTTP headers to carry them on.

async function patchPgPool(): Promise<{
  configs: Array<Record<string, unknown>>;
  restore: () => void;
}> {
  const mod = (await import("pg")) as unknown as { default?: Record<string, unknown> };
  const pgExports = (mod.default ?? (mod as unknown as Record<string, unknown>)) as {
    Pool?: unknown;
  };
  const RealPool = pgExports.Pool;
  const configs: Array<Record<string, unknown>> = [];
  class CapturingPool {
    constructor(cfg: Record<string, unknown>) {
      configs.push(cfg);
      return {
        on: () => {},
        query: async () => ({ rows: [{ result: 1 }], rowCount: 1 }),
        end: async () => {},
        connect: async () => {
          throw new Error("connect not used by this test");
        },
      };
    }
  }
  pgExports.Pool = CapturingPool;
  return {
    configs,
    restore: () => {
      pgExports.Pool = RealPool;
    },
  };
}

describe("PgTransport headers/timeout contract", () => {
  afterEach(() => {
    restoreGlobals();
  });

  it("throws at construction when headers are passed (wire protocol has none)", () => {
    assert.throws(
      () => new PgTransport("postgres://nucleus@localhost:5432/nucleus", { headers: { "X-Api-Key": "k" } }),
      (err: unknown) => {
        assert(err instanceof NucleusError);
        assert.equal(err.code, "PG_HEADERS_UNSUPPORTED");
        assert.match(err.message, /PostgreSQL wire protocol has no HTTP headers/);
        return true;
      },
    );
  });

  it("createTransport rejects headers for postgres:// URLs instead of silently dropping them", () => {
    delete (globalThis as Record<string, unknown>).window;
    delete (globalThis as Record<string, unknown>).navigator;
    assert.throws(
      () =>
        createTransport({
          url: "postgres://nucleus@localhost:5432/nucleus",
          headers: { "X-Api-Key": "k" },
        }),
      /PostgreSQL wire protocol has no HTTP headers/,
    );
  });

  it("maps timeout onto the pool's native statement/query/connection timeouts", async () => {
    const { configs, restore } = await patchPgPool();
    try {
      const transport = new PgTransport("postgres://nucleus@localhost:5432/nucleus", { timeout: 5000 });
      await transport.ping();
      assert.equal(configs.length, 1);
      assert.equal(configs[0].connectionString, "postgres://nucleus@localhost:5432/nucleus");
      assert.equal(configs[0].max, 8);
      assert.equal(configs[0].statement_timeout, 5000);
      assert.equal(configs[0].query_timeout, 5000);
      assert.equal(configs[0].connectionTimeoutMillis, 5000);
      await transport.close();
    } finally {
      restore();
    }
  });

  it("createTransport threads timeout into the PgTransport pool", async () => {
    delete (globalThis as Record<string, unknown>).window;
    delete (globalThis as Record<string, unknown>).navigator;
    const { configs, restore } = await patchPgPool();
    try {
      const transport = createTransport({ url: "postgres://nucleus@localhost:5432/nucleus", timeout: 1234 });
      assert(transport instanceof PgTransport);
      await transport.ping();
      assert.equal(configs.length, 1);
      assert.equal(configs[0].statement_timeout, 1234);
      assert.equal(configs[0].connectionTimeoutMillis, 1234);
      await transport.close();
    } finally {
      restore();
    }
  });

  it("plain construction leaves the pool config untouched", async () => {
    const { configs, restore } = await patchPgPool();
    try {
      const transport = new PgTransport("postgres://nucleus@localhost:5432/nucleus");
      await transport.ping();
      assert.equal(configs.length, 1);
      assert.equal(configs[0].connectionString, "postgres://nucleus@localhost:5432/nucleus");
      assert.equal("statement_timeout" in configs[0], false);
      assert.equal("query_timeout" in configs[0], false);
      assert.equal("connectionTimeoutMillis" in configs[0], false);
      await transport.close();
    } finally {
      restore();
    }
  });
});

// =========================================================================
// HttpTransport — signal handling (X04 MAJOR-1)
// =========================================================================
//
// request() used to arm its timer off `controller` instead of `timeout`, so
// the DEFAULT construction (timeout undefined) plus any live signal ran
// setTimeout(cb, undefined) — firing ~immediately, aborting the INTERNAL
// controller, and surfacing every request as NucleusConnectionError against
// a healthy server. These tests run against a real local HTTP server: no
// transport in this file may carry a signal without proving it works.

import http from "node:http";
import { NucleusNotSupportedError } from "./index.js";

describe("HttpTransport signal handling (X04 MAJOR-1)", () => {
  let server: http.Server | undefined;

  async function startServer(handler: (res: http.ServerResponse) => Promise<void>): Promise<number> {
    server = http.createServer((_req, res) => {
      void handler(res);
    });
    await new Promise<void>((r) => server!.listen(0, "127.0.0.1", r));
    return (server.address() as { port: number }).port;
  }

  afterEach(async () => {
    restoreGlobals();
    if (server) {
      await new Promise<void>((r) => server!.close(() => r()));
      server = undefined;
    }
  });

  it("deadline aborts a real streaming response after headers arrive (TSD-08)", async () => {
    const port = await startServer(async (res) => {
      res.setHeader('Content-Type', 'application/json');
      res.flushHeaders();
      res.write('{"ok":true,"data":');
      await new Promise((resolve) => setTimeout(resolve, 100));
      res.end('[]}');
    });
    await assert.rejects(new HttpTransport(`http://127.0.0.1:${port}`, {}, 20).query('SELECT 1'), { name: 'TimeoutError' });
  });

  it("default construction (no timeout) with a live signal completes normally", async () => {
    const delay = (ms: number) => new Promise((r) => setTimeout(r, ms));
    const port = await startServer(async (res) => {
      await delay(150);
      res.end(JSON.stringify({ ok: true, data: [{ val: 42 }], rowCount: 1 }));
    });
    const transport = new HttpTransport(`http://127.0.0.1:${port}`); // no timeout — the default construction
    const ac = new AbortController(); // live, never aborted
    const result = await transport.query<{ val: number }>("SELECT 42 AS val", [], { signal: ac.signal });
    assert.equal(result.rows[0].val, 42);
    await transport.close();
  });

  it("signal with an explicit timeout completes when the server answers in time", async () => {
    const port = await startServer(async (res) => {
      res.end(JSON.stringify({ ok: true, data: [{ val: 42 }], rowCount: 1 }));
    });
    const transport = new HttpTransport(`http://127.0.0.1:${port}`, {}, 5000);
    const ac = new AbortController();
    const result = await transport.query<{ val: number }>("SELECT 42 AS val", [], { signal: ac.signal });
    assert.equal(result.rows[0].val, 42);
    await transport.close();
  });

  it("aborting the signal mid-flight rejects with AbortError", async () => {
    const delay = (ms: number) => new Promise((r) => setTimeout(r, ms));
    const port = await startServer(async (res) => {
      await delay(400);
      res.end(JSON.stringify({ ok: true, data: [{ val: 42 }], rowCount: 1 }));
    });
    const transport = new HttpTransport(`http://127.0.0.1:${port}`); // default construction
    const ac = new AbortController();
    setTimeout(() => ac.abort(), 50);
    await assert.rejects(
      () => transport.query("SELECT pg_sleep(1)", [], { signal: ac.signal }),
      (err: unknown) => err instanceof DOMException && err.name === "AbortError",
    );
    await transport.close();
  });

  it("timeout=0 arms no timer — a slow-ish response still completes", async () => {
    const delay = (ms: number) => new Promise((r) => setTimeout(r, ms));
    const port = await startServer(async (res) => {
      await delay(120);
      res.end(JSON.stringify({ ok: true, data: [{ val: 42 }], rowCount: 1 }));
    });
    const transport = new HttpTransport(`http://127.0.0.1:${port}`, {}, 0);
    const result = await transport.query<{ val: number }>("SELECT 42 AS val");
    assert.equal(result.rows[0].val, 42);
    await transport.close();
  });
});

// =========================================================================
// PgTransport — queryCancelable pid-probe window honesty (X04 MAJOR-2)
// =========================================================================
//
// The abort listener used to install a no-op cancelAttempt whenever the
// pg_backend_pid probe had not answered yet (or had failed), which dodged
// the !cancelAttempt guard: the statement resolved normally and the
// caller's abort was silently swallowed. A failed probe meant a PERMANENT
// silent no-cancel mode. Both paths must reject with the honest
// could-not-be-dispatched / probe-failed error instead.

interface CancelProbeCfg {
  pidDelayMs?: number;
  pidFails?: boolean;
  statementMs?: number;
  hangStatement?: boolean;
}

interface CancelProbeRecords {
  poolQueries: Array<{ sql: string; params?: unknown[] }>;
  releases: Array<Error | undefined>;
}

async function patchCancelPool(cfg: CancelProbeCfg): Promise<{ records: CancelProbeRecords; restore: () => void }> {
  const mod = (await import("pg")) as unknown as { default?: Record<string, unknown> };
  const pgExports = (mod.default ?? (mod as unknown as Record<string, unknown>)) as { Pool?: unknown };
  const RealPool = pgExports.Pool;
  const records: CancelProbeRecords = { poolQueries: [], releases: [] };
  const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
  const PID = 4242;
  let cancelStatement: ((err: Error) => void) | null = null;
  class CancelProbePool {
    constructor() {
      return {
        on: () => {},
        end: async () => {},
        query: async (sql: string, params?: unknown[]) => {
          records.poolQueries.push({ sql, params });
          if (sql.includes("pg_cancel_backend")) {
            // Mirrors the real engine: the canceled backend's statement
            // rejects with SQLSTATE 57014.
            cancelStatement?.(new Error("canceling statement due to user request"));
            return { rows: [{ canceled: true }], rowCount: 1 };
          }
          // No-signal queries go straight through pool.query.
          if (cfg.statementMs) await sleep(cfg.statementMs);
          return { rows: [{ v: 42 }], rowCount: 1 };
        },
        connect: async () => ({
          query: async (sql: string) => {
            if (sql.includes("pg_cancel_backend")) {
              records.poolQueries.push({ sql, params: [PID] });
              cancelStatement?.(new Error("canceling statement due to user request"));
              return { rows: [{ canceled: true }], rowCount: 1 };
            }
            if (sql.includes("pg_backend_pid")) {
              if (cfg.pidDelayMs) await sleep(cfg.pidDelayMs);
              if (cfg.pidFails) throw new Error("engine lacks pg_backend_pid");
              return { rows: [{ pid: PID }], rowCount: 1 };
            }
            if (cfg.hangStatement) {
              // In flight until pg_cancel_backend arrives.
              return new Promise((_resolve, reject) => {
                cancelStatement = reject;
              });
            }
            if (cfg.statementMs) await sleep(cfg.statementMs);
            return { rows: [{ v: 42 }], rowCount: 1 };
          },
          release: (err?: Error) => {
            records.releases.push(err);
          },
        }),
      };
    }
  }
  pgExports.Pool = CancelProbePool;
  return { records, restore: () => { pgExports.Pool = RealPool; } };
}

describe("PgTransport queryCancelable pid-window honesty (X04 MAJOR-2)", () => {
  it("abort during the pid-probe window rejects — never resolves silently", async () => {
    const { restore } = await patchCancelPool({ pidDelayMs: 150 });
    try {
      const transport = new PgTransport("postgres://nucleus@localhost:5432/nucleus");
      const ac = new AbortController();
      setTimeout(() => ac.abort(), 40); // inside the 150 ms probe window
      await assert.rejects(
        () => transport.fetchval("SELECT 42 AS v", [], { signal: ac.signal }),
        (err: unknown) => {
          assert.equal((err as Error).name, "AbortError");
          return true;
        },
      );
      await transport.close();
    } finally {
      restore();
    }
  });

  it("failed pid probe + abort mid-statement rejects with the probe failure as cause", async () => {
    const { records, restore } = await patchCancelPool({ pidFails: true, statementMs: 120 });
    try {
      const transport = new PgTransport("postgres://nucleus@localhost:5432/nucleus");
      const ac = new AbortController();
      setTimeout(() => ac.abort(), 40); // after the probe failed, statement in flight
      await assert.rejects(
        () => transport.fetchval("SELECT 42 AS v", [], { signal: ac.signal }),
        (err: unknown) => {
          assert(err instanceof NucleusNotSupportedError);
          assert.match(err.message, /could not be confirmed/);
          assert.match(String((err as NucleusNotSupportedError).cause), /engine lacks pg_backend_pid/);
          return true;
        },
      );
      assert.equal(records.poolQueries.length, 0, "no pg_cancel_backend can be dispatched without a pid");
      await transport.close();
    } finally {
      restore();
    }
  });

  it("abort mid-statement with pid known dispatches pg_cancel_backend and surfaces the cancellation", async () => {
    const { records, restore } = await patchCancelPool({ hangStatement: true });
    try {
      const transport = new PgTransport("postgres://nucleus@localhost:5432/nucleus");
      const ac = new AbortController();
      setTimeout(() => ac.abort(), 40);
      await assert.rejects(
        () => transport.fetchval("SELECT 1 FROM slow", [], { signal: ac.signal }),
        /canceling statement due to user request/,
      );
      assert.equal(records.poolQueries.length, 1);
      assert.match(records.poolQueries[0].sql, /pg_cancel_backend/);
      assert.equal(records.poolQueries[0].params?.[0], 4242);
      assert.ok(records.releases.at(-1) instanceof Error, "canceled statement must release(err) so the pool destroys the client");
      await transport.close();
    } finally {
      restore();
    }
  });

  it("failed pid probe without an abort still runs the query (documented fallback, not a block)", async () => {
    const { records, restore } = await patchCancelPool({ pidFails: true, statementMs: 5 });
    try {
      const transport = new PgTransport("postgres://nucleus@localhost:5432/nucleus");
      const v = await transport.fetchval<number>("SELECT 42 AS v");
      assert.equal(v, 42);
      assert.ok(
        records.poolQueries.every((q) => !q.sql.includes("pg_cancel_backend")),
        "no cancel may be dispatched when nothing aborted",
      );
      assert.ok(records.releases.every((err) => err === undefined), "clean releases — nothing was canceled");
      await transport.close();
    } finally {
      restore();
    }
  });
});

describe('Mobile operation semantics (TSD-05/06)', () => {
  afterEach(restoreGlobals);
  it('BEGIN with missing or invalid remote identity is unknown after one dispatch', async () => {
    for (const txId of [undefined, null, '', '   ', 12, {}, 'bad\nheader']) {
      for (const kind of ['http', 'mobile']) {
        let sends = 0;
        let created = 0;
        globalThis.fetch = (async () => {
          sends++; created++;
          return new Response(JSON.stringify({ ok: true, data: { txId } }));
        }) as typeof fetch;
        const transport = kind === 'http' ? new HttpTransport('http://local') : new MobileTransport({ url: 'http://local', retryDelay: 1 });
        await assert.rejects(() => transport.beginTransaction(), (error: any) => {
          assert.equal(error.code, 'UNKNOWN_OUTCOME');
          assert.equal(error.cause.code, 'TRANSACTION_ERROR');
          assert.equal(error.meta.operation, 'BEGIN');
          return true;
        });
        assert.equal(sends, 1);
        assert.equal(created, 1);
        await transport.close();
      }
    }
  });
  it('explicit BEGIN rejection remains definitive and is never replayed', async () => {
    let sends = 0;
    globalThis.fetch = (async () => { sends++; return new Response(JSON.stringify({ ok: false, error: 'BEGIN rejected' })); }) as typeof fetch;
    const transport = new MobileTransport({ url: 'http://local', retryDelay: 1 });
    await assert.rejects(() => transport.beginTransaction(), { code: 'TRANSACTION_ERROR' });
    assert.equal(sends, 1);
    await transport.close();
  });
  it('dispatches every scalar model mutation and reports lost responses without replay', async () => {
    let value = 0;
    globalThis.fetch = (async (_url, init) => {
      const { sql } = JSON.parse(String(init?.body)) as { sql: string };
      value++;
      if (sql.includes('LOST')) throw new TypeError('response lost after commit');
      return new Response(JSON.stringify({ ok: true, data: [{ value }], affected: 1 }));
    }) as typeof fetch;
    const mobile = new MobileTransport({ url: 'http://local', retryDelay: 1, cacheEnabled: true });
    for (const sql of ['SELECT KV_INCR($1)', 'SELECT KV_SETNX($1)', 'SELECT KV_DEL($1)',
      'SELECT DOC_INSERT($1)', 'SELECT STREAM_READGROUP($1)', 'SELECT PUBSUB_PUBLISH($1)',
      'SELECT BLOB_STORE($1)', 'SELECT DATALOG_ASSERT($1)']) {
      const first = await mobile.fetchval<number>(sql, ['k']);
      assert.equal(await mobile.fetchval(sql, ['k']), first! + 1);
    }
    for (const call of [() => mobile.execute('LOST'), () => mobile.fetchval('SELECT LOST()'),
      () => mobile.beginTransaction()]) {
      // BEGIN is deliberately given the same lost-response model.
      const saved = globalThis.fetch;
      globalThis.fetch = (async () => { value++; throw new TypeError('response lost after commit'); }) as typeof fetch;
      const before = value;
      await assert.rejects(call, { code: 'UNKNOWN_OUTCOME' });
      assert.equal(value, before + 1);
      globalThis.fetch = saved;
    }
  });
  it('actual KV plugin increments twice and SETNX reflects the second attempt', async () => {
    let counter = 0;
    let locked = false;
    globalThis.fetch = (async (_url, init) => {
      const { sql } = JSON.parse(String(init?.body)) as { sql: string };
      let value: unknown;
      if (sql.includes('KV_INCR')) value = ++counter;
      else if (sql.includes('KV_SETNX')) { value = !locked; locked = true; }
      else throw new Error('unexpected SQL');
      return new Response(JSON.stringify({ data: [{ value }] }));
    }) as typeof fetch;
    const mobile = new MobileTransport({ url: 'http://local', cacheEnabled: true });
    const features = { version: 'test', isNucleus: true, hasKV: true, hasVector: false, hasTimeSeries: false,
      hasDocument: false, hasGraph: false, hasFTS: false, hasGeo: false, hasBlob: false,
      hasStreams: false, hasColumnar: false, hasDatalog: false, hasCDC: false, hasPubSub: false };
    const { kv } = withKV.init(mobile, features);
    assert.equal(await kv.incr('counter'), 1);
    assert.equal(await kv.incr('counter'), 2);
    assert.equal(await kv.setNX('lock', 'owner'), true);
    assert.equal(await kv.setNX('lock', 'owner'), false);
  });
  it('suppresses shared caching for the lifetime of a remote transaction', async () => {
    let reads = 0;
    globalThis.fetch = (async (url) => {
      if (String(url).endsWith('/begin')) return new Response(JSON.stringify({ data: { txId: 'test' } }));
      if (String(url).endsWith('/commit')) return new Response(JSON.stringify({ ok: true }));
      return new Response(JSON.stringify({ data: [{ value: ++reads }] }));
    }) as typeof fetch;
    const mobile = new MobileTransport({ url: 'http://local', cacheEnabled: true });
    const opts = { readOnly: true, cache: true };
    await mobile.query('read', [], opts);
    const tx = await mobile.beginTransaction();
    await mobile.query('read', [], opts);
    await mobile.query('read', [], opts);
    assert.equal(reads, 3);
    await tx.commit();
    await mobile.query('read', [], opts);
    await mobile.query('read', [], opts);
    assert.equal(reads, 4);
  });
  it('checks abort before a cache hit and fences reads overlapping a write', async () => {
    let resolveRead!: (r: Response) => void;
    let reads = 0;
    globalThis.fetch = (async (_url, init) => {
      if (String(init?.body).includes('read')) {
        reads++;
        if (reads === 1) return new Promise<Response>((resolve) => { resolveRead = resolve; });
        return new Response(JSON.stringify({ data: [{ value: 2 }] }));
      }
      return new Response(JSON.stringify({ affected: 1 }));
    }) as typeof fetch;
    const mobile = new MobileTransport({ url: 'http://local', cacheEnabled: true });
    const opts = { readOnly: true, cache: true };
    const stale = mobile.query('read', [], opts);
    await mobile.execute('write');
    resolveRead(new Response(JSON.stringify({ data: [{ value: 1 }] })));
    await stale;
    assert.equal((await mobile.query<{ value: number }>('read', [], opts)).rows[0].value, 2);
    const ac = new AbortController(); ac.abort();
    await assert.rejects(mobile.query('read', [], { ...opts, signal: ac.signal }), { name: 'AbortError' });
    assert.equal(reads, 2);
  });
  it('does not retry an HTTP 500 for raw SQL mutations', async () => {
    let sends = 0;
    globalThis.fetch = (async () => { sends++; return new Response('ambiguous server failure', { status: 500 }); }) as typeof fetch;
    const mobile = new MobileTransport({ url: 'http://local', retryDelay: 1 });
    await assert.rejects(mobile.query('SELECT KV_INCR($1)'), { code: 'UNKNOWN_OUTCOME' });
    assert.equal(sends, 1);
  });
});

describe('HTTP complete-response deadline (TSD-08)', () => {
  afterEach(restoreGlobals);
  for (const status of [200, 500]) {
    it(`bounds a stalled ${status} body after headers`, async () => {
      let internal!: AbortSignal;
      globalThis.fetch = (async (_url, init) => {
        internal = init!.signal!;
        return { ok: status === 200, status, json: () => new Promise(() => {}), text: () => new Promise(() => {}) } as unknown as Response;
      }) as typeof fetch;
      await assert.rejects(new HttpTransport('http://local', {}, 5).query('SELECT 1'), { name: 'TimeoutError' });
      assert.equal(internal.aborted, true);
    });
  }
  it('forwards caller abort after headers with timeout explicitly disabled', async () => {
    const ac = new AbortController();
    let internal!: AbortSignal;
    let entered!: () => void;
    const bodyEntered = new Promise<void>((resolve) => { entered = resolve; });
    globalThis.fetch = (async (_url, init) => {
      internal = init!.signal!;
      return { ok: true, json: () => { entered(); return new Promise(() => {}); } } as unknown as Response;
    }) as typeof fetch;
    const result = new HttpTransport('http://local', {}, 0).query('SELECT 1', [], { signal: ac.signal });
    await bodyEntered;
    ac.abort();
    await assert.rejects(result, { name: 'AbortError' });
    assert.equal(internal.aborted, true);
  });
  it('defaults to 30 seconds and accepts explicit zero, refusing invalid timeouts', async () => {
    const defaults = new HttpTransport('http://local') as unknown as { timeout: number };
    const disabled = new HttpTransport('http://local', {}, 0) as unknown as { timeout: number };
    assert.equal(defaults.timeout, 30_000);
    assert.equal(disabled.timeout, 0);
    for (const value of [-1, Infinity, NaN]) assert.throws(() => new HttpTransport('http://local', {}, value), RangeError);
  });
});

describe('Pg cancellation rejection lifecycle (TSD-07)', () => {
  it('observes cancel failure immediately, drains on target rejection, and reserves an independent pool', async () => {
    const mod = await import('pg');
    const pg = (mod.default ?? mod) as unknown as { Pool: unknown };
    const original = pg.Pool;
    let finishTarget!: (v: unknown) => void;
    let finishCancel!: () => void;
    let submitted!: () => void;
    let cancelEntered!: () => void;
    const targetEntered = new Promise<void>((resolve) => { submitted = resolve; });
    const cancellationEntered = new Promise<void>((resolve) => { cancelEntered = resolve; });
    const releases: string[] = [];
    const unhandled: unknown[] = [];
    const onUnhandled = (error: unknown): void => { unhandled.push(error); };
    process.on('unhandledRejection', onUnhandled);
    let created = 0;
    class Pool {
      id = ++created;
      on(): void {}
      async end(): Promise<void> {}
      async query(): Promise<never> { throw new Error('application pool must never dispatch cancellation'); }
      async connect() {
        const id = this.id;
        return {
          query: async (sql: string) => {
            if (sql.includes('pg_backend_pid')) return { rows: [{ pid: 42 }], rowCount: 1 };
            if (sql.includes('pg_cancel_backend')) {
              assert.equal(id, 2);
              cancelEntered();
              await new Promise<void>((resolve) => { finishCancel = resolve; });
              throw new Error('side channel lost');
            }
            submitted();
            await new Promise((resolve) => { finishTarget = resolve; });
            throw new Error('target failed');
          },
          release: () => { releases.push(String(id)); },
        };
      }
    }
    pg.Pool = Pool;
    const t = new PgTransport('postgres://local');
    try {
      const ac = new AbortController();
      const operation = t.query('business', [], { signal: ac.signal });
      const rejected = assert.rejects(operation, (err: unknown) => {
        assert(err instanceof NucleusError);
        assert.equal(err.code, 'CANCELLATION_FAILED');
        assert.equal(err.meta?.cancellation, 'failed');
        return true;
      });
      await targetEntered; ac.abort(); await cancellationEntered;
      finishTarget(null);
      await new Promise((resolve) => setImmediate(resolve));
      assert.deepEqual(releases, []);
      finishCancel();
      await rejected;
      await new Promise((resolve) => setImmediate(resolve));
      assert.deepEqual(unhandled, []);
      assert.deepEqual(releases, ['2', '1']);
      assert.equal(created, 2);
    } finally {
      process.removeListener('unhandledRejection', onUnhandled);
      await t.close();
      pg.Pool = original;
    }
  });
  it('does not produce unhandled rejection when cancellation fails before a still-running target', async () => {
    const mod = await import('pg');
    const pg = (mod.default ?? mod) as unknown as { Pool: unknown };
    const original = pg.Pool;
    const unhandled: unknown[] = [];
    const observe = (err: unknown): void => { unhandled.push(err); };
    let finish!: () => void;
    let ready!: () => void;
    const entered = new Promise<void>((resolve) => { ready = resolve; });
    process.on('unhandledRejection', observe);
    class Pool {
      on(): void {}
      async end(): Promise<void> {}
      async connect() {
        return { query: async (sql: string) => {
          if (sql.includes('pg_backend_pid')) return { rows: [{ pid: 42 }], rowCount: 1 };
          if (sql.includes('pg_cancel_backend')) throw Object.assign(new Error('unsupported'), { code: '0A000' });
          ready(); await new Promise<void>((resolve) => { finish = resolve; });
          return { rows: [], rowCount: 0 };
        }, release: () => {} };
      }
    }
    pg.Pool = Pool;
    const t = new PgTransport('postgres://local');
    try {
      const ac = new AbortController();
      const operation = t.query('business', [], { signal: ac.signal });
      const rejected = assert.rejects(operation, (err: unknown) => {
        assert(err instanceof NucleusNotSupportedError);
        assert.equal(err.meta?.cancellation, 'unsupported'); return true;
      });
      await entered; ac.abort();
      await new Promise((resolve) => setTimeout(resolve, 10));
      assert.deepEqual(unhandled, []);
      finish(); await rejected;
    } finally { process.removeListener('unhandledRejection', observe); await t.close(); pg.Pool = original; }
  });
});
