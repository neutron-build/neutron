import assert from "node:assert/strict";
import { describe, it, afterEach } from "node:test";

import {
  HttpTransport,
  MobileTransport,
  EmbeddedTransport,
  PgTransport,
  createTransport,
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

    const result = await transport.query("SELECT * FROM users");
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

  it("caches SELECT queries", async () => {
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

    const result1 = await transport.query("SELECT * FROM users");
    const result2 = await transport.query("SELECT * FROM users");
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
    const result1 = await transport.query("SELECT * FROM users");
    assert.equal(calls.length, 1);

    // Invalidate cache entries containing "users"
    transport.invalidateCache("users");

    // Second call should hit the server again
    const result2 = await transport.query("SELECT * FROM users");
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

    await transport.query("SELECT * FROM users");
    assert.equal(calls.length, 1);

    transport.invalidateCache();

    const result2 = await transport.query("SELECT * FROM users");
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
    // The promise is pending (queued), not resolved yet
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

    // Fill the queue
    transport.execute("INSERT INTO a VALUES (1)");
    transport.execute("INSERT INTO b VALUES (2)");
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

    // Should throw a connection error wrapping the abort
    await assert.rejects(
      () => transport.query("SELECT 1"),
      (err: unknown) => {
        assert(err instanceof NucleusConnectionError);
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
