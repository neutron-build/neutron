import assert from "node:assert/strict";
import { describe, it, afterEach } from "node:test";

import {
  HttpTransport,
  MobileTransport,
  EmbeddedTransport,
  createTransport,
  NucleusConnectionError,
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
