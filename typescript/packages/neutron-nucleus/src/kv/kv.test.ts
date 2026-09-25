// ---------------------------------------------------------------------------
// @neutron-build/nucleus/kv — unit tests (X04)
//
// Wire-level tests over a mock transport: key scoping, the namespace-bound
// resource surface, and cancellation threading. Engine-level truth (TTL
// truncation, expiry boundaries, restart) lives in models.live.test.ts.
// ---------------------------------------------------------------------------

import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { withKV } from "./index.js";
import type { KVModel } from "./index.js";
import type { Transport, TransactionTransport, QueryResult, IsolationLevel } from "../types.js";

interface Call {
  method: "query" | "execute" | "fetchval";
  sql: string;
  params?: unknown[];
  opts?: { signal?: AbortSignal };
}

class RecordingTransport implements Transport {
  readonly calls: Call[] = [];
  private nextResult: unknown = null;

  setResult(r: unknown): void {
    this.nextResult = r;
  }

  // The Transport contract: an already-aborted signal rejects before
  // anything is sent (real transports do this natively; the mock must too).
  private checkAborted(opts?: { signal?: AbortSignal }): void {
    if (opts?.signal?.aborted) {
      throw new DOMException('This operation was aborted', 'AbortError');
    }
  }

  async query<T = Record<string, unknown>>(sql: string, params: unknown[] = [], opts?: { signal?: AbortSignal }): Promise<QueryResult<T>> {
    this.checkAborted(opts);
    this.calls.push({ method: "query", sql, params, opts });
    return { rows: (this.nextResult ? [this.nextResult] : []) as T[], rowCount: this.nextResult ? 1 : 0 };
  }

  async execute(sql: string, params: unknown[] = [], opts?: { signal?: AbortSignal }): Promise<number> {
    this.checkAborted(opts);
    this.calls.push({ method: "execute", sql, params, opts });
    return 1;
  }

  async fetchval<T = unknown>(sql: string, params: unknown[] = [], opts?: { signal?: AbortSignal }): Promise<T | null> {
    this.checkAborted(opts);
    this.calls.push({ method: "fetchval", sql, params, opts });
    return this.nextResult as T | null;
  }

  async beginTransaction(_isolation?: IsolationLevel): Promise<TransactionTransport> {
    throw new Error("not needed");
  }
  async close(): Promise<void> {}
  async ping(): Promise<void> {}
}

const FEATURES = {
  isNucleus: true,
  hasKV: true,
  hasVector: false,
  hasTimeSeries: false,
  hasDocument: false,
  hasGraph: false,
  hasFTS: false,
  hasGeo: false,
  hasBlob: false,
  hasStreams: false,
  hasColumnar: false,
  hasDatalog: false,
  hasCDC: false,
  hasPubSub: false,
  version: "Nucleus 1.0.2 (test)",
};

function makeKv(t: RecordingTransport): KVModel {
  return withKV.init(t, FEATURES).kv;
}

describe("KV namespace option symmetry (X04 defect)", () => {
  it("get resolves the same namespace prefix set() wrote", async () => {
    const t = new RecordingTransport();
    const kv = makeKv(t);

    await kv.set("session", "abc", { namespace: "user:1" });
    const setParams = t.calls.at(-1)!.params;
    assert.equal(setParams![0], "user:1:session");

    t.setResult("abc");
    const v = await kv.get("session", { namespace: "user:1" });
    // Fail-before (pre-fix): get ignored the namespace and sent the bare
    // key "session", so the namespaced write was unreachable.
    assert.equal(v, "abc");
    assert.equal(t.calls.at(-1)!.params![0], "user:1:session");
  });

  it("delete/ttl/exists/incr/expire all resolve the namespace", async () => {
    const t = new RecordingTransport();
    const kv = makeKv(t);
    const ns = { namespace: "cfg" };

    await kv.delete("k", ns);
    await kv.ttl("k", ns);
    await kv.exists("k", ns);
    await kv.incr("k", 1, ns);
    await kv.expire("k", 60, ns);
    await kv.cdel("k", "v", ns);
    await kv.cexpire("k", "v", 60, ns);

    const keys = t.calls.slice(-7).map((c) => c.params![0]);
    assert.deepEqual(keys, [
      "cfg:k",
      "cfg:k",
      "cfg:k",
      "cfg:k",
      "cfg:k",
      "cfg:k",
      "cfg:k",
    ]);
  });

  it("scan prefixes the pattern and strips the scope from returned keys", async () => {
    const t = new RecordingTransport();
    const kv = makeKv(t);
    t.setResult(JSON.stringify(["cfg:a", "cfg:b"]));

    const keys = await kv.scan("*", 100, { namespace: "cfg" });
    assert.equal(t.calls.at(-1)!.params![0], "cfg:*");
    assert.deepEqual(keys, ["a", "b"]);
  });
});

describe("KV namespace-bound resource surface", () => {
  it("binding emits ZERO statements (a reference, never DDL)", async () => {
    const t = new RecordingTransport();
    const kv = makeKv(t);
    const ns = kv.namespace("prefs", { schema: "public", table: "users" });
    assert.equal(t.calls.length, 0);
    void ns;
  });

  it("keys are scoped <schema>.<table>:<name>: and isolated from bare keys", async () => {
    const t = new RecordingTransport();
    const kv = makeKv(t);
    const ns = kv.namespace("prefs", { schema: "public", table: "users" });

    await ns.set("theme", "dark");
    assert.equal(t.calls.at(-1)!.params![0], "public.users:prefs:theme");

    // A bare key with the same local name is a DIFFERENT key.
    await kv.set("theme", "light");
    assert.equal(t.calls.at(-1)!.params![0], "theme");
  });

  it("namespaced scan strips the bound scope", async () => {
    const t = new RecordingTransport();
    const kv = makeKv(t);
    const ns = kv.namespace("prefs", { schema: "public", table: "users" });
    t.setResult(JSON.stringify(["public.users:prefs:theme", "public.users:prefs:lang"]));

    const keys = await ns.scan("*");
    assert.equal(t.calls.at(-1)!.params![0], "public.users:prefs:*");
    assert.deepEqual(keys, ["theme", "lang"]);
  });

  it("bound view + per-call namespace: scan strips BOTH scopes (aligned with the root model)", async () => {
    // X04 LOW: this exact combo used to query the right prefixed pattern but
    // strip only the bound scope — returning keys still carrying the inner
    // `cfg:` prefix, while the same call on the root model strips it.
    const t = new RecordingTransport();
    const kv = makeKv(t);
    const ns = kv.namespace("prefs", { schema: "public", table: "users" });
    t.setResult(JSON.stringify(["public.users:prefs:cfg:a", "public.users:prefs:cfg:b"]));

    const keys = await ns.scan("*", 100, { namespace: "cfg" });
    assert.equal(t.calls.at(-1)!.params![0], "public.users:prefs:cfg:*");
    assert.deepEqual(keys, ["a", "b"]);
  });

  it("rejects invalid schema/table identifiers at bind time", () => {
    const t = new RecordingTransport();
    const kv = makeKv(t);
    assert.throws(() => kv.namespace("prefs", { schema: "public; DROP", table: "users" }), /Invalid namespace schema/);
    assert.throws(() => kv.namespace("prefs", { schema: "public", table: "u sers" }), /Invalid namespace table/);
  });

  it("rejects namespace names containing ':'", () => {
    const kv = makeKv(new RecordingTransport());
    assert.throws(() => kv.namespace("a:b"), /':' would make scoped keys ambiguous/);
  });

  it("namespace without identity uses the bare name scope", async () => {
    const t = new RecordingTransport();
    const kv = makeKv(t);
    const ns = kv.namespace("cache");
    await ns.set("k", "v");
    assert.equal(t.calls.at(-1)!.params![0], "cache:k");
  });

  it("exposes collection ops under the scope too", async () => {
    const t = new RecordingTransport();
    const kv = makeKv(t);
    const ns = kv.namespace("q", { schema: "app", table: "jobs" });
    t.setResult(JSON.stringify(["a", "b"]));
    const members = await ns.smembers("done");
    assert.equal(t.calls.at(-1)!.params![0], "app.jobs:q:done");
    assert.deepEqual(members, ["a", "b"]);
  });
});

describe("KV cancellation threading", () => {
  it("an already-aborted signal rejects before anything is sent", async () => {
    const t = new RecordingTransport();
    const kv = makeKv(t);
    const ac = new AbortController();
    ac.abort();
    await assert.rejects(kv.set("k", "v", { signal: ac.signal }));
    await assert.rejects(kv.get("k", { signal: ac.signal }));
    assert.equal(t.calls.length, 0);
  });

  it("a live signal is forwarded to the transport", async () => {
    const t = new RecordingTransport();
    const kv = makeKv(t);
    const ac = new AbortController();
    await kv.set("k", "v", { ttl: 5, signal: ac.signal });
    const last = t.calls.at(-1)!;
    assert.equal(last.opts?.signal, ac.signal);
    assert.deepEqual(last.params, ["k", "v", 5]);
  });
});
