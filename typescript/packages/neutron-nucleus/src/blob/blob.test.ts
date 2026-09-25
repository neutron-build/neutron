// ---------------------------------------------------------------------------
// @neutron-build/nucleus/blob — unit tests (X04)
//
// Wire-level range/stream/cancellation semantics over a recording transport.
// Engine-level truth (byte-exactness, empty blobs, restart, cleanup) lives in
// models.live.test.ts against a real Nucleus.
// ---------------------------------------------------------------------------

import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { withBlob, BlobRangeError, BLOB_MAX_BYTES } from "./index.js";
import type { BlobModel } from "./index.js";
import type { Transport, TransactionTransport, QueryResult, IsolationLevel } from "../types.js";

interface Call {
  method: "query" | "execute" | "fetchval";
  sql: string;
  params?: unknown[];
}

/**
 * Transport with a tiny in-memory blob store speaking the engine's SQL
 * surface (BLOB_STORE/BLOB_GET/BLOB_DELETE/BLOB_META/BLOB_TAG). This is a
 * wire-faithful stub of the ENGINE PROTOCOL, not a mock of the module under
 * test; byte semantics are what the live engine does (hex in, hex out,
 * NULL only when missing).
 */
class BlobProtocolTransport implements Transport {
  readonly calls: Call[] = [];
  readonly store = new Map<string, { hex: string; contentType: string }>();
  private execCount = 0;

  private checkAborted(opts?: { signal?: AbortSignal }): void {
    if (opts?.signal?.aborted) throw new DOMException("This operation was aborted", "AbortError");
  }

  private failTagAfter: number | null = null;

  /** Simulate an abort arriving after N tag statements (mid-tag-loop). */
  abortTagAfter(n: number): void {
    this.failTagAfter = n;
  }

  async query<T = Record<string, unknown>>(sql: string, params: unknown[] = [], opts?: { signal?: AbortSignal }): Promise<QueryResult<T>> {
    this.checkAborted(opts);
    this.calls.push({ method: "query", sql, params });
    const rows = [this.evalSql(sql, params)] as T[];
    return { rows: rows.some((r) => r === undefined) ? ([] as T[]) : rows, rowCount: 1 };
  }

  async execute(sql: string, params: unknown[] = [], opts?: { signal?: AbortSignal }): Promise<number> {
    this.checkAborted(opts);
    this.calls.push({ method: "execute", sql, params });
    this.evalSql(sql, params);
    return 1;
  }

  async fetchval<T = unknown>(sql: string, params: unknown[] = [], opts?: { signal?: AbortSignal }): Promise<T | null> {
    this.checkAborted(opts);
    this.calls.push({ method: "fetchval", sql, params });
    return this.evalSql(sql, params) as T | null;
  }

  async beginTransaction(_isolation?: IsolationLevel): Promise<TransactionTransport> {
    throw new Error("not needed");
  }
  async close(): Promise<void> {}
  async ping(): Promise<void> {}

  private evalSql(sql: string, params: unknown[]): unknown {
    if (sql.startsWith("SELECT BLOB_STORE")) {
      this.execCount++;
      this.store.set(String(params[0]), { hex: String(params[1]), contentType: String(params[2] ?? "application/octet-stream") });
      return true;
    }
    if (sql.startsWith("SELECT BLOB_GET")) {
      return this.store.get(String(params[0]))?.hex ?? null;
    }
    if (sql.startsWith("SELECT BLOB_DELETE")) {
      return this.store.delete(String(params[0]));
    }
    if (sql.startsWith("SELECT BLOB_META")) {
      const e = this.store.get(String(params[0]));
      if (!e) return null;
      return JSON.stringify({ size: e.hex.length / 2, content_type: e.contentType, created_at: 1, updated_at: 1 });
    }
    if (sql.startsWith("SELECT BLOB_TAG")) {
      if (this.failTagAfter != null && this.execCount >= 0) {
        const tagCalls = this.calls.filter((c) => c.sql.startsWith("SELECT BLOB_TAG")).length;
        if (tagCalls >= this.failTagAfter) {
          throw new DOMException("This operation was aborted", "AbortError");
        }
      }
      return true;
    }
    if (sql.startsWith("SELECT BLOB_LIST")) {
      const prefix = String(params[0] ?? "");
      return JSON.stringify([...this.store.keys()].filter((k) => k.startsWith(prefix)));
    }
    if (sql.startsWith("SELECT BLOB_COUNT")) return this.store.size;
    return null;
  }
}

const FEATURES = {
  isNucleus: true,
  hasKV: false,
  hasVector: false,
  hasTimeSeries: false,
  hasDocument: false,
  hasGraph: false,
  hasFTS: false,
  hasGeo: false,
  hasBlob: true,
  hasStreams: false,
  hasColumnar: false,
  hasDatalog: false,
  hasCDC: false,
  hasPubSub: false,
  version: "Nucleus 1.0.2 (test)",
};

function hexOf(bytes: number[]): string {
  return bytes.map((b) => b.toString(16).padStart(2, "0")).join("");
}

function makeBlob(t: BlobProtocolTransport): BlobModel {
  return withBlob.init(t, FEATURES).blob;
}

describe("blob ranges (wire semantics)", () => {
  it("byte-exact slice with clamping at EOF", async () => {
    const t = new BlobProtocolTransport();
    const blob = makeBlob(t);
    // 0..255 twice = 512 bytes
    const data = new Uint8Array(512).map((_, i) => i % 256);
    await blob.put("b", "k", data);

    const whole = await blob.getRange("b", "k", 0, 512);
    assert.deepEqual([...whole!.data], [...data]);
    assert.equal(whole!.total, 512);

    const mid = await blob.getRange("b", "k", 100, 10);
    assert.deepEqual([...mid!.data], [...data.slice(100, 110)]);

    const clamped = await blob.getRange("b", "k", 500, 100);
    assert.deepEqual([...clamped!.data], [...data.slice(500, 512)]);
    assert.equal(clamped!.total, 512);
  });

  it("empty range, offset==size, and empty blob", async () => {
    const t = new BlobProtocolTransport();
    const blob = makeBlob(t);
    await blob.put("b", "k", new Uint8Array([1, 2, 3]));

    const zero = await blob.getRange("b", "k", 1, 0);
    assert.equal(zero!.data.byteLength, 0);
    assert.equal(zero!.total, 3);

    const atEof = await blob.getRange("b", "k", 3, 5);
    assert.equal(atEof!.data.byteLength, 0);
    assert.equal(atEof!.total, 3);

    await blob.put("b", "empty", new Uint8Array(0));
    const empty = await blob.getRange("b", "empty", 0, 10);
    assert.equal(empty!.data.byteLength, 0);
    assert.equal(empty!.total, 0);
    const got = await blob.get("b", "empty");
    assert.equal(got!.data.byteLength, 0);
  });

  it("offset beyond EOF throws BlobRangeError; missing blob returns null", async () => {
    const t = new BlobProtocolTransport();
    const blob = makeBlob(t);
    await blob.put("b", "k", new Uint8Array([1]));

    await assert.rejects(blob.getRange("b", "k", 2, 1), BlobRangeError);
    await assert.rejects(blob.getRange("b", "k", 2, 1), /range not satisfiable/);
    assert.equal(await blob.getRange("b", "missing", 0, 1), null);
  });

  it("negative / non-integer offset and length are rejected", async () => {
    const t = new BlobProtocolTransport();
    const blob = makeBlob(t);
    await assert.rejects(blob.getRange("b", "k", -1, 5), /non-negative integers/);
    await assert.rejects(blob.getRange("b", "k", 0, -5), /non-negative integers/);
    await assert.rejects(blob.getRange("b", "k", 0.5, 5), /non-negative integers/);
  });
});

describe("blob streaming", () => {
  it("openRead concatenates back to the source bytes; chunk size honored", async () => {
    const t = new BlobProtocolTransport();
    const blob = makeBlob(t);
    const data = new Uint8Array(1000).map((_, i) => (i * 7) % 256);
    await blob.put("b", "k", data);

    const chunks: Uint8Array[] = [];
    for await (const c of blob.openRead("b", "k", { chunkBytes: 64 })) chunks.push(c);
    assert.equal(chunks.length, Math.ceil(1000 / 64));
    assert.equal(chunks.at(-1)!.byteLength, 1000 % 64);
    const joined = new Uint8Array(1000);
    let at = 0;
    for (const c of chunks) { joined.set(c, at); at += c.byteLength; }
    assert.deepEqual([...joined], [...data]);
  });

  it("openRead aborts between chunks", async () => {
    const t = new BlobProtocolTransport();
    const blob = makeBlob(t);
    await blob.put("b", "k", new Uint8Array(300));
    const ac = new AbortController();
    const collected: Uint8Array[] = [];
    await assert.rejects(async () => {
      for await (const c of blob.openRead("b", "k", { chunkBytes: 100, signal: ac.signal })) {
        collected.push(c);
        if (collected.length === 2) ac.abort();
      }
    }, /aborted/);
    assert.equal(collected.length, 2);
  });

  it("openRead rejects invalid chunkBytes and missing blobs", async () => {
    const t = new BlobProtocolTransport();
    const blob = makeBlob(t);
    await assert.rejects(async () => {
      for await (const _ of blob.openRead("b", "k", { chunkBytes: 0 })) void _;
    }, /chunkBytes/);
    await assert.rejects(async () => {
      for await (const _ of blob.openRead("b", "missing")) void _;
    }, /not found/);
  });

  it("putStream concatenates and stores once; abort mid-accumulation stores nothing", async () => {
    const t = new BlobProtocolTransport();
    const blob = makeBlob(t);

    async function* src(): AsyncIterable<Uint8Array> {
      yield new Uint8Array([1, 2]);
      yield new Uint8Array([3, 4, 5]);
    }
    const res = await blob.putStream("b", "k", src());
    assert.equal(res.size, 5);
    const stores = t.calls.filter((c) => c.sql.startsWith("SELECT BLOB_STORE"));
    assert.equal(stores.length, 1);
    assert.equal(hexOf([1, 2, 3, 4, 5]), stores[0].params![1]);

    const before = t.store.size;
    const ac = new AbortController();
    async function* slow(): AsyncIterable<Uint8Array> {
      yield new Uint8Array([9]);
      ac.abort();
      yield new Uint8Array([9, 9]);
    }
    await assert.rejects(blob.putStream("b", "k2", slow(), { signal: ac.signal }), /aborted/);
    assert.equal(t.store.size, before);
  });

  it("putStream enforces the engine's 100 MB cap before storing", async () => {
    const t = new BlobProtocolTransport();
    const blob = makeBlob(t);
    const big = new Uint8Array(BLOB_MAX_BYTES + 1);
    async function* oneShot(): AsyncIterable<Uint8Array> {
      yield big;
    }
    await assert.rejects(blob.putStream("b", "k", oneShot()), /BLOB_TOO_LARGE|cap/);
    assert.equal(t.calls.filter((c) => c.sql.startsWith("SELECT BLOB_STORE")).length, 0);
  });
});

describe("blob put cancellation cleanup (no partial blob)", () => {
  it("abort during the tag loop deletes the stored blob and rethrows", async () => {
    const t = new BlobProtocolTransport();
    const blob = makeBlob(t);
    t.abortTagAfter(1);

    await assert.rejects(
      blob.put("b", "k", new Uint8Array([1, 2, 3]), {
        contentType: "text/plain",
        metadata: { a: "1", b: "2" },
      }),
      /aborted/,
    );
    assert.equal(t.store.size, 0, "canceled put must leave no blob");
    const del = t.calls.filter((c) => c.sql.startsWith("SELECT BLOB_DELETE"));
    assert.equal(del.length, 1);
  });

  it("a completed put with metadata stores blob + tags", async () => {
    const t = new BlobProtocolTransport();
    const blob = makeBlob(t);
    await blob.put("b", "k", new Uint8Array([1]), { contentType: "text/csv", metadata: { x: "y" } });
    assert.equal(t.store.size, 1);
    assert.equal(t.store.get("b/k")!.contentType, "text/csv");
    assert.equal(t.calls.filter((c) => c.sql.startsWith("SELECT BLOB_TAG")).length, 1);
  });
});

describe("blob identity-bound buckets", () => {
  it("binding emits zero statements; keys scoped <schema>.<table>:<name>/", async () => {
    const t = new BlobProtocolTransport();
    const blob = makeBlob(t);
    const b = blob.bucket("avatars", { schema: "public", table: "users" });
    assert.equal(t.calls.length, 0);
    await b.put("k", new Uint8Array([7]));
    assert.equal(t.calls.at(-1)!.params![0], "public.users:avatars/k");

    // Scoped listing strips the scope; bare bucket unaffected.
    assert.deepEqual(await b.list(""), ["k"]);
    assert.deepEqual(await blob.list("avatars", ""), []);
  });

  it("rejects invalid identifiers and ambiguous bucket names", () => {
    const blob = makeBlob(new BlobProtocolTransport());
    assert.throws(() => blob.bucket("a/b"), /ambiguous/);
    assert.throws(() => blob.bucket("a:b"), /ambiguous/);
    assert.throws(() => blob.bucket("ok", { schema: "x y", table: "t" }), /Invalid bucket schema/);
  });
});
