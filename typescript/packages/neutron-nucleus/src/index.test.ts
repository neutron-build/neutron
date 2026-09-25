import assert from "node:assert/strict";
import { describe, it, beforeEach } from "node:test";

// ---------------------------------------------------------------------------
// We import real exports from the package — errors, helpers, types, and the
// client builder. Model plugins are tested by verifying the SQL they generate
// against a mock transport.
// ---------------------------------------------------------------------------

import {
  NucleusError,
  NucleusConnectionError,
  NucleusQueryError,
  NucleusNotFoundError,
  NucleusConflictError,
  NucleusTransactionError,
  NucleusFeatureError,
  NucleusAuthError,
  createClient,
  HttpTransport,
  createTransport,
  detectFeatures,
  migrate,
  migrateDown,
  migrationStatus,
  adoptMigrations,
  forceUnlockMigrations,
  migrationLockInfo,
  migrationChecksum,
  legacyGoSdkChecksum,
} from "./index.js";

import type {
  Transport,
  TransactionTransport,
  QueryResult,
  IsolationLevel,
  NucleusFeatures,
} from "./types.js";

import { requireNucleus, assertIdentifier } from "./helpers.js";

// Model plugins
import { withSQL } from "./sql/index.js";
import { withKV } from "./kv/index.js";
import { withVector } from "./vector/index.js";
import { withDocument } from "./document/index.js";
import { withGraph } from "./graph/index.js";
import { withFTS } from "./fts/index.js";
import { withGeo } from "./geo/index.js";
import { withBlob } from "./blob/index.js";
import { withTimeSeries } from "./timeseries/index.js";
import { withStreams } from "./streams/index.js";
import { withColumnar } from "./columnar/index.js";
import { withDatalog } from "./datalog/index.js";
import { withCDC } from "./cdc/index.js";
import { withPubSub } from "./pubsub/index.js";

import type { Migration } from "./migrate.js";

// =========================================================================
// Mock Transport
// =========================================================================

interface MockCall {
  method: string;
  args: unknown[];
}

class MockTransport implements Transport {
  readonly calls: MockCall[] = [];
  queryResults: Map<string, unknown> = new Map();
  fetchvalResults: Map<string, unknown> = new Map();
  executeResult = 0;
  private txIdCounter = 0;

  reset(): void {
    this.calls.length = 0;
    this.queryResults.clear();
    this.fetchvalResults.clear();
    this.executeResult = 0;
  }

  /** Register a fetchval result keyed by the beginning of the SQL. */
  onFetchval(sqlPrefix: string, result: unknown): void {
    this.fetchvalResults.set(sqlPrefix, result);
  }

  /** Register a query result keyed by the beginning of the SQL. */
  onQuery(sqlPrefix: string, rows: unknown[]): void {
    this.queryResults.set(sqlPrefix, rows);
  }

  async query<T = Record<string, unknown>>(sql: string, params: unknown[] = []): Promise<QueryResult<T>> {
    this.calls.push({ method: "query", args: [sql, params] });
    for (const [prefix, rows] of this.queryResults) {
      if (sql.startsWith(prefix)) {
        const r = rows as T[];
        return { rows: r, rowCount: r.length };
      }
    }
    return { rows: [], rowCount: 0 };
  }

  async execute(sql: string, params: unknown[] = []): Promise<number> {
    this.calls.push({ method: "execute", args: [sql, params] });
    return this.executeResult;
  }

  async fetchval<T = unknown>(sql: string, params: unknown[] = []): Promise<T | null> {
    this.calls.push({ method: "fetchval", args: [sql, params] });
    for (const [prefix, result] of this.fetchvalResults) {
      if (sql.startsWith(prefix)) {
        return result as T;
      }
    }
    return null;
  }

  async beginTransaction(_isolationLevel?: IsolationLevel): Promise<TransactionTransport> {
    this.calls.push({ method: "beginTransaction", args: [_isolationLevel] });
    return new MockTxTransport(this);
  }

  async close(): Promise<void> {
    this.calls.push({ method: "close", args: [] });
  }

  async ping(): Promise<void> {
    this.calls.push({ method: "ping", args: [] });
  }
}

class MockTxTransport implements TransactionTransport {
  private finished = false;

  constructor(private readonly parent: MockTransport) {}

  async query<T = Record<string, unknown>>(sql: string, params: unknown[] = []): Promise<QueryResult<T>> {
    return this.parent.query<T>(sql, params);
  }

  async execute(sql: string, params: unknown[] = []): Promise<number> {
    return this.parent.execute(sql, params);
  }

  async fetchval<T = unknown>(sql: string, params: unknown[] = []): Promise<T | null> {
    return this.parent.fetchval<T>(sql, params);
  }

  async beginTransaction(): Promise<TransactionTransport> {
    throw new NucleusTransactionError("Nested transactions are not supported");
  }

  async commit(): Promise<void> {
    this.finished = true;
  }

  async rollback(): Promise<void> {
    this.finished = true;
  }

  async close(): Promise<void> {
    if (!this.finished) await this.rollback();
  }

  async ping(): Promise<void> {
    await this.query("SELECT 1");
  }
}

// =========================================================================
// Helpers for features
// =========================================================================

/** MockTransport variant that serves distinct documents per id (DOC_GET). */
class DocStoreTransport extends MockTransport {
  docs = new Map<string, string>();
  ids: number[] = [];

  override async fetchval<T = unknown>(sql: string, params: unknown[] = []): Promise<T | null> {
    if (sql.startsWith("SELECT DOC_GET")) {
      return (this.docs.get(String(params[1])) ?? null) as T;
    }
    if (sql.startsWith("SELECT DOC_QUERY")) {
      return this.ids.join(",") as unknown as T;
    }
    return super.fetchval<T>(sql, params);
  }
}

function nucleusFeatures(): NucleusFeatures {
  return {
    isNucleus: true,
    hasKV: true,
    hasVector: true,
    hasTimeSeries: true,
    hasDocument: true,
    hasGraph: true,
    hasFTS: true,
    hasGeo: true,
    hasBlob: true,
    hasStreams: true,
    hasColumnar: true,
    hasDatalog: true,
    hasCDC: true,
    hasPubSub: true,
    version: "Nucleus 0.1.0",
  };
}

function pgFeatures(): NucleusFeatures {
  return {
    isNucleus: false,
    hasKV: false,
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
    version: "PostgreSQL 16.0",
  };
}

// =========================================================================
// Tests
// =========================================================================

// ---------------------------------------------------------------------------
// Error hierarchy
// ---------------------------------------------------------------------------

describe("NucleusError hierarchy", () => {
  it("NucleusError has code and message", () => {
    const err = new NucleusError("TEST", "test message");
    assert.equal(err.code, "TEST");
    assert.equal(err.message, "test message");
    assert.equal(err.name, "NucleusError");
  });

  it("NucleusError accepts meta and cause", () => {
    const cause = new Error("root");
    const err = new NucleusError("X", "msg", { cause, meta: { key: "val" } });
    assert.equal(err.cause, cause);
    assert.deepEqual(err.meta, { key: "val" });
  });

  it("NucleusConnectionError has CONNECTION_ERROR code", () => {
    const err = new NucleusConnectionError("offline");
    assert.equal(err.code, "CONNECTION_ERROR");
    assert.equal(err.name, "NucleusConnectionError");
    assert.ok(err instanceof NucleusError);
  });

  it("NucleusQueryError has QUERY_ERROR code", () => {
    const err = new NucleusQueryError("bad sql");
    assert.equal(err.code, "QUERY_ERROR");
    assert.equal(err.name, "NucleusQueryError");
    assert.ok(err instanceof NucleusError);
  });

  it("NucleusNotFoundError has NOT_FOUND code", () => {
    const err = new NucleusNotFoundError("row missing");
    assert.equal(err.code, "NOT_FOUND");
    assert.equal(err.name, "NucleusNotFoundError");
    assert.ok(err instanceof NucleusError);
  });

  it("NucleusConflictError has CONFLICT code", () => {
    const err = new NucleusConflictError("duplicate key");
    assert.equal(err.code, "CONFLICT");
    assert.equal(err.name, "NucleusConflictError");
    assert.ok(err instanceof NucleusError);
  });

  it("NucleusTransactionError has TRANSACTION_ERROR code", () => {
    const err = new NucleusTransactionError("commit failed");
    assert.equal(err.code, "TRANSACTION_ERROR");
    assert.equal(err.name, "NucleusTransactionError");
    assert.ok(err instanceof NucleusError);
  });

  it("NucleusFeatureError includes feature name in message", () => {
    const err = new NucleusFeatureError("KV");
    assert.equal(err.code, "FEATURE_UNAVAILABLE");
    assert.ok(err.message.includes("KV"));
    assert.ok(err.message.includes("Nucleus"));
    assert.equal(err.name, "NucleusFeatureError");
    assert.ok(err instanceof NucleusError);
  });

  it("NucleusAuthError has AUTH_ERROR code", () => {
    const err = new NucleusAuthError("forbidden");
    assert.equal(err.code, "AUTH_ERROR");
    assert.equal(err.name, "NucleusAuthError");
    assert.ok(err instanceof NucleusError);
  });
});

// ---------------------------------------------------------------------------
// helpers: requireNucleus
// ---------------------------------------------------------------------------

describe("requireNucleus", () => {
  it("does not throw for Nucleus features", () => {
    assert.doesNotThrow(() => requireNucleus(nucleusFeatures(), "KV"));
  });

  it("throws NucleusFeatureError for PostgreSQL features", () => {
    assert.throws(() => requireNucleus(pgFeatures(), "KV"), NucleusFeatureError);
  });
});

// ---------------------------------------------------------------------------
// helpers: assertIdentifier
// ---------------------------------------------------------------------------

describe("assertIdentifier", () => {
  it("accepts valid SQL identifiers", () => {
    assert.doesNotThrow(() => assertIdentifier("users", "table name"));
    assert.doesNotThrow(() => assertIdentifier("_private", "column"));
    assert.doesNotThrow(() => assertIdentifier("table123", "name"));
  });

  it("rejects identifiers with spaces", () => {
    assert.throws(() => assertIdentifier("my table", "name"));
  });

  it("rejects identifiers starting with numbers", () => {
    assert.throws(() => assertIdentifier("123table", "name"));
  });

  it("rejects identifiers with special characters", () => {
    assert.throws(() => assertIdentifier("table;DROP", "name"));
    assert.throws(() => assertIdentifier("table-name", "name"));
  });

  it("rejects empty string", () => {
    assert.throws(() => assertIdentifier("", "name"));
  });
});

// ---------------------------------------------------------------------------
// Feature detection
// ---------------------------------------------------------------------------

describe("detectFeatures", () => {
  it("returns isNucleus=true when version contains Nucleus", async () => {
    const transport = new MockTransport();
    transport.onFetchval("SELECT VERSION()", "Nucleus 0.1.0");

    const features = await detectFeatures(transport);
    assert.equal(features.isNucleus, true);
    assert.equal(features.hasKV, true);
    assert.equal(features.hasVector, true);
    assert.equal(features.version, "Nucleus 0.1.0");
  });

  it("returns isNucleus=false for PostgreSQL", async () => {
    const transport = new MockTransport();
    transport.onFetchval("SELECT VERSION()", "PostgreSQL 16.0");

    const features = await detectFeatures(transport);
    assert.equal(features.isNucleus, false);
    assert.equal(features.hasKV, false);
    assert.equal(features.hasVector, false);
    assert.equal(features.version, "PostgreSQL 16.0");
  });

  it("returns isNucleus=false when version is null", async () => {
    const transport = new MockTransport();
    const features = await detectFeatures(transport);
    assert.equal(features.isNucleus, false);
    assert.equal(features.version, "");
  });
});

// ---------------------------------------------------------------------------
// Client builder
// ---------------------------------------------------------------------------

describe("createClient", () => {
  it("returns a builder with use() and connect() methods", () => {
    const builder = createClient({ url: "http://localhost:3000", transport: new MockTransport() });
    assert.equal(typeof builder.use, "function");
    assert.equal(typeof builder.connect, "function");
  });

  it("connects and creates a base client", async () => {
    const transport = new MockTransport();
    transport.onFetchval("SELECT VERSION()", "Nucleus 0.1.0");

    const client = await createClient({ url: "http://localhost:3000", transport }).connect();
    assert.equal(client.features.isNucleus, true);
    assert.equal(typeof client.close, "function");
    assert.equal(typeof client.ping, "function");
  });

  it("merges plugin contributions via use()", async () => {
    const transport = new MockTransport();
    transport.onFetchval("SELECT VERSION()", "Nucleus 0.1.0");

    const client = await createClient({ url: "http://localhost:3000", transport })
      .use(withSQL)
      .use(withKV)
      .connect();

    assert.ok("sql" in client);
    assert.ok("kv" in client);
    assert.equal(typeof client.sql.query, "function");
    assert.equal(typeof client.kv.get, "function");
  });

  it("chains multiple plugins", async () => {
    const transport = new MockTransport();
    transport.onFetchval("SELECT VERSION()", "Nucleus 0.1.0");

    const client = await createClient({ url: "http://localhost:3000", transport })
      .use(withSQL)
      .use(withKV)
      .use(withVector)
      .use(withDocument)
      .use(withGraph)
      .use(withFTS)
      .use(withGeo)
      .use(withBlob)
      .use(withTimeSeries)
      .use(withStreams)
      .use(withColumnar)
      .use(withDatalog)
      .use(withCDC)
      .use(withPubSub)
      .connect();

    // All 14 model properties should exist
    assert.ok("sql" in client);
    assert.ok("kv" in client);
    assert.ok("vector" in client);
    assert.ok("document" in client);
    assert.ok("graph" in client);
    assert.ok("fts" in client);
    assert.ok("geo" in client);
    assert.ok("blob" in client);
    assert.ok("timeseries" in client);
    assert.ok("streams" in client);
    assert.ok("columnar" in client);
    assert.ok("datalog" in client);
    assert.ok("cdc" in client);
    assert.ok("pubsub" in client);
  });
});

// ---------------------------------------------------------------------------
// HttpTransport
// ---------------------------------------------------------------------------

describe("HttpTransport", () => {
  it("strips trailing slash from URL", () => {
    const transport = new HttpTransport("http://localhost:3000/");
    // The transport is created without error; we verify by checking close doesn't throw
    assert.doesNotReject(() => transport.close());
  });
});

// ---------------------------------------------------------------------------
// createTransport
// ---------------------------------------------------------------------------

describe("createTransport", () => {
  it("returns an HttpTransport in a standard environment", () => {
    const transport = createTransport({ url: "http://localhost:3000" });
    assert.ok(transport instanceof HttpTransport);
  });
});

// ---------------------------------------------------------------------------
// SQL plugin
// ---------------------------------------------------------------------------

describe("withSQL plugin", () => {
  let transport: MockTransport;
  let sql: ReturnType<typeof withSQL.init>["sql"];

  beforeEach(() => {
    transport = new MockTransport();
    sql = withSQL.init(transport, nucleusFeatures()).sql;
  });

  it("has name sql", () => {
    assert.equal(withSQL.name, "sql");
  });

  it("query returns rows", async () => {
    transport.onQuery("SELECT", [{ id: 1, name: "Alice" }]);
    const rows = await sql.query("SELECT * FROM users");
    assert.deepEqual(rows, [{ id: 1, name: "Alice" }]);
  });

  it("queryOne returns first row", async () => {
    transport.onQuery("SELECT", [{ id: 1 }]);
    const row = await sql.queryOne("SELECT * FROM users WHERE id = $1", 1);
    assert.deepEqual(row, { id: 1 });
  });

  it("queryOne throws NotFoundError when no rows", async () => {
    await assert.rejects(() => sql.queryOne("SELECT * FROM users WHERE id = $1", 999), NucleusNotFoundError);
  });

  it("queryOneOrNull returns null when no rows", async () => {
    const row = await sql.queryOneOrNull("SELECT * FROM users WHERE id = $1", 999);
    assert.equal(row, null);
  });

  it("execute calls transport.execute", async () => {
    transport.executeResult = 3;
    const count = await sql.execute("DELETE FROM users WHERE active = $1", false);
    assert.equal(count, 3);
    assert.equal(transport.calls[0].method, "execute");
  });

  it("executeBatch runs each statement", async () => {
    transport.executeResult = 1;
    const counts = await sql.executeBatch([
      { sql: "INSERT INTO a VALUES ($1)", params: [1] },
      { sql: "INSERT INTO b VALUES ($1)", params: [2] },
    ]);
    assert.deepEqual(counts, [1, 1]);
  });

  it("fetchval returns scalar value", async () => {
    transport.onFetchval("SELECT COUNT", 42);
    const count = await sql.fetchval("SELECT COUNT(*) FROM users");
    assert.equal(count, 42);
  });

  it("transaction commits on success", async () => {
    const result = await sql.transaction(async (tx) => {
      await tx.execute("INSERT INTO users VALUES ($1)", 1);
      return "done";
    });
    assert.equal(result, "done");
  });

  it("transaction rolls back on error", async () => {
    await assert.rejects(
      () =>
        sql.transaction(async (_tx) => {
          throw new Error("fail");
        }),
      Error
    );
  });
});

// ---------------------------------------------------------------------------
// KV plugin
// ---------------------------------------------------------------------------

describe("withKV plugin", () => {
  let transport: MockTransport;
  let kv: ReturnType<typeof withKV.init>["kv"];

  beforeEach(() => {
    transport = new MockTransport();
    kv = withKV.init(transport, nucleusFeatures()).kv;
  });

  it("has name kv", () => {
    assert.equal(withKV.name, "kv");
  });

  it("get sends KV_GET SQL", async () => {
    transport.onFetchval("SELECT KV_GET", "hello");
    const val = await kv.get("key1");
    assert.equal(val, "hello");
    const call = transport.calls[0];
    assert.equal(call.method, "fetchval");
    assert.deepEqual((call.args[1] as unknown[]), ["key1"]);
  });

  it("getTyped parses JSON", async () => {
    transport.onFetchval("SELECT KV_GET", '{"a":1}');
    const val = await kv.getTyped<{ a: number }>("key1");
    assert.deepEqual(val, { a: 1 });
  });

  it("getTyped returns null for missing key", async () => {
    const val = await kv.getTyped("missing");
    assert.equal(val, null);
  });

  it("set sends KV_SET SQL without TTL", async () => {
    await kv.set("key", "value");
    const call = transport.calls[0];
    assert.equal(call.method, "execute");
    assert.ok((call.args[0] as string).includes("KV_SET"));
  });

  it("set sends KV_SET SQL with TTL", async () => {
    await kv.set("key", "value", { ttl: 60 });
    const call = transport.calls[0];
    assert.ok((call.args[0] as string).includes("KV_SET"));
    assert.ok((call.args[1] as unknown[]).includes(60));
  });

  it("set prepends namespace", async () => {
    await kv.set("key", "value", { namespace: "cache" });
    const call = transport.calls[0];
    assert.ok((call.args[1] as unknown[]).includes("cache:key"));
  });

  it("setNX sends KV_SETNX SQL", async () => {
    transport.onFetchval("SELECT KV_SETNX", true);
    const result = await kv.setNX("key", "value");
    assert.equal(result, true);
  });

  it("setNX prepends namespace like its siblings", async () => {
    transport.onFetchval("SELECT KV_SETNX", true);
    await kv.setNX("key", "value", { namespace: "cache", ttl: 30 });
    const call = transport.calls[0];
    const params = call.args[1] as unknown[];
    assert.ok(params.includes("cache:key"));
    assert.ok(params.includes(30));
  });

  it("delete sends KV_DEL SQL", async () => {
    transport.onFetchval("SELECT KV_DEL", true);
    const result = await kv.delete("key");
    assert.equal(result, true);
  });

  it("exists sends KV_EXISTS SQL", async () => {
    transport.onFetchval("SELECT KV_EXISTS", true);
    const result = await kv.exists("key");
    assert.equal(result, true);
  });

  it("incr sends KV_INCR SQL", async () => {
    transport.onFetchval("SELECT KV_INCR", 5);
    const result = await kv.incr("counter");
    assert.equal(result, 5);
  });

  it("incr with amount sends KV_INCR with two params", async () => {
    transport.onFetchval("SELECT KV_INCR", 10);
    await kv.incr("counter", 5);
    const call = transport.calls[0];
    assert.ok((call.args[1] as unknown[]).includes(5));
  });

  it("throws on PostgreSQL for KV operations", async () => {
    const pgKv = withKV.init(transport, pgFeatures()).kv;
    await assert.rejects(() => pgKv.get("key"), NucleusFeatureError);
  });

  // List operations
  it("lpush sends KV_LPUSH SQL", async () => {
    transport.onFetchval("SELECT KV_LPUSH", 3);
    const len = await kv.lpush("list", "val");
    assert.equal(len, 3);
  });

  it("rpop sends KV_RPOP SQL", async () => {
    transport.onFetchval("SELECT KV_RPOP", "last");
    const val = await kv.rpop("list");
    assert.equal(val, "last");
  });

  // Hash operations
  it("hset sends KV_HSET SQL", async () => {
    transport.onFetchval("SELECT KV_HSET", true);
    const result = await kv.hset("hash", "field", "value");
    assert.equal(result, true);
  });

  it("hget sends KV_HGET SQL", async () => {
    transport.onFetchval("SELECT KV_HGET", "value");
    const val = await kv.hget("hash", "field");
    assert.equal(val, "value");
  });

  // Set operations
  it("sadd sends KV_SADD SQL", async () => {
    transport.onFetchval("SELECT KV_SADD", true);
    const result = await kv.sadd("myset", "member");
    assert.equal(result, true);
  });

  // Sorted set operations
  it("zadd sends KV_ZADD SQL", async () => {
    transport.onFetchval("SELECT KV_ZADD", true);
    const result = await kv.zadd("zset", 1.5, "member");
    assert.equal(result, true);
  });

  // HyperLogLog
  it("pfadd sends KV_PFADD SQL", async () => {
    transport.onFetchval("SELECT KV_PFADD", true);
    const result = await kv.pfadd("hll", "element");
    assert.equal(result, true);
  });
});

// ---------------------------------------------------------------------------
// Vector plugin
// ---------------------------------------------------------------------------

describe("withVector plugin", () => {
  let transport: MockTransport;
  let vector: ReturnType<typeof withVector.init>["vector"];

  beforeEach(() => {
    transport = new MockTransport();
    vector = withVector.init(transport, nucleusFeatures()).vector;
  });

  it("has name vector", () => {
    assert.equal(withVector.name, "vector");
  });

  it("createCollection sends CREATE TABLE and CREATE INDEX", async () => {
    await vector.createCollection("embeddings", 384);
    assert.equal(transport.calls.length, 2);
    assert.ok((transport.calls[0].args[0] as string).includes("CREATE TABLE"));
    assert.equal(
      transport.calls[0].args[0],
      "CREATE TABLE IF NOT EXISTS embeddings (rid BIGSERIAL PRIMARY KEY, id TEXT NOT NULL UNIQUE, embedding VECTOR(384), metadata JSONB DEFAULT '{}')",
    );
    assert.equal(
      transport.calls[1].args[0],
      "CREATE INDEX IF NOT EXISTS idx_embeddings_embedding ON embeddings USING HNSW (embedding) WITH (metric = 'cosine')",
    );
  });

  it("createCollection validates identifier", async () => {
    await assert.rejects(() => vector.createCollection("drop;--", 3), Error);
  });

  it("insert sends INSERT with VECTOR()", async () => {
    await vector.insert("embeddings", "doc1", [1, 0, 0]);
    const call = transport.calls[0];
    assert.ok((call.args[0] as string).includes("INSERT INTO embeddings"));
    assert.ok((call.args[0] as string).includes("VECTOR($2)"));
  });

  it("delete sends DELETE FROM", async () => {
    await vector.delete("embeddings", "doc1");
    const call = transport.calls[0];
    assert.ok((call.args[0] as string).includes("DELETE FROM embeddings"));
  });

  it("search sends VECTOR_DISTANCE query", async () => {
    transport.onQuery("SELECT id", [{ id: "doc1", metadata: '{"title":"test"}', distance: 0.5 }]);
    const results = await vector.search("embeddings", [1, 0, 0], { limit: 5 });
    assert.equal(results.length, 1);
    assert.equal(results[0].distance, 0.5);
    assert.equal(results[0].score, 2); // 1/0.5
  });

  it("search with filter adds WHERE clause", async () => {
    transport.onQuery("SELECT id", []);
    await vector.search("embeddings", [1, 0, 0], { filter: { type: "article" } });
    const call = transport.calls[0];
    assert.ok((call.args[0] as string).includes("WHERE"));
    assert.ok((call.args[0] as string).includes("metadata"));
  });

  it("dims sends VECTOR_DIMS", async () => {
    transport.onFetchval("SELECT VECTOR_DIMS", 3);
    const d = await vector.dims([1, 2, 3]);
    assert.equal(d, 3);
  });

  it("distance sends VECTOR_DISTANCE", async () => {
    transport.onFetchval("SELECT VECTOR_DISTANCE", 0.25);
    const d = await vector.distance([1, 0], [0, 1], "l2");
    assert.equal(d, 0.25);
  });

  it("throws on PostgreSQL", async () => {
    const pgVec = withVector.init(transport, pgFeatures()).vector;
    await assert.rejects(() => pgVec.dims([1, 2, 3]), NucleusFeatureError);
  });
});

// ---------------------------------------------------------------------------
// Document plugin
// ---------------------------------------------------------------------------

describe("withDocument plugin", () => {
  let transport: MockTransport;
  let doc: ReturnType<typeof withDocument.init>["document"];

  beforeEach(() => {
    transport = new MockTransport();
    doc = withDocument.init(transport, nucleusFeatures()).document;
  });

  it("has name document", () => {
    assert.equal(withDocument.name, "document");
  });

  it("insert sends DOC_INSERT", async () => {
    transport.onFetchval("SELECT DOC_INSERT", 1);
    const id = await doc.insert("posts", { title: "Hello" });
    assert.equal(id, 1);
  });

  it("get sends DOC_GET", async () => {
    transport.onFetchval("SELECT DOC_GET", '{"title":"Hello"}');
    const result = await doc.get(1);
    assert.deepEqual(result, { title: "Hello" });
  });

  it("get returns null for missing doc", async () => {
    const result = await doc.get(999);
    assert.equal(result, null);
  });

  it("count sends DOC_COUNT", async () => {
    transport.onFetchval("SELECT DOC_COUNT", 5);
    const c = await doc.count();
    assert.equal(c, 5);
  });

  it("throws on PostgreSQL", async () => {
    const pgDoc = withDocument.init(transport, pgFeatures()).document;
    await assert.rejects(() => pgDoc.insert("coll", {}), NucleusFeatureError);
  });

  it("find sorts numeric fields numerically, not lexicographically", async () => {
    const docTransport = new DocStoreTransport();
    docTransport.ids = [1, 2, 3];
    docTransport.docs.set("1", JSON.stringify({ title: "a", count: 10 }));
    docTransport.docs.set("2", JSON.stringify({ title: "b", count: 9 }));
    docTransport.docs.set("3", JSON.stringify({ title: "c", count: 2 }));
    const docStore = withDocument.init(docTransport, nucleusFeatures()).document;

    const out = await docStore.find("posts", {}, { sortField: "count" });
    assert.deepEqual(
      out.map((d) => d.count),
      [2, 9, 10],
    );
  });

  it("findTyped honors sortField, sortAsc and fields", async () => {
    const docTransport = new DocStoreTransport();
    docTransport.ids = [1, 2, 3];
    docTransport.docs.set("1", JSON.stringify({ title: "a", count: 10 }));
    docTransport.docs.set("2", JSON.stringify({ title: "b", count: 9 }));
    docTransport.docs.set("3", JSON.stringify({ title: "c", count: 2 }));
    const docStore = withDocument.init(docTransport, nucleusFeatures()).document;

    const out = await docStore.findTyped<{ count: number }>("posts", {}, {
      sortField: "count",
      sortAsc: false,
      fields: ["count"],
    });
    assert.deepEqual(out, [{ count: 10 }, { count: 9 }, { count: 2 }]);
  });
});

// ---------------------------------------------------------------------------
// Graph plugin
// ---------------------------------------------------------------------------

describe("withGraph plugin", () => {
  let transport: MockTransport;
  let graph: ReturnType<typeof withGraph.init>["graph"];

  beforeEach(() => {
    transport = new MockTransport();
    graph = withGraph.init(transport, nucleusFeatures()).graph;
  });

  it("has name graph", () => {
    assert.equal(withGraph.name, "graph");
  });

  it("addNode sends GRAPH_ADD_NODE", async () => {
    transport.onFetchval("SELECT GRAPH_ADD_NODE", 1);
    const id = await graph.addNode(["Person"]);
    assert.equal(id, 1);
    assert.ok((transport.calls[0].args[0] as string).includes("GRAPH_ADD_NODE"));
  });

  it("addNode with properties includes JSON", async () => {
    transport.onFetchval("SELECT GRAPH_ADD_NODE", 2);
    await graph.addNode(["Person"], { name: "Alice" });
    const call = transport.calls[0];
    assert.ok((call.args[1] as unknown[]).length === 2);
  });

  it("addEdge sends GRAPH_ADD_EDGE", async () => {
    transport.onFetchval("SELECT GRAPH_ADD_EDGE", 10);
    const id = await graph.addEdge(1, 2, "KNOWS");
    assert.equal(id, 10);
  });

  it("deleteNode sends GRAPH_DELETE_NODE", async () => {
    transport.onFetchval("SELECT GRAPH_DELETE_NODE", true);
    const result = await graph.deleteNode(1);
    assert.equal(result, true);
  });

  it("query sends GRAPH_QUERY", async () => {
    transport.onFetchval("SELECT GRAPH_QUERY", '{"columns":["n"],"rows":[{"n":1}]}');
    const result = await graph.query("MATCH (n) RETURN n");
    assert.deepEqual(result.columns, ["n"]);
    assert.equal(result.rows.length, 1);
  });

  it("query returns empty for null result", async () => {
    const result = await graph.query("MATCH (n) RETURN n");
    assert.deepEqual(result, { columns: [], rows: [] });
  });

  it("nodeCount sends GRAPH_NODE_COUNT", async () => {
    transport.onFetchval("SELECT GRAPH_NODE_COUNT", 10);
    const count = await graph.nodeCount();
    assert.equal(count, 10);
  });

  it("shortestPath sends GRAPH_SHORTEST_PATH", async () => {
    transport.onFetchval("SELECT GRAPH_SHORTEST_PATH", "[1,2,3]");
    const path = await graph.shortestPath(1, 3);
    assert.deepEqual(path, [1, 2, 3]);
  });

  it("throws on PostgreSQL", async () => {
    const pgGraph = withGraph.init(transport, pgFeatures()).graph;
    await assert.rejects(() => pgGraph.addNode(["X"]), NucleusFeatureError);
  });
});

// ---------------------------------------------------------------------------
// FTS plugin
// ---------------------------------------------------------------------------

describe("withFTS plugin", () => {
  let transport: MockTransport;
  let fts: ReturnType<typeof withFTS.init>["fts"];

  beforeEach(() => {
    transport = new MockTransport();
    fts = withFTS.init(transport, nucleusFeatures()).fts;
  });

  it("has name fts", () => {
    assert.equal(withFTS.name, "fts");
  });

  it("index sends FTS_INDEX", async () => {
    transport.onFetchval("SELECT FTS_INDEX", true);
    const result = await fts.index(1, "Hello world");
    assert.equal(result, true);
  });

  it("search sends FTS_SEARCH", async () => {
    transport.onFetchval("SELECT FTS_SEARCH", '[{"doc_id":1,"score":0.95}]');
    const results = await fts.search("hello");
    assert.equal(results.length, 1);
    assert.equal(results[0].docId, 1);
  });

  it("search with fuzzy sends FTS_FUZZY_SEARCH", async () => {
    transport.onFetchval("SELECT FTS_FUZZY_SEARCH", '[{"doc_id":1,"score":0.8}]');
    const results = await fts.search("hllo", { fuzzyDistance: 1 });
    assert.equal(results.length, 1);
  });

  it("remove sends FTS_REMOVE", async () => {
    transport.onFetchval("SELECT FTS_REMOVE", true);
    const result = await fts.remove(1);
    assert.equal(result, true);
  });

  it("docCount sends FTS_DOC_COUNT", async () => {
    transport.onFetchval("SELECT FTS_DOC_COUNT", 100);
    const count = await fts.docCount();
    assert.equal(count, 100);
  });

  it("throws on PostgreSQL", async () => {
    const pgFts = withFTS.init(transport, pgFeatures()).fts;
    await assert.rejects(() => pgFts.index(1, "text"), NucleusFeatureError);
  });
});

// ---------------------------------------------------------------------------
// Geo plugin
// ---------------------------------------------------------------------------

describe("withGeo plugin", () => {
  let transport: MockTransport;
  let geo: ReturnType<typeof withGeo.init>["geo"];

  beforeEach(() => {
    transport = new MockTransport();
    geo = withGeo.init(transport, nucleusFeatures()).geo;
  });

  it("has name geo", () => {
    assert.equal(withGeo.name, "geo");
  });

  it("distance sends GEO_DISTANCE", async () => {
    transport.onFetchval("SELECT GEO_DISTANCE", 1234.56);
    const d = await geo.distance({ lat: 40.7, lon: -74.0 }, { lat: 34.0, lon: -118.2 });
    assert.equal(d, 1234.56);
  });

  it("within sends GEO_WITHIN", async () => {
    transport.onFetchval("SELECT GEO_WITHIN", true);
    const result = await geo.within({ lat: 40.7, lon: -74.0 }, { lat: 40.71, lon: -74.01 }, 5000);
    assert.equal(result, true);
  });

  it("area requires at least 3 points", async () => {
    await assert.rejects(() => geo.area([{ lat: 0, lon: 0 }, { lat: 1, lon: 1 }]), Error);
  });

  it("area sends GEO_AREA for 3+ points", async () => {
    transport.onFetchval("SELECT GEO_AREA", 1000);
    const a = await geo.area([{ lat: 0, lon: 0 }, { lat: 1, lon: 0 }, { lat: 1, lon: 1 }]);
    assert.equal(a, 1000);
  });

  it("insert validates layer identifier", async () => {
    await assert.rejects(() => geo.insert("bad name", 0, 0, {}), Error);
  });

  it("throws on PostgreSQL", async () => {
    const pgGeo = withGeo.init(transport, pgFeatures()).geo;
    await assert.rejects(() => pgGeo.distance({ lat: 0, lon: 0 }, { lat: 1, lon: 1 }), NucleusFeatureError);
  });
});

// ---------------------------------------------------------------------------
// Blob plugin
// ---------------------------------------------------------------------------

describe("withBlob plugin", () => {
  let transport: MockTransport;
  let blob: ReturnType<typeof withBlob.init>["blob"];

  beforeEach(() => {
    transport = new MockTransport();
    blob = withBlob.init(transport, nucleusFeatures()).blob;
  });

  it("has name blob", () => {
    assert.equal(withBlob.name, "blob");
  });

  it("put sends BLOB_STORE with hex data", async () => {
    const data = new Uint8Array([0x48, 0x65, 0x6c, 0x6c, 0x6f]); // "Hello"
    await blob.put("mybucket", "file.bin", data);
    const call = transport.calls[0];
    assert.ok((call.args[0] as string).includes("BLOB_STORE"));
    const params = call.args[1] as unknown[];
    assert.equal(params[0], "mybucket/file.bin");
    assert.equal(params[1], "48656c6c6f");
  });

  it("put accepts string data as hex pass-through", async () => {
    await blob.put("bucket", "key", "abcdef");
    const params = transport.calls[0].args[1] as unknown[];
    assert.equal(params[1], "abcdef");
  });

  it("put rejects non-hex string data at write time", async () => {
    // fromHex validates on read; without write-side validation, garbage
    // written now throws confusingly on every later read.
    await assert.rejects(() => blob.put("bucket", "key", "zzzz"), /Invalid hex/);
    assert.equal(transport.calls.length, 0);
  });

  it("put rejects odd-length string data at write time", async () => {
    await assert.rejects(() => blob.put("bucket", "key", "abc"), /Invalid hex/);
  });

  it("put stores metadata tags", async () => {
    await blob.put("bucket", "key", "aa", { metadata: { env: "prod" } });
    // Should have 2 calls: BLOB_STORE + BLOB_TAG
    assert.equal(transport.calls.length, 2);
    assert.ok((transport.calls[1].args[0] as string).includes("BLOB_TAG"));
  });

  it("get returns data and meta", async () => {
    transport.onFetchval("SELECT BLOB_GET", "48656c6c6f");
    transport.onFetchval("SELECT BLOB_META", JSON.stringify({
      size: 5,
      content_type: "application/octet-stream",
      created_at: 1704067200000,
      updated_at: 1704067200000,
    }));
    const result = await blob.get("bucket", "file.bin");
    assert.ok(result);
    assert.deepEqual(result.data, new Uint8Array([0x48, 0x65, 0x6c, 0x6c, 0x6f]));
  });

  it("get returns null for missing blob", async () => {
    const result = await blob.get("bucket", "missing");
    assert.equal(result, null);
  });

  it("delete sends BLOB_DELETE", async () => {
    transport.onFetchval("SELECT BLOB_DELETE", true);
    const result = await blob.delete("bucket", "key");
    assert.equal(result, true);
  });

  it("blobCount sends BLOB_COUNT", async () => {
    transport.onFetchval("SELECT BLOB_COUNT", 42);
    const count = await blob.blobCount();
    assert.equal(count, 42);
  });

  it("throws on PostgreSQL", async () => {
    const pgBlob = withBlob.init(transport, pgFeatures()).blob;
    await assert.rejects(() => pgBlob.blobCount(), NucleusFeatureError);
  });
});

// ---------------------------------------------------------------------------
// TimeSeries plugin
// ---------------------------------------------------------------------------

describe("withTimeSeries plugin", () => {
  let transport: MockTransport;
  let ts: ReturnType<typeof withTimeSeries.init>["timeseries"];

  beforeEach(() => {
    transport = new MockTransport();
    ts = withTimeSeries.init(transport, nucleusFeatures()).timeseries;
  });

  it("has name timeseries", () => {
    assert.equal(withTimeSeries.name, "timeseries");
  });

  it("write sends TS_INSERT for each point", async () => {
    await ts.write("cpu", [
      { timestamp: new Date(1000), value: 50 },
      { timestamp: new Date(2000), value: 60 },
    ]);
    assert.equal(transport.calls.length, 2);
    assert.ok((transport.calls[0].args[0] as string).includes("TS_INSERT"));
  });

  it("last sends TS_LAST", async () => {
    transport.onFetchval("SELECT TS_LAST", 75);
    const val = await ts.last("cpu");
    assert.equal(val, 75);
  });

  it("count sends TS_COUNT", async () => {
    transport.onFetchval("SELECT TS_COUNT", 1000);
    const c = await ts.count("cpu");
    assert.equal(c, 1000);
  });

  it("rangeCount sends TS_RANGE_COUNT", async () => {
    transport.onFetchval("SELECT TS_RANGE_COUNT", 50);
    const c = await ts.rangeCount("cpu", new Date(0), new Date(10000));
    assert.equal(c, 50);
  });

  it("retention sends TS_RETENTION", async () => {
    transport.onFetchval("SELECT TS_RETENTION", "OK");
    const result = await ts.retention(30);
    assert.equal(result, true);
  });

  it("throws on PostgreSQL", async () => {
    const pgTs = withTimeSeries.init(transport, pgFeatures()).timeseries;
    await assert.rejects(() => pgTs.count("cpu"), NucleusFeatureError);
  });
});

// ---------------------------------------------------------------------------
// Streams plugin
// ---------------------------------------------------------------------------

/** Transport whose fetchval/query reject with a driver-style error. */
class RejectingTransport implements Transport {
  constructor(private readonly error: unknown) {}

  async query<T>(): Promise<QueryResult<T>> {
    throw this.error;
  }

  async execute(): Promise<number> {
    throw this.error;
  }

  async fetchval<T>(): Promise<T | null> {
    throw this.error;
  }

  async beginTransaction(): Promise<TransactionTransport> {
    throw this.error;
  }

  async close(): Promise<void> {}

  async ping(): Promise<void> {
    throw this.error;
  }
}

describe("withStreams plugin", () => {
  let transport: MockTransport;
  let streams: ReturnType<typeof withStreams.init>["streams"];

  beforeEach(() => {
    transport = new MockTransport();
    streams = withStreams.init(transport, nucleusFeatures()).streams;
  });

  it("has name streams", () => {
    assert.equal(withStreams.name, "streams");
  });

  it("xadd sends STREAM_XADD with variadic args", async () => {
    transport.onFetchval("SELECT STREAM_XADD", "1000-0");
    const id = await streams.xadd("mystream", { key: "val" });
    assert.equal(id, "1000-0");
    const call = transport.calls[0];
    assert.ok((call.args[0] as string).includes("STREAM_XADD"));
  });

  it("xlen sends STREAM_XLEN", async () => {
    transport.onFetchval("SELECT STREAM_XLEN", 5);
    const len = await streams.xlen("mystream");
    assert.equal(len, 5);
  });

  it("xrange sends STREAM_XRANGE", async () => {
    transport.onFetchval("SELECT STREAM_XRANGE", '[{"id":"1-0","fields":{"a":"1"}}]');
    const entries = await streams.xrange("mystream", 0, 10000, 10);
    assert.equal(entries.length, 1);
    assert.equal(entries[0].id, "1-0");
  });

  it("xgroupCreate sends STREAM_XGROUP_CREATE", async () => {
    transport.onFetchval("SELECT STREAM_XGROUP_CREATE", true);
    const result = await streams.xgroupCreate("mystream", "grp", 0);
    assert.equal(result, true);
  });

  it("xreadGroup returns [] when caught up (payload is \"[]\")", async () => {
    transport.onFetchval("SELECT STREAM_XREADGROUP", "[]");
    const entries = await streams.xreadGroup("mystream", "grp", "c1", 10);
    assert.deepEqual(entries, []);
  });

  it("xreadGroup surfaces NOGROUP for a missing group (pg error, SQLSTATE 22000)", async () => {
    const nogroup = Object.assign(
      new Error("NOGROUP No such consumer group 'grp' for stream 'mystream'"),
      { code: "22000" },
    );
    const failing = new RejectingTransport(nogroup);
    const s = withStreams.init(failing, nucleusFeatures()).streams;
    await assert.rejects(() => s.xreadGroup("mystream", "grp", "c1", 10), (err: unknown) => {
      assert.match((err as Error).message, /NOGROUP/);
      assert.equal((err as { code?: string }).code, "22000");
      return true;
    });
  });

  it("xreadGroup rejects an empty payload instead of reading it as caught-up", async () => {
    // Since Nucleus v0.1.8 a caught-up success carries "[]", never "".
    // An empty payload is a contract violation — swallowing it as [] is how
    // a vanished group silently skips every unprocessed entry.
    transport.onFetchval("SELECT STREAM_XREADGROUP", "");
    await assert.rejects(() => streams.xreadGroup("mystream", "grp", "c1", 10), NucleusQueryError);
  });

  it("xgroupCreate surfaces BUSYGROUP for an existing group (SQLSTATE 23000)", async () => {
    const busy = Object.assign(
      new Error("BUSYGROUP Consumer Group name already exists"),
      { code: "23000" },
    );
    const failing = new RejectingTransport(busy);
    const s = withStreams.init(failing, nucleusFeatures()).streams;
    await assert.rejects(() => s.xgroupCreate("mystream", "grp", 0), (err: unknown) => {
      assert.match((err as Error).message, /BUSYGROUP/);
      assert.equal((err as { code?: string }).code, "23000");
      return true;
    });
  });

  it("xadd surfaces engine failures (rejected WAL write)", async () => {
    const err = Object.assign(
      new Error("STREAM_XADD could not log entry 5-0 for stream 'mystream': No space left on device"),
      { code: "53100" },
    );
    const failing = new RejectingTransport(err);
    const s = withStreams.init(failing, nucleusFeatures()).streams;
    await assert.rejects(() => s.xadd("mystream", { a: "b" }), /STREAM_XADD/);
  });

  it("xrange maps an empty payload to [] (missing stream legitimately answers \"\")", async () => {
    transport.onFetchval("SELECT STREAM_XRANGE", "");
    assert.deepEqual(await streams.xrange("missing", 0, 10000, 10), []);
  });

  it("xread maps an empty payload to [] (missing stream legitimately answers \"\")", async () => {
    transport.onFetchval("SELECT STREAM_XREAD", "");
    assert.deepEqual(await streams.xread("missing", 0, 10), []);
  });

  it("xread passes a full \"<ms>-<seq>\" cursor through — the gapless resume form", async () => {
    transport.onFetchval("SELECT STREAM_XREAD", '[{"id":"1000-1","fields":{"a":"1"}}]');
    const entries = await streams.xread("mystream", "1000-0", 10);
    assert.equal(entries.length, 1);
    assert.equal(entries[0].id, "1000-1");
    assert.deepEqual(transport.calls[0].args[1], ["mystream", "1000-0", 10]);
  });

  it("xrange accepts full id bounds and passes them through", async () => {
    transport.onFetchval("SELECT STREAM_XRANGE", '[{"id":"1000-1","fields":{"a":"1"}}]');
    await streams.xrange("mystream", "1000-0", "1000-5", 10);
    assert.deepEqual(transport.calls[0].args[1], ["mystream", "1000-0", "1000-5", 10]);
  });

  it("xread keeps accepting a bare millisecond cursor (documented gap risk)", async () => {
    transport.onFetchval("SELECT STREAM_XREAD", "[]");
    assert.deepEqual(await streams.xread("mystream", 999, 10), []);
    assert.deepEqual(transport.calls[0].args[1], ["mystream", 999, 10]);
  });

  it("throws on PostgreSQL", async () => {
    const pgStreams = withStreams.init(transport, pgFeatures()).streams;
    await assert.rejects(() => pgStreams.xlen("s"), NucleusFeatureError);
  });
});

// ---------------------------------------------------------------------------
// Columnar plugin
// ---------------------------------------------------------------------------

describe("withColumnar plugin", () => {
  let transport: MockTransport;
  let columnar: ReturnType<typeof withColumnar.init>["columnar"];

  beforeEach(() => {
    transport = new MockTransport();
    columnar = withColumnar.init(transport, nucleusFeatures()).columnar;
  });

  it("has name columnar", () => {
    assert.equal(withColumnar.name, "columnar");
  });

  it("insert sends COLUMNAR_INSERT with variadic col/val args", async () => {
    transport.onFetchval("SELECT COLUMNAR_INSERT", "OK");
    const result = await columnar.insert("events", { type: "click" });
    assert.equal(result, true);
    const params = transport.calls[0].args[1] as unknown[];
    assert.deepEqual(params, ["events", "type", "click"]);
  });

  it("insert rejects an empty values object", async () => {
    await assert.rejects(() => columnar.insert("events", {}), /at least one column/);
  });

  it("count sends COLUMNAR_COUNT", async () => {
    transport.onFetchval("SELECT COLUMNAR_COUNT", 1000);
    const c = await columnar.count("events");
    assert.equal(c, 1000);
  });

  it("sum sends COLUMNAR_SUM", async () => {
    transport.onFetchval("SELECT COLUMNAR_SUM", 5000);
    const s = await columnar.sum("events", "amount");
    assert.equal(s, 5000);
  });

  it("avg sends COLUMNAR_AVG", async () => {
    transport.onFetchval("SELECT COLUMNAR_AVG", 42.5);
    const a = await columnar.avg("events", "score");
    assert.equal(a, 42.5);
  });

  it("validates table name identifier", async () => {
    await assert.rejects(() => columnar.count("bad table"), Error);
  });

  it("throws on PostgreSQL", async () => {
    const pgCol = withColumnar.init(transport, pgFeatures()).columnar;
    await assert.rejects(() => pgCol.count("t"), NucleusFeatureError);
  });
});

// ---------------------------------------------------------------------------
// Datalog plugin
// ---------------------------------------------------------------------------

describe("withDatalog plugin", () => {
  let transport: MockTransport;
  let datalog: ReturnType<typeof withDatalog.init>["datalog"];

  beforeEach(() => {
    transport = new MockTransport();
    datalog = withDatalog.init(transport, nucleusFeatures()).datalog;
  });

  it("has name datalog", () => {
    assert.equal(withDatalog.name, "datalog");
  });

  it("assert sends DATALOG_ASSERT and returns the engine status string", async () => {
    transport.onFetchval("SELECT DATALOG_ASSERT", "ASSERT parent/2");
    const result = await datalog.assert("parent(alice, bob)");
    assert.equal(result, "ASSERT parent/2");
  });

  it("retract sends DATALOG_RETRACT and returns the engine status string", async () => {
    transport.onFetchval("SELECT DATALOG_RETRACT", "RETRACT parent/2");
    const result = await datalog.retract("parent(alice, bob)");
    assert.equal(result, "RETRACT parent/2");
  });

  it("rule sends DATALOG_RULE as ONE combined parameter (verified engine surface)", async () => {
    // The engine takes a single 'head :- body' string; the old two-parameter
    // form was parsed as a bare fact and rejected — it never worked.
    transport.onFetchval("SELECT DATALOG_RULE", "RULE ancestor/2");
    const result = await datalog.rule("ancestor(X, Y)", "parent(X, Y)");
    assert.equal(result, "RULE ancestor/2");
    assert.deepEqual(transport.calls[0].args, ["SELECT DATALOG_RULE($1)", ["ancestor(X, Y) :- parent(X, Y)"]]);
  });

  it("query sends DATALOG_QUERY", async () => {
    transport.onFetchval("SELECT DATALOG_QUERY", '[["alice"], ["bob"]]');
    const result = await datalog.query("parent(X, bob)");
    assert.equal(result, '[["alice"], ["bob"]]');
  });

  it("clear sends DATALOG_CLEAR and returns the engine status string", async () => {
    transport.onFetchval("SELECT DATALOG_CLEAR", "CLEAR parent");
    const result = await datalog.clear("parent");
    assert.equal(result, "CLEAR parent");
  });

  it("importGraph sends DATALOG_IMPORT_GRAPH and returns the engine status string", async () => {
    transport.onFetchval("SELECT DATALOG_IMPORT_GRAPH", "IMPORTED 50 edges into knows");
    const result = await datalog.importGraph("knows");
    assert.equal(result, "IMPORTED 50 edges into knows");
  });

  it("throws on PostgreSQL", async () => {
    const pgDl = withDatalog.init(transport, pgFeatures()).datalog;
    await assert.rejects(() => pgDl.assert("fact"), NucleusFeatureError);
  });
});

// ---------------------------------------------------------------------------
// CDC plugin
// ---------------------------------------------------------------------------

describe("withCDC plugin", () => {
  let transport: MockTransport;
  let cdc: ReturnType<typeof withCDC.init>["cdc"];

  beforeEach(() => {
    transport = new MockTransport();
    cdc = withCDC.init(transport, nucleusFeatures()).cdc;
  });

  it("has name cdc", () => {
    assert.equal(withCDC.name, "cdc");
  });

  it("read sends CDC_READ with after_sequence and limit", async () => {
    transport.onFetchval("SELECT CDC_READ", '[{"seq":1,"table":"users","change":"INSERT","ts":1704067200000}]');
    const events = await cdc.read(0);
    assert.equal(events.length, 1);
    assert.equal(events[0].seq, 1);
    assert.equal(events[0].change, "INSERT");
    const params = transport.calls[0].args[1] as unknown[];
    assert.deepEqual(params, [0, 100]);
  });

  it("count sends CDC_COUNT", async () => {
    transport.onFetchval("SELECT CDC_COUNT", 100);
    const c = await cdc.count();
    assert.equal(c, 100);
  });

  it("tableRead sends CDC_TABLE_READ", async () => {
    transport.onFetchval("SELECT CDC_TABLE_READ", "[]");
    const events = await cdc.tableRead("users", 0);
    assert.deepEqual(events, []);
    const params = transport.calls[0].args[1] as unknown[];
    assert.deepEqual(params, ["users", 0, 100]);
  });

  it("throws on PostgreSQL", async () => {
    const pgCdc = withCDC.init(transport, pgFeatures()).cdc;
    await assert.rejects(() => pgCdc.count(), NucleusFeatureError);
  });
});

// ---------------------------------------------------------------------------
// PubSub plugin
// ---------------------------------------------------------------------------

describe("withPubSub plugin", () => {
  let transport: MockTransport;
  let pubsub: ReturnType<typeof withPubSub.init>["pubsub"];

  beforeEach(() => {
    transport = new MockTransport();
    pubsub = withPubSub.init(transport, nucleusFeatures()).pubsub;
  });

  it("has name pubsub", () => {
    assert.equal(withPubSub.name, "pubsub");
  });

  it("publish sends PUBSUB_PUBLISH", async () => {
    transport.onFetchval("SELECT PUBSUB_PUBLISH", 3);
    const count = await pubsub.publish("chat", "hello");
    assert.equal(count, 3);
  });

  it("channels sends PUBSUB_CHANNELS() — the engine takes no pattern argument", async () => {
    // X05: the engine's PUBSUB_CHANNELS arm reads no arguments; the old
    // `channels(pattern)` overload sent PUBSUB_CHANNELS($1), which the engine
    // does not filter with — the pattern was silently ignored and every
    // channel came back. The parameter is removed; filtering client-side off
    // the comma-separated string would fabricate a server feature.
    transport.onFetchval("SELECT PUBSUB_CHANNELS()", "chat,events");
    const result = await pubsub.channels();
    assert.equal(result, "chat,events");
  });

  it("subscribers sends PUBSUB_SUBSCRIBERS", async () => {
    transport.onFetchval("SELECT PUBSUB_SUBSCRIBERS", 5);
    const count = await pubsub.subscribers("chat");
    assert.equal(count, 5);
  });

  it("throws on PostgreSQL", async () => {
    const pgPub = withPubSub.init(transport, pgFeatures()).pubsub;
    await assert.rejects(() => pgPub.publish("ch", "msg"), NucleusFeatureError);
  });
});

// ---------------------------------------------------------------------------
// Migration system
// ---------------------------------------------------------------------------

// v2Row shapes a history row the runner trusts: format v2 plus the canonical
// checksum of the up SQL (adopted-unverified rows carry checksum null).
function v2Row(m: Migration): { version: number; checksum: string; format: string } {
  return { version: m.version, checksum: migrationChecksum(m.up), format: "v2" };
}

describe("migrate", () => {
  let transport: MockTransport;

  const migrations: Migration[] = [
    { version: 1, name: "create_users", up: "CREATE TABLE users (id INT)", down: "DROP TABLE users" },
    { version: 2, name: "add_email", up: "ALTER TABLE users ADD COLUMN email TEXT", down: "ALTER TABLE users DROP COLUMN email" },
    { version: 3, name: "create_posts", up: "CREATE TABLE posts (id INT)", down: "DROP TABLE posts" },
  ];

  beforeEach(() => {
    transport = new MockTransport();
    // The ledger claim INSERT must succeed on the mock (1 = claimed).
    transport.executeResult = 1;
    // Make ensureTable + appliedRows work: SELECT version returns empty.
    transport.onQuery("SELECT version", []);
  });

  it("runs all pending migrations", async () => {
    const ran = await migrate(transport, migrations);
    assert.deepEqual(ran, ["create_users", "add_email", "create_posts"]);
  });

  it("skips already applied migrations", async () => {
    transport.onQuery("SELECT version", [v2Row(migrations[0])]);
    const ran = await migrate(transport, migrations);
    assert.deepEqual(ran, ["add_email", "create_posts"]);
  });

  it("returns empty array when all are applied", async () => {
    transport.onQuery("SELECT version", migrations.map(v2Row));
    const ran = await migrate(transport, migrations);
    assert.deepEqual(ran, []);
  });

  it("runs migrations in ascending version order", async () => {
    const reversed = [...migrations].reverse();
    const ran = await migrate(transport, reversed);
    assert.deepEqual(ran, ["create_users", "add_email", "create_posts"]);
  });

  it("records checksum, owner and format with each migration", async () => {
    await migrate(transport, [migrations[0]]);
    const insert = transport.calls.find(
      (c) => c.method === "execute" && String(c.args[0]).includes("INSERT INTO _neutron_migrations"),
    );
    assert.ok(insert, "history INSERT not issued");
    const params = insert!.args[1] as unknown[];
    assert.equal(params[2], migrationChecksum(migrations[0].up));
    assert.equal(params[4], "v2");
    assert.ok(String(params[3]).startsWith("nucleus-ts-sdk@"));
  });

  it("refuses a modified applied migration before any new mutation", async () => {
    transport.onQuery("SELECT version", [v2Row(migrations[0])]);
    const tampered: Migration[] = [
      { ...migrations[0], up: "CREATE TABLE users (id BIGINT)" },
      migrations[1],
    ];
    await assert.rejects(
      () => migrate(transport, tampered),
      (err: Error) => err.message.includes("modified since it was applied"),
    );
    // Refusal precedes mutations: no history INSERT was issued.
    assert.ok(!transport.calls.some(
      (c) => c.method === "execute" && String(c.args[0]).includes("INSERT INTO _neutron_migrations"),
    ));
  });

  it("refuses legacy-format history rows until adoption", async () => {
    transport.onQuery("SELECT version", [
      { version: 1, checksum: "deadbeef", format: null },
    ]);
    await assert.rejects(
      () => migrate(transport, migrations),
      (err: Error) => err.message.includes("adopt"),
    );
    assert.ok(!transport.calls.some(
      (c) => c.method === "execute" && String(c.args[0]).includes("INSERT INTO _neutron_migrations"),
    ));
  });

  it(" exempts adopted-unverified rows (null checksum, v2 format)", async () => {
    transport.onQuery("SELECT version", [
      { version: 1, checksum: null, format: "v2" },
    ]);
    const ran = await migrate(transport, migrations);
    assert.deepEqual(ran, ["add_email", "create_posts"]);
  });

  it("aborts the lock wait on signal and never steals the claim", async () => {
    const transportHeld = new MockTransport();
    transportHeld.executeResult = 0; // claim INSERT conflicts: held elsewhere
    const controller = new AbortController();
    setTimeout(() => controller.abort(), 60);
    await assert.rejects(
      () => migrate(transportHeld, migrations, { signal: controller.signal }),
      () => true,
    );
    // No time-based takeover path exists: no UPDATE ever touches the lock
    // table (a steal would be one).
    const lockWrites = transportHeld.calls.filter(
      (c) => c.method === "execute" && String(c.args[0]).includes("_neutron_migration_lock"),
    );
    assert.ok(lockWrites.length > 0, "claim was never attempted");
    for (const c of lockWrites) {
      const sql = String(c.args[0]);
      assert.ok(
        !sql.trimStart().toUpperCase().startsWith("UPDATE"),
        `unexpected lock-table mutation (steal path?): ${sql}`,
      );
    }
  });
});

describe("migrateDown", () => {
  let transport: MockTransport;

  const migrations: Migration[] = [
    { version: 1, name: "create_users", up: "CREATE TABLE users (id INT)", down: "DROP TABLE users" },
    { version: 2, name: "add_email", up: "ALTER TABLE users ADD COLUMN email TEXT", down: "ALTER TABLE users DROP COLUMN email" },
  ];

  beforeEach(() => {
    transport = new MockTransport();
    transport.executeResult = 1;
  });

  it("rolls back the most recent migration", async () => {
    transport.onQuery("SELECT version", migrations.map(v2Row));
    const rolled = await migrateDown(transport, migrations, 1);
    assert.deepEqual(rolled, ["add_email"]);
  });

  it("rolls back multiple steps", async () => {
    transport.onQuery("SELECT version", migrations.map(v2Row));
    const rolled = await migrateDown(transport, migrations, 2);
    assert.deepEqual(rolled, ["add_email", "create_users"]);
  });

  it("throws when migration has no down SQL", async () => {
    const noDown: Migration[] = [{ version: 1, name: "irreversible", up: "DO SOMETHING" }];
    transport.onQuery("SELECT version", [v2Row(noDown[0])]);
    await assert.rejects(() => migrateDown(transport, noDown, 1), Error);
  });
});

describe("adoptMigrations", () => {
  it("verifies rows whose legacy Go SDK digest reproduces from the plan", async () => {
    const transport = new MockTransport();
    transport.executeResult = 1;
    const m: Migration = { version: 1, name: "first", up: "CREATE TABLE a (id INT)" };
    transport.onFetchval("SELECT EXISTS", 1);
    transport.onQuery("SELECT version, name", [
      { version: 1, name: "first", checksum: legacyGoSdkChecksum(1, "first", m.up) },
    ]);
    const report = await adoptMigrations(transport, [m]);
    assert.deepEqual(report.verified, [1]);
    assert.deepEqual(report.unverified, []);
    const update = transport.calls.find(
      (c) => c.method === "execute" && String(c.args[0]).startsWith("UPDATE _neutron_migrations"),
    );
    assert.ok(update, "adoption UPDATE not issued");
    assert.equal((update!.args[1] as unknown[])[0], migrationChecksum(m.up));
  });

  it("adopts unverifiable rows with NULL checksum, never baselined", async () => {
    const transport = new MockTransport();
    transport.executeResult = 1;
    const m: Migration = { version: 1, name: "first", up: "CREATE TABLE a (id INT)" };
    transport.onFetchval("SELECT EXISTS", 1);
    transport.onQuery("SELECT version, name", [{ version: 1, name: "first", checksum: null }]);
    const report = await adoptMigrations(transport, [m]);
    assert.deepEqual(report.verified, []);
    assert.deepEqual(report.unverified, [1]);
    const update = transport.calls.find(
      (c) => c.method === "execute" && String(c.args[0]).startsWith("UPDATE _neutron_migrations"),
    );
    assert.ok(update, "adoption UPDATE not issued");
    // The checksum is set to a literal NULL in SQL (never a computed
    // baseline); the only parameters are owner/format/version.
    assert.ok(String((update!.args[1] as unknown[])[0]).startsWith("nucleus-ts-sdk@"));
  });

  it("aborts when a recorded checksum matches neither digest", async () => {
    const transport = new MockTransport();
    transport.executeResult = 1;
    const m: Migration = { version: 1, name: "first", up: "CREATE TABLE a (id INT)" };
    transport.onFetchval("SELECT EXISTS", 1);
    transport.onQuery("SELECT version, name", [{ version: 1, name: "first", checksum: "deadbeef" }]);
    await assert.rejects(
      () => adoptMigrations(transport, [m]),
      (err: Error) => err.message.includes("neither"),
    );
  });
});

describe("migration checksums", () => {
  // Golden vectors pin the canonical algorithm across the CLI and both SDKs
  // (contracts/data/MIGRATIONS.md §3).
  it("matches the cross-language golden vectors", () => {
    assert.equal(
      migrationChecksum("CREATE TABLE x (id INT)\n"),
      "c4b873a900b90da54e1de8efb0da3f7294599c0e96b5c50f2ff411bd7274a65a",
    );
    assert.equal(
      migrationChecksum("CREATE TABLE users (id serial PRIMARY KEY);\nALTER TABLE users ADD COLUMN email TEXT;\n"),
      "5df840dd9f1517a75a84c53d78c6caf338ecff05e211ea8a08df48f21b983f9c",
    );
    assert.equal(
      legacyGoSdkChecksum(1, "first", "CREATE TABLE legacy_a (id INT)"),
      "208474566c268521846034e32490fac1e893a2d6390018f75bb915b3722d995f",
    );
  });
});

describe("migrationLockInfo / forceUnlockMigrations", () => {
  it("reports an unheld lock", async () => {
    const transport = new MockTransport();
    transport.onQuery("SELECT owner", []);
    const info = await migrationLockInfo(transport);
    assert.equal(info.held, false);
    assert.equal(info.owner, null);
  });

  it("reports holder diagnostics", async () => {
    const transport = new MockTransport();
    transport.onQuery("SELECT owner", [{ owner: "someone", heartbeat: "2026-09-22 00:00:00+00" }]);
    const info = await migrationLockInfo(transport);
    assert.equal(info.held, true);
    assert.equal(info.owner, "someone");
    assert.equal(info.heartbeat, "2026-09-22 00:00:00+00");
  });

  it("force-unlock deletes the claim row", async () => {
    const transport = new MockTransport();
    transport.executeResult = 1;
    await forceUnlockMigrations(transport);
    const del = transport.calls.find(
      (c) => c.method === "execute" && String(c.args[0]).includes("DELETE FROM _neutron_migration_lock"),
    );
    assert.ok(del, "claim row not deleted");
  });
});

describe("migrationStatus", () => {
  it("returns applied migrations", async () => {
    const transport = new MockTransport();
    transport.onQuery("SELECT version, name", [
      { version: 1, name: "init", applied_at: "2024-01-01T00:00:00Z" },
    ]);
    const status = await migrationStatus(transport);
    assert.equal(status.length, 1);
    assert.equal(status[0].version, 1);
    assert.equal(status[0].name, "init");
    assert.ok(status[0].appliedAt instanceof Date);
  });

  it("returns empty array when no migrations applied", async () => {
    const transport = new MockTransport();
    const status = await migrationStatus(transport);
    assert.deepEqual(status, []);
  });
});

// ---------------------------------------------------------------------------
// Plugin metadata
// ---------------------------------------------------------------------------

describe("Plugin names", () => {
  it("all 14 plugins have unique names", () => {
    const plugins = [
      withSQL, withKV, withVector, withDocument, withGraph,
      withFTS, withGeo, withBlob, withTimeSeries, withStreams,
      withColumnar, withDatalog, withCDC, withPubSub,
    ];
    const names = plugins.map((p) => p.name);
    assert.equal(new Set(names).size, 14);
  });

  it("all plugins have init function", () => {
    const plugins = [
      withSQL, withKV, withVector, withDocument, withGraph,
      withFTS, withGeo, withBlob, withTimeSeries, withStreams,
      withColumnar, withDatalog, withCDC, withPubSub,
    ];
    for (const p of plugins) {
      assert.equal(typeof p.init, "function");
    }
  });
});

// =========================================================================
// X02 — documents and graph relationships: defect fixes, schema-aware
// collections, bounded traversal, SQL identity tying. Unit level with a
// param-aware counting transport (statement-counter proofs are first-class).
// =========================================================================

import { DocumentValidationError } from "./document/index.js";
import { validateDocument, type DocumentSchema } from "./document/schema.js";
import { NucleusNotSupportedError } from "./errors.js";
import { CAPABILITY_PROBE_NAME, NucleusCapabilityError } from "./capabilities.js";
import {
  SQL_ID_ROW_KEY,
  SQL_ID_SCHEMA_KEY,
  SQL_ID_TABLE_KEY,
  MAX_TRAVERSAL_DEPTH,
} from "./graph/traverse.js";

/**
 * Param-aware transport: responses keyed by (sql, params[0]); counts every
 * statement. The read-only capability probes (capabilities.ts) are answered
 * like a Nucleus 1.0.2 engine answers them and recorded in `probeCalls`, NOT
 * in `calls` — so `statementCount` counts the operation's own statements;
 * probe behaviour has its own tests below. `probeMode` switches the probe
 * answers to a missing surface (server error 42883) or a wrong value.
 */
class X02Transport implements Transport {
  readonly calls: Array<{ sql: string; params: unknown[] }> = [];
  readonly probeCalls: Array<{ sql: string; params: unknown[] }> = [];
  probeMode: "supported" | "missing" | "wrong" | "network" = "supported";
  private fetchvalHandlers: Array<(sql: string, params: unknown[]) => unknown> = [];
  private queryHandler: ((sql: string, params: unknown[]) => unknown[]) | null = null;

  onFetchval(handler: (sql: string, params: unknown[]) => unknown): void {
    this.fetchvalHandlers.push(handler);
  }

  onQuery(handler: (sql: string, params: unknown[]) => unknown[]): void {
    this.queryHandler = handler;
  }

  get statementCount(): number {
    return this.calls.length;
  }

  async query<T = Record<string, unknown>>(sql: string, params: unknown[] = []): Promise<QueryResult<T>> {
    this.calls.push({ sql, params });
    const rows = this.queryHandler ? (this.queryHandler(sql, params) as T[]) : [];
    return { rows, rowCount: rows.length };
  }

  async execute(sql: string, params: unknown[] = []): Promise<number> {
    this.calls.push({ sql, params });
    return 1;
  }

  private isProbe(sql: string, params: unknown[]): boolean {
    if (params.some((p) => String(p).includes(CAPABILITY_PROBE_NAME))) return true;
    if (sql === "SELECT GRAPH_NODE($1)" && params[0] === "0") return true;
    if (sql === "SELECT GRAPH_NEIGHBORS($1, $2)" && params[0] === 0) return true;
    return false;
  }

  private probeAnswer(sql: string): unknown {
    if (this.probeMode === "network") throw new Error("socket hang up");
    if (this.probeMode === "missing") {
      throw Object.assign(new Error(`unknown function: ${sql.slice(7, sql.indexOf("("))}`), { code: "42883" });
    }
    const wrong = this.probeMode === "wrong";
    if (sql.startsWith("SELECT DOC_COUNT")) return wrong ? 3 : 0;
    if (sql.startsWith("SELECT GRAPH_NEIGHBORS")) return wrong ? nb([[1, 1, "X"]]) : "[]";
    if (sql.startsWith("SELECT GRAPH_QUERY")) return wrong ? "not json" : '{"columns":["n","n.sqlref_schema"],"rows":[]}';
    return null;
  }

  async fetchval<T = unknown>(sql: string, params: unknown[] = []): Promise<T | null> {
    if (this.isProbe(sql, params)) {
      this.probeCalls.push({ sql, params });
      return this.probeAnswer(sql) as T | null;
    }
    this.calls.push({ sql, params });
    for (const h of this.fetchvalHandlers) {
      const r = h(sql, params);
      if (r !== undefined) return r as T;
    }
    return null;
  }

  async beginTransaction(_isolation?: IsolationLevel): Promise<TransactionTransport> {
    throw new Error("not needed");
  }
  async close(): Promise<void> {}
  async ping(): Promise<void> {}
}

const NEIGHBOR_SQL = "SELECT GRAPH_NEIGHBORS($1, $2)";
const nb = (entries: Array<[number, number, string]>) =>
  JSON.stringify(entries.map(([neighbor_id, edge_id, edge_type]) => ({ neighbor_id, edge_id, edge_type })));

describe("X02 graph defect fixes", () => {
  let transport: X02Transport;
  let graph: ReturnType<typeof withGraph.init>["graph"];

  beforeEach(() => {
    transport = new X02Transport();
    graph = withGraph.init(transport, nucleusFeatures()).graph;
  });

  it("D3: addNode with multiple labels fails closed with zero statements (was: silent 'A:B' munge)", async () => {
    await assert.rejects(
      () => graph.addNode(["Person", "Admin"]),
      /exactly one label[\s\S]*got 2/,
    );
    assert.equal(transport.statementCount, 0);
  });

  it("D3: addNode sends ONE unwrapped label", async () => {
    transport.onFetchval((sql) => (sql.startsWith("SELECT GRAPH_ADD_NODE") ? 7 : undefined));
    const id = await graph.addNode(["Person"]);
    assert.equal(id, 7);
    assert.deepEqual(transport.calls[0].params, ["Person"]);
  });

  it("D2: addEdge refused for a missing endpoint names it (was: fake id 0)", async () => {
    transport.onFetchval((sql, params) => {
      if (sql.startsWith("SELECT GRAPH_ADD_EDGE")) return null; // engine refuses
      if (sql.startsWith("SELECT GRAPH_NODE")) {
        return Number(params[0]) === 1 ? '{"id":1}' : null; // node 1 exists, 99 missing
      }
      return undefined;
    });
    await assert.rejects(
      () => graph.addEdge(1, 99, "KNOWS"),
      (err: unknown) =>
        err instanceof NucleusNotFoundError && /to node 99 does not exist/.test(err.message),
    );
  });

  it("D4: query with params fails closed with zero statements (was: params silently dropped)", async () => {
    await assert.rejects(
      () => graph.query("MATCH (n) WHERE n.i = $target RETURN n", { target: 1 }),
      (err: unknown) => err instanceof NucleusNotSupportedError && /no parameter substitution/.test(err.message),
    );
    assert.equal(transport.statementCount, 0);
  });

  it("D1: shortestPath with maxDepth never asks the unbounded engine scalar", async () => {
    // Chain 1->2->3->4 via neighbors; the old code sent GRAPH_SHORTEST_PATH($1,$2,$3)
    // and the engine ignored the bound (live-reproduced).
    transport.onFetchval((sql, params) => {
      if (sql.startsWith("SELECT GRAPH_NEIGHBORS")) {
        const node = Number(params[0]);
        return node < 4 ? nb([[node + 1, node, "NEXT"]]) : nb([]);
      }
      return undefined;
    });
    const withinBound = await graph.shortestPath(1, 3, 2);
    assert.deepEqual(withinBound, [1, 2, 3]);
    const beyondBound = await graph.shortestPath(1, 4, 2);
    assert.deepEqual(beyondBound, []);
    assert.ok(transport.calls.every((c) => !c.sql.includes("GRAPH_SHORTEST_PATH")));
  });

  it("D1: shortestPath without maxDepth still delegates to the engine scalar", async () => {
    transport.onFetchval((sql) => (sql.startsWith("SELECT GRAPH_SHORTEST_PATH") ? "[1,2,3]" : undefined));
    const path = await graph.shortestPath(1, 3);
    assert.deepEqual(path, [1, 2, 3]);
    assert.ok(transport.calls[0].sql.startsWith("SELECT GRAPH_SHORTEST_PATH($1, $2)"));
  });
});


describe("X02 bounded traversal", () => {
  let transport: X02Transport;
  let graph: ReturnType<typeof withGraph.init>["graph"];

  beforeEach(() => {
    transport = new X02Transport();
    graph = withGraph.init(transport, nucleusFeatures()).graph;
  });

  function chainAndFan(): void {
    // 1 -> 2 -> {3, 4}; 3 -> 5; 5 -> 1 (cycle back); 4 -> 4 (self loop)
    transport.onFetchval((sql, params) => {
      if (sql.startsWith("SELECT GRAPH_NODE")) {
        return Number(params[0]) <= 5 ? '{"id":1}' : null;
      }
      if (sql.startsWith(NEIGHBOR_SQL)) {
        const node = Number(params[0]);
        const map: Record<number, string> = {
          1: nb([[2, 10, "NEXT"]]),
          2: nb([[3, 11, "NEXT"], [4, 12, "SKIP"]]),
          3: nb([[5, 13, "NEXT"]]),
          4: nb([[4, 14, "LOOP"]]),
          5: nb([[1, 15, "NEXT"]]),
        };
        return map[node] ?? nb([]);
      }
      return undefined;
    });
  }

  it("maxDepth is required and bounded — invalid bounds throw before any statement", async () => {
    for (const bad of [undefined, 0, -1, 1.5, MAX_TRAVERSAL_DEPTH + 1] as unknown[]) {
      await assert.rejects(() => graph.traverse(1, { maxDepth: bad as number }), /maxDepth/);
    }
    assert.equal(transport.statementCount, 0);
  });

  it("BFS respects depth, reports via-edge metadata and orders by (depth, id)", async () => {
    chainAndFan();
    const result = await graph.traverse(1, { maxDepth: 2 });
    assert.equal(result.startPresent, true);
    assert.equal(result.truncated, false);
    assert.deepEqual(
      result.nodes.map((n) => [n.id, n.depth, n.viaEdgeType]),
      [
        [2, 1, "NEXT"],
        [3, 2, "NEXT"],
        [4, 2, "SKIP"],
      ],
    );
  });

  it("cycles are safe: the back-edge 5->1 and the self-loop 4->4 never re-expand a node", async () => {
    chainAndFan();
    const result = await graph.traverse(1, { maxDepth: 32 });
    // Every node expanded at most once -> statements = 1 (GRAPH_NODE) + 5 expansions.
    assert.equal(result.statements, 6);
    const ids = result.nodes.map((n) => n.id).sort((a, b) => a - b);
    assert.deepEqual(ids, [2, 3, 4, 5]);
  });

  it("edgeTypes filter restricts traversal client-side", async () => {
    chainAndFan();
    const result = await graph.traverse(1, { maxDepth: 2, edgeTypes: ["NEXT"] });
    assert.deepEqual(
      result.nodes.map((n) => n.id),
      [2, 3],
    );
  });

  it("the visit budget truncates honestly", async () => {
    chainAndFan();
    const result = await graph.traverse(1, { maxDepth: 2, maxNodes: 2 });
    assert.equal(result.truncated, true);
    assert.equal(result.nodes.length, 2);
  });

  it("a missing start node reports startPresent=false and traverses nothing", async () => {
    chainAndFan();
    const result = await graph.traverse(999, { maxDepth: 3 });
    assert.equal(result.startPresent, false);
    assert.deepEqual(result.nodes, []);
    assert.equal(result.statements, 1);
  });

  it("direction is passed through to every hop", async () => {
    const seenDirections: unknown[] = [];
    transport.onFetchval((sql, params) => {
      if (sql.startsWith(NEIGHBOR_SQL)) {
        seenDirections.push(params[1]);
        return nb([]);
      }
      if (sql.startsWith("SELECT GRAPH_NODE")) return '{"id":1}';
      return undefined;
    });
    await graph.traverse(1, { maxDepth: 1, direction: "in" });
    assert.deepEqual(seenDirections, ["in"]);
  });
});

describe("X02 sqlNodes — SQL identity tying", () => {
  let transport: X02Transport;
  let graph: ReturnType<typeof withGraph.init>["graph"];

  beforeEach(() => {
    transport = new X02Transport();
    graph = withGraph.init(transport, nucleusFeatures()).graph;
  });

  it("binding emits zero statements and validates the reference", () => {
    const bound = graph.sqlNodes({ table: "users" });
    assert.equal(transport.statementCount, 0);
    assert.deepEqual(bound.boundTo, { table: "users" });
    assert.equal(bound.idColumn, "id");
    assert.throws(() => graph.sqlNodes({ table: 'bad "table"' }), /Invalid SQL table reference/);
    assert.throws(() => graph.sqlNodes({ table: "users" }, { idColumn: "x; drop" }), /plain identifier/);
  });

  it("addRowNode stamps the row identity into node properties", async () => {
    transport.onFetchval((sql) => (sql.startsWith("SELECT GRAPH_ADD_NODE") ? 42 : undefined));
    const bound = graph.sqlNodes({ schema: "app", table: "users" });
    const id = await bound.addRowNode(5, "User", { name: "Ada" });
    assert.equal(id, 42);
    // One lookup (no node yet for row 5), then the insert.
    assert.deepEqual(transport.calls.map((c) => c.sql), ["SELECT GRAPH_QUERY($1)", "SELECT GRAPH_ADD_NODE($1, $2)"]);
    const props = JSON.parse(transport.calls[1].params[1] as string);
    assert.equal(props.name, "Ada");
    assert.equal(props[SQL_ID_SCHEMA_KEY], "app");
    assert.equal(props[SQL_ID_TABLE_KEY], "users");
    assert.equal(props[SQL_ID_ROW_KEY], 5);
  });

  it("findNodeByRow queries by stamped properties and returns the node id", async () => {
    transport.onFetchval((sql) => {
      if (sql.startsWith("SELECT GRAPH_QUERY")) return '{"columns":["n"],"rows":[[7]]}';
      return undefined;
    });
    const bound = graph.sqlNodes({ table: "users" });
    assert.equal(await bound.findNodeByRow(5), 7);
    const cypher = transport.calls[0].params[0] as string;
    assert.match(cypher, /n\.sqlref_table = 'users'/);
    assert.match(cypher, /n\.sqlref_row = 5\b/);
    assert.doesNotMatch(cypher, /"--"/); // identifier-validated: no quote break-out possible
  });

  it("hydrate reports rows, deleted rows and unstamped nodes honestly", async () => {
    transport.onFetchval((sql, params) => {
      if (sql.startsWith("SELECT GRAPH_NODE")) {
        const node = Number(params[0]);
        if (node === 1)
          return JSON.stringify({ id: 1, labels: ["User"], properties: { [SQL_ID_TABLE_KEY]: "users", [SQL_ID_ROW_KEY]: 10 } });
        if (node === 2)
          return JSON.stringify({ id: 2, labels: ["User"], properties: { [SQL_ID_TABLE_KEY]: "users", [SQL_ID_ROW_KEY]: 11 } });
        if (node === 3) return JSON.stringify({ id: 3, labels: ["Tag"], properties: {} });
        return null;
      }
      return undefined;
    });
    transport.onQuery((sql, params) => {
      assert.match(sql, /^SELECT \* FROM "users" WHERE "id" IN \(\$1(, \$2)*\)$/);
      // Row 10 exists; row 11 was deleted.
      return params.map((p) => (p === 10 ? { id: 10, name: "Ada" } : null)).filter((r) => r !== null) as unknown[];
    });
    const bound = graph.sqlNodes({ table: "users" });
    const hydrated = await bound.hydrate([1, 2, 3, 4]);
    assert.equal(hydrated[0].row && (hydrated[0].row as Record<string, unknown>).name, "Ada");
    assert.deepEqual(hydrated[0].sqlRef, { schema: undefined, table: "users", id: 10, idColumn: "id" });
    assert.equal(hydrated[1].row, null, "deleted SQL row reads as null, not stale data");
    assert.equal(hydrated[1].sqlRef?.id, 11);
    assert.deepEqual(hydrated[2], { nodeId: 3, sqlRef: null, row: null }, "unstamped node has no SQL identity");
    assert.deepEqual(hydrated[3], { nodeId: 4, sqlRef: null, row: null }, "missing graph node hydrates to nothing");
  });

  it("addRowEdge names the rows that have no node", async () => {
    const bound = graph.sqlNodes({ table: "users" });
    transport.onFetchval((sql, params) => {
      if (sql.startsWith("SELECT GRAPH_QUERY")) {
        return String(params[0]).includes("77") ? '{"columns":["n"],"rows":[]}' : '{"columns":["n"],"rows":[[3]]}';
      }
      if (sql.startsWith("SELECT GRAPH_ADD_EDGE")) return 50;
      return undefined;
    });
    await assert.rejects(
      () => bound.addRowEdge(9, 77, "FOLLOWS"),
      (err: unknown) => err instanceof NucleusNotFoundError && /row 77 of "users" has no node/.test(String(err.message)),
    );
    const edgeId = await bound.addRowEdge(9, 10, "FOLLOWS");
    assert.equal(edgeId, 50);
  });
});

describe("X02 document collections — schema-aware validation", () => {
  let transport: X02Transport;
  let doc: ReturnType<typeof withDocument.init>["document"];

  const userSchema: DocumentSchema = {
    fields: {
      id: { type: "integer" },
      login: { type: "string" },
      email: { type: "string", optional: true, nullable: true },
      tags: { type: "array", optional: true, items: { type: "string" } },
      profile: { type: "object", optional: true, fields: { bio: { type: "string", optional: true } }, additionalProperties: false },
    },
  };

  beforeEach(() => {
    transport = new X02Transport();
    doc = withDocument.init(transport, nucleusFeatures()).document;
  });

  it("collection() validates the name and records identity metadata with zero statements", () => {
    const c = doc.collection("tenants_a", { schema: userSchema, boundTo: { table: "users" } });
    assert.equal(transport.statementCount, 0);
    assert.equal(c.name, "tenants_a");
    assert.deepEqual(c.boundTo, { table: "users" });
    assert.throws(() => doc.collection("bad name!"), /Invalid document collection name/);
    assert.throws(() => doc.collection(""), /non-empty name/);
  });

  it("an invalid document is rejected BEFORE any statement (counter 0) with precise paths", async () => {
    const c = doc.collection("tenants_a", { schema: userSchema });
    await assert.rejects(
      () => c.insert({ id: "not-an-int", login: 5, tags: ["ok", 7], profile: { bio: "x", extra: 1 } }),
      (err: unknown) => {
        assert.ok(err instanceof DocumentValidationError);
        const paths = err.issues.map((i) => i.path);
        assert.deepEqual(paths, ["$.id", "$.login", "$.tags[1]", "$.profile.extra"]);
        assert.match(err.issues[0].expected, /integer/);
        assert.equal(err.issues[0].got, '"not-an-int"');
        return true;
      },
    );
    assert.equal(transport.statementCount, 0);
  });

  it("a valid document inserts through the scoped DOC_INSERT form", async () => {
    transport.onFetchval((sql) => (sql.startsWith("SELECT DOC_INSERT") ? 12 : undefined));
    const c = doc.collection("tenants_a", { schema: userSchema });
    const id = await c.insert({ id: 12, login: "ada", tags: ["x"], profile: { bio: "hi" } });
    assert.equal(id, 12);
    assert.equal(transport.calls[0].sql, "SELECT DOC_INSERT($1, $2)");
    assert.equal(transport.calls[0].params[0], "tenants_a");
  });

  it("integer fields reject beyond-safe integers (the engine coerces numbers to f64)", async () => {
    const c = doc.collection("tenants_a", { schema: userSchema });
    await assert.rejects(
      () => c.insert({ id: 2 ** 53 + 1, login: "ada" }),
      (err: unknown) =>
        err instanceof DocumentValidationError && /would round it/.test(err.issues[0].expected),
    );
    assert.equal(transport.statementCount, 0);
    // The boundary itself is fine (MAX_SAFE_INTEGER = 2^53 - 1).
    transport.onFetchval(() => 1);
    await c.insert({ id: 2 ** 53 - 1, login: "ada" });
  });

  it("update validates the MERGED document and never sends DOC_UPDATE on violation", async () => {
    transport.onFetchval((sql) => {
      if (sql.startsWith("SELECT DOC_GET")) return JSON.stringify({ id: 1, login: "ada" });
      if (sql.startsWith("SELECT DOC_UPDATE")) return true;
      return undefined;
    });
    const c = doc.collection("tenants_a", { schema: userSchema });
    await assert.rejects(
      () => c.update(1, { id: "oops" }),
      (err: unknown) => err instanceof DocumentValidationError && err.issues[0].path === "$.id",
    );
    assert.ok(transport.calls.every((call) => !call.sql.includes("DOC_UPDATE")));
    assert.equal(await c.update(1, { login: "grace" }), true);
    const updateCall = transport.calls.find((call) => call.sql.includes("DOC_UPDATE"));
    assert.ok(updateCall);
    assert.equal(updateCall.sql, "SELECT DOC_UPDATE($1, $2, $3)");
    const merged = JSON.parse(updateCall.params[2] as string);
    assert.equal(merged.login, "grace");
  });

  it("missing/deleted records are specified: get null, update false, delete false, path null", async () => {
    transport.onFetchval((sql) => {
      if (sql.startsWith("SELECT DOC_GET")) return null;
      if (sql.startsWith("SELECT DOC_UPDATE")) return false;
      if (sql.startsWith("SELECT DOC_DELETE")) return false;
      if (sql.startsWith("SELECT DOC_PATH_IN")) return null;
      return undefined;
    });
    const c = doc.collection("tenants_a");
    assert.equal(await c.get(99), null);
    assert.equal(await c.update(99, { a: 1 }), false);
    assert.equal(await c.delete(99), false);
    assert.equal(await c.path(99, "a", "b"), null);
  });

  it("D5: a null DOC_INSERT answer throws instead of returning a fake id 0", async () => {
    const c = doc.collection("tenants_a");
    await assert.rejects(() => c.insert({ a: 1 }), /DOC_INSERT returned no id/);
    await assert.rejects(() => doc.insert("", { a: 1 }), /DOC_INSERT returned no id/);
  });

  it("fail-closed on plain PostgreSQL with zero statements (new surfaces)", async () => {
    const pgDoc = withDocument.init(transport, pgFeatures()).document;
    const pgGraph = withGraph.init(transport, pgFeatures()).graph;
    await assert.rejects(() => pgDoc.collection("x").insert({ a: 1 }), NucleusFeatureError);
    await assert.rejects(() => pgGraph.traverse(1, { maxDepth: 1 }), NucleusFeatureError);
    assert.throws(() => pgGraph.sqlNodes({ table: "t" }), NucleusFeatureError);
    assert.equal(transport.statementCount, 0);
  });
});

describe("X02 document schema validator — hand oracles", () => {
  const schema: DocumentSchema = {
    fields: {
      a: { type: "string" },
      b: { type: "integer", optional: true },
      c: { type: "number", optional: true, nullable: true },
      d: { type: "boolean" },
    },
    additionalProperties: true,
  };

  it("accepts the valid shapes and rejects the invalid ones at the exact path", () => {
    assert.deepEqual(validateDocument(schema, { a: "x", d: true }), []);
    assert.deepEqual(validateDocument(schema, { a: "x", d: true, b: 3, c: null }), []);
    assert.deepEqual(validateDocument(schema, { a: "x", d: true, z: "open by default" }), []);

    const wrongType = validateDocument(schema, { a: 1, d: true });
    assert.equal(wrongType.length, 1);
    assert.equal(wrongType[0].path, "$.a");
    assert.equal(wrongType[0].got, "1");

    const missing = validateDocument(schema, {});
    assert.equal(missing[0].path, "$.a");
    assert.equal(missing[0].got, "absent");

    const nonNullableNull = validateDocument(schema, { a: "x", d: true, c: null, b: null });
    assert.deepEqual(nonNullableNull.map((i) => i.path), ["$.b"]);
    const requiredAbsent = validateDocument(schema, { a: "x" });
    assert.deepEqual(requiredAbsent.map((i) => i.path), ["$.d"]);

    const nonInteger = validateDocument(schema, { a: "x", d: true, b: 1.5 });
    assert.equal(nonInteger[0].path, "$.b");
    assert.equal(nonInteger[0].got, "1.5");
  });

  it("additionalProperties:false rejects undeclared keys at every declared level", () => {
    const closed: DocumentSchema = {
      fields: { o: { type: "object", fields: { x: { type: "string" } }, additionalProperties: false } },
      additionalProperties: false,
    };
    assert.deepEqual(validateDocument(closed, { o: { x: "y" } }), []);
    const issues = validateDocument(closed, { o: { x: "y", rogue: 1 }, stray: true });
    assert.deepEqual(issues.map((i) => i.path), ["$.o.rogue", "$.stray"]);
  });

  it("a non-object document is rejected at the root", () => {
    for (const bad of [null, 5, "x", [1]]) {
      const issues = validateDocument({ fields: {} }, bad);
      assert.equal(issues.length, 1);
      assert.equal(issues[0].path, "$");
    }
  });
});

describe("X02 capability gate — probe-resolved, fail-closed", () => {
  let transport: X02Transport;

  beforeEach(() => {
    transport = new X02Transport();
  });

  it("probes run once per client, before the first gated statement, and are read-only", async () => {
    transport.onFetchval((sql) => (sql.startsWith("SELECT DOC_INSERT") ? 1 : undefined));
    const doc = withDocument.init(transport, nucleusFeatures()).document;
    const c = doc.collection("tenants_a");
    await c.insert({ a: 1 });
    await c.insert({ a: 2 });
    assert.deepEqual(
      transport.probeCalls.map((p) => p.sql),
      ["SELECT DOC_COUNT($1)", "SELECT DOC_GET($1, $2)", "SELECT DOC_PATH_IN($1, $2, $3)"],
    );
    assert.ok(transport.probeCalls.every((p) => /^SELECT (DOC_COUNT|DOC_GET|DOC_PATH_IN)\(/.test(p.sql)));
  });

  it("a missing engine surface fails closed with NucleusCapabilityError and zero operation statements", async () => {
    transport.probeMode = "missing";
    const doc = withDocument.init(transport, nucleusFeatures()).document;
    const graph = withGraph.init(transport, nucleusFeatures()).graph;
    const isCap = (cap: string) => (err: unknown) =>
      err instanceof NucleusCapabilityError &&
      err instanceof NucleusNotSupportedError &&
      err.capability === cap &&
      err.status === "unsupported" &&
      /42883|unknown function/.test(err.evidence);
    await assert.rejects(() => doc.collection("a").insert({ a: 1 }), isCap("document-collections"));
    await assert.rejects(() => doc.collection("a").get(1), isCap("document-collections"));
    await assert.rejects(() => graph.traverse(1, { maxDepth: 2 }), isCap("graph-adjacency"));
    await assert.rejects(() => graph.shortestPath(1, 2, 3), isCap("graph-adjacency"));
    await assert.rejects(() => graph.sqlNodes({ table: "users" }).findNodeByRow(1), isCap("graph-property-match"));
    await assert.rejects(() => graph.sqlNodes({ table: "users" }).hydrate([1]), isCap("graph-adjacency"));
    assert.equal(transport.statementCount, 0);
  });

  it("a wrong probe answer resolves unsupported (value-asserting, not acceptance)", async () => {
    transport.probeMode = "wrong";
    const doc = withDocument.init(transport, nucleusFeatures()).document;
    const graph = withGraph.init(transport, nucleusFeatures()).graph;
    await assert.rejects(
      () => doc.collection("a").count(),
      (err: unknown) => err instanceof NucleusCapabilityError && /answered wrong: expected 0 documents/.test(err.evidence),
    );
    await assert.rejects(
      () => graph.traverse(1, { maxDepth: 1 }),
      (err: unknown) => err instanceof NucleusCapabilityError && /expected no neighbors/.test(err.evidence),
    );
    assert.equal(transport.statementCount, 0);
  });

  it("a transport failure during a probe is rethrown and not cached", async () => {
    transport.probeMode = "network";
    transport.onFetchval((sql) => (sql.startsWith("SELECT DOC_COUNT") ? 4 : undefined));
    const doc = withDocument.init(transport, nucleusFeatures()).document;
    const c = doc.collection("a");
    await assert.rejects(() => c.count(), /socket hang up/);
    transport.probeMode = "supported";
    assert.equal(await c.count(), 4);
  });

  it("capabilities() reports probed support and the measured-absent set without probing the absent ones", async () => {
    const graph = withGraph.init(transport, nucleusFeatures()).graph;
    const report = await graph.capabilities();
    const byCap = Object.fromEntries(report.map((r) => [r.capability, r.status]));
    assert.deepEqual(byCap, {
      "graph-adjacency": "supported",
      "graph-property-match": "supported",
      "graph-tenant-isolation": "unsupported",
      "graph-query-parameters": "unsupported",
      "graph-multi-label": "unsupported",
      "specialty-session-isolation": "unsupported",
      "atomic-sql-specialty-writes": "unsupported",
    });
    assert.equal(transport.probeCalls.length, 3); // adjacency (2) + property match (1)
    const doc = withDocument.init(transport, nucleusFeatures()).document;
    const docReport = await doc.capabilities();
    assert.deepEqual(docReport.map((r) => [r.capability, r.status]), [
      ["document-collections", "supported"],
      ["specialty-session-isolation", "unsupported"],
      ["atomic-sql-specialty-writes", "unsupported"],
    ]);
  });

  it("multi-label and query parameters fail as named capabilities", async () => {
    const graph = withGraph.init(transport, nucleusFeatures()).graph;
    await assert.rejects(
      () => graph.addNode(["A", "B"]),
      (err: unknown) => err instanceof NucleusCapabilityError && err.capability === "graph-multi-label",
    );
    await assert.rejects(
      () => graph.query("MATCH (n) RETURN n", { x: 1 }),
      (err: unknown) => err instanceof NucleusCapabilityError && err.capability === "graph-query-parameters",
    );
    assert.equal(transport.statementCount + transport.probeCalls.length, 0);
  });
});

describe("X02 review fixes — identity, truncation, validator edges", () => {
  let transport: X02Transport;
  let graph: ReturnType<typeof withGraph.init>["graph"];

  beforeEach(() => {
    transport = new X02Transport();
    graph = withGraph.init(transport, nucleusFeatures()).graph;
  });

  it("hydrate never claims a node stamped for the same table name in another schema", async () => {
    transport.onFetchval((sql, params) => {
      if (sql.startsWith("SELECT GRAPH_NODE")) {
        const node = Number(params[0]);
        const schema = node === 1 ? "app" : node === 2 ? "other" : undefined;
        const props: Record<string, unknown> = { [SQL_ID_TABLE_KEY]: "users", [SQL_ID_ROW_KEY]: 10 };
        if (schema) props[SQL_ID_SCHEMA_KEY] = schema;
        return JSON.stringify({ id: node, labels: ["User"], properties: props });
      }
      return undefined;
    });
    transport.onQuery(() => [{ id: 10, name: "Ada" }]);
    const app = await graph.sqlNodes({ schema: "app", table: "users" }).hydrate([1, 2, 3]);
    assert.equal(app[0].sqlRef?.schema, "app");
    assert.equal((app[0].row as Record<string, unknown>).name, "Ada");
    assert.deepEqual(app[1], { nodeId: 2, sqlRef: null, row: null });
    assert.deepEqual(app[2], { nodeId: 3, sqlRef: null, row: null });
    const unqualified = await graph.sqlNodes({ table: "users" }).hydrate([1, 3]);
    assert.deepEqual(unqualified[0], { nodeId: 1, sqlRef: null, row: null });
    assert.equal(unqualified[1].sqlRef?.id, 10);
  });

  it("findNodeByRow filters the schema stamp and reports duplicate stamps as a conflict", async () => {
    transport.onFetchval((sql) =>
      sql.startsWith("SELECT GRAPH_QUERY") ? '{"columns":["n","n.sqlref_schema"],"rows":[[7,null],[8,"app"],[9,"app"]]}' : undefined,
    );
    assert.equal(await graph.sqlNodes({ table: "users" }).findNodeByRow(5), 7);
    await assert.rejects(
      () => graph.sqlNodes({ schema: "app", table: "users" }).findNodeByRow(5),
      (err: unknown) => err instanceof NucleusConflictError && /stamped on 2 nodes \(8, 9\)/.test(err.message),
    );
    assert.match(transport.calls[0].params[0] as string, /RETURN n, n\.sqlref_schema$/);
  });

  it("addRowNode refuses a second node for the same row, before any write", async () => {
    transport.onFetchval((sql) =>
      sql.startsWith("SELECT GRAPH_QUERY") ? '{"columns":["n","n.sqlref_schema"],"rows":[[7,null]]}' : undefined,
    );
    await assert.rejects(
      () => graph.sqlNodes({ table: "users" }).addRowNode(5, "User"),
      (err: unknown) => err instanceof NucleusConflictError && /row 5 of "users" already has node 7/.test(err.message),
    );
    assert.ok(transport.calls.every((c) => !c.sql.includes("GRAPH_ADD_NODE")));
  });

  it("hydrate accepts int8 keys that arrive as digit strings", async () => {
    transport.onFetchval((sql) =>
      sql.startsWith("SELECT GRAPH_NODE")
        ? JSON.stringify({ id: 1, labels: ["U"], properties: { [SQL_ID_TABLE_KEY]: "users", [SQL_ID_ROW_KEY]: 10 } })
        : undefined,
    );
    transport.onQuery(() => [{ id: "10", name: "Ada" }]);
    const [h] = await graph.sqlNodes({ table: "users" }).hydrate([1]);
    assert.equal((h.row as Record<string, unknown>).name, "Ada");
  });

  it("a truncated traversal is still ordered by (depth, id)", async () => {
    transport.onFetchval((sql, params) => {
      if (sql.startsWith("SELECT GRAPH_NODE")) return '{"id":1}';
      if (sql.startsWith(NEIGHBOR_SQL)) {
        return Number(params[0]) === 1 ? nb([[9, 1, "E"], [3, 2, "E"], [5, 3, "E"]]) : nb([]);
      }
      return undefined;
    });
    const r = await graph.traverse(1, { maxDepth: 3, maxNodes: 2 });
    assert.equal(r.truncated, true);
    assert.deepEqual(r.nodes.map((n) => n.id), [3, 9]);
  });

  it("addNode turns a null engine answer into an error, not node 0", async () => {
    await assert.rejects(() => graph.addNode(["A"]), /GRAPH_ADD_NODE returned no id/);
  });

  it("collection ids must be positive safe integers — rejected before any statement", async () => {
    const doc = withDocument.init(transport, nucleusFeatures()).document;
    const c = doc.collection("a");
    for (const bad of [0, -1, 1.5, Number.NaN, 2 ** 53]) {
      await assert.rejects(() => c.get(bad), /positive safe integer/);
      await assert.rejects(() => c.delete(bad), /positive safe integer/);
    }
    assert.throws(() => doc.collection("a", { boundTo: { table: "bad name" } }), /Invalid SQL table reference/);
    assert.equal(transport.statementCount + transport.probeCalls.length, 0);
  });

  it("validator: undefined is absence, and object fields must be plain JSON objects", () => {
    const schema: DocumentSchema = {
      fields: { a: { type: "string", optional: true }, o: { type: "object", optional: true } },
      additionalProperties: false,
    };
    assert.deepEqual(validateDocument(schema, { a: undefined, stray: undefined }), []);
    assert.deepEqual(validateDocument({ fields: { a: { type: "string" } } }, { a: undefined }).map((i) => i.got), ["absent"]);
    const dated = validateDocument(schema, { o: new Date(0) });
    assert.deepEqual(dated.map((i) => [i.path, i.got]), [["$.o", "Date instance"]]);
    assert.equal(validateDocument(schema, new Map() as unknown)[0].path, "$");
    assert.equal(validateDocument({ fields: { n: { type: "integer" } } }, { n: 5n })[0].got, "bigint 5n");
  });
});
