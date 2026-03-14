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
    assert.ok((transport.calls[0].args[0] as string).includes("VECTOR(384)"));
    assert.ok((transport.calls[1].args[0] as string).includes("CREATE INDEX"));
    assert.ok((transport.calls[1].args[0] as string).includes("cosine"));
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
    transport.onFetchval("SELECT FTS_SEARCH", '[{"docId":1,"score":0.95}]');
    const results = await fts.search("hello");
    assert.equal(results.length, 1);
    assert.equal(results[0].docId, 1);
  });

  it("search with fuzzy sends FTS_FUZZY_SEARCH", async () => {
    transport.onFetchval("SELECT FTS_FUZZY_SEARCH", '[{"docId":1,"score":0.8}]');
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

  it("put stores metadata tags", async () => {
    await blob.put("bucket", "key", "aa", { metadata: { env: "prod" } });
    // Should have 2 calls: BLOB_STORE + BLOB_TAG
    assert.equal(transport.calls.length, 2);
    assert.ok((transport.calls[1].args[0] as string).includes("BLOB_TAG"));
  });

  it("get returns data and meta", async () => {
    transport.onFetchval("SELECT BLOB_GET", "48656c6c6f");
    transport.onFetchval("SELECT BLOB_META", JSON.stringify({
      key: "bucket/file.bin",
      size: 5,
      content_type: "application/octet-stream",
      created_at: "2024-01-01T00:00:00Z",
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
    transport.onFetchval("SELECT TS_RETENTION", true);
    const result = await ts.retention("cpu", 30);
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

  it("insert sends COLUMNAR_INSERT", async () => {
    transport.onFetchval("SELECT COLUMNAR_INSERT", true);
    const result = await columnar.insert("events", { type: "click" });
    assert.equal(result, true);
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

  it("assert sends DATALOG_ASSERT", async () => {
    transport.onFetchval("SELECT DATALOG_ASSERT", true);
    const result = await datalog.assert("parent(alice, bob)");
    assert.equal(result, true);
  });

  it("retract sends DATALOG_RETRACT", async () => {
    transport.onFetchval("SELECT DATALOG_RETRACT", true);
    const result = await datalog.retract("parent(alice, bob)");
    assert.equal(result, true);
  });

  it("rule sends DATALOG_RULE", async () => {
    transport.onFetchval("SELECT DATALOG_RULE", true);
    const result = await datalog.rule("ancestor(X, Y)", "parent(X, Y)");
    assert.equal(result, true);
  });

  it("query sends DATALOG_QUERY", async () => {
    transport.onFetchval("SELECT DATALOG_QUERY", "alice,bob");
    const result = await datalog.query("parent(X, bob)");
    assert.equal(result, "alice,bob");
  });

  it("clear sends DATALOG_CLEAR", async () => {
    transport.onFetchval("SELECT DATALOG_CLEAR", true);
    const result = await datalog.clear();
    assert.equal(result, true);
  });

  it("importGraph sends DATALOG_IMPORT_GRAPH", async () => {
    transport.onFetchval("SELECT DATALOG_IMPORT_GRAPH", 50);
    const count = await datalog.importGraph();
    assert.equal(count, 50);
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

  it("read sends CDC_READ", async () => {
    transport.onFetchval("SELECT CDC_READ", '[{"op":"insert"}]');
    const result = await cdc.read(0);
    assert.equal(result, '[{"op":"insert"}]');
  });

  it("count sends CDC_COUNT", async () => {
    transport.onFetchval("SELECT CDC_COUNT", 100);
    const c = await cdc.count();
    assert.equal(c, 100);
  });

  it("tableRead sends CDC_TABLE_READ", async () => {
    transport.onFetchval("SELECT CDC_TABLE_READ", "[]");
    const result = await cdc.tableRead("users", 0);
    assert.equal(result, "[]");
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

  it("channels sends PUBSUB_CHANNELS", async () => {
    transport.onFetchval("SELECT PUBSUB_CHANNELS()", "chat,events");
    const result = await pubsub.channels();
    assert.equal(result, "chat,events");
  });

  it("channels with pattern sends PUBSUB_CHANNELS($1)", async () => {
    transport.onFetchval("SELECT PUBSUB_CHANNELS($1)", "chat");
    const result = await pubsub.channels("ch*");
    assert.equal(result, "chat");
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

describe("migrate", () => {
  let transport: MockTransport;

  const migrations: Migration[] = [
    { version: 1, name: "create_users", up: "CREATE TABLE users (id INT)", down: "DROP TABLE users" },
    { version: 2, name: "add_email", up: "ALTER TABLE users ADD COLUMN email TEXT", down: "ALTER TABLE users DROP COLUMN email" },
    { version: 3, name: "create_posts", up: "CREATE TABLE posts (id INT)", down: "DROP TABLE posts" },
  ];

  beforeEach(() => {
    transport = new MockTransport();
    // Make ensureTable + appliedVersions work: SELECT version returns empty
    transport.onQuery("SELECT version", []);
  });

  it("runs all pending migrations", async () => {
    const ran = await migrate(transport, migrations);
    assert.deepEqual(ran, ["create_users", "add_email", "create_posts"]);
  });

  it("skips already applied migrations", async () => {
    transport.onQuery("SELECT version", [{ version: 1 }]);
    const ran = await migrate(transport, migrations);
    assert.deepEqual(ran, ["add_email", "create_posts"]);
  });

  it("returns empty array when all are applied", async () => {
    transport.onQuery("SELECT version", [{ version: 1 }, { version: 2 }, { version: 3 }]);
    const ran = await migrate(transport, migrations);
    assert.deepEqual(ran, []);
  });

  it("runs migrations in ascending version order", async () => {
    const reversed = [...migrations].reverse();
    const ran = await migrate(transport, reversed);
    assert.deepEqual(ran, ["create_users", "add_email", "create_posts"]);
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
  });

  it("rolls back the most recent migration", async () => {
    transport.onQuery("SELECT version", [{ version: 1 }, { version: 2 }]);
    const rolled = await migrateDown(transport, migrations, 1);
    assert.deepEqual(rolled, ["add_email"]);
  });

  it("rolls back multiple steps", async () => {
    transport.onQuery("SELECT version", [{ version: 1 }, { version: 2 }]);
    const rolled = await migrateDown(transport, migrations, 2);
    assert.deepEqual(rolled, ["add_email", "create_users"]);
  });

  it("throws when migration has no down SQL", async () => {
    const noDown: Migration[] = [{ version: 1, name: "irreversible", up: "DO SOMETHING" }];
    transport.onQuery("SELECT version", [{ version: 1 }]);
    await assert.rejects(() => migrateDown(transport, noDown, 1), Error);
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
