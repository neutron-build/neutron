// ---------------------------------------------------------------------------
// @neutron/nucleus — integration tests
//
// These tests verify end-to-end wiring of the Nucleus client:
// - Connection flow (transport -> feature detection -> plugin init)
// - Feature detection with granular NUCLEUS_FEATURES() support
// - Each data model's SQL function interface
// - Error handling for unsupported models on plain Postgres
// - Plugin composition and type safety
// ---------------------------------------------------------------------------

import assert from "node:assert/strict";
import { describe, it, beforeEach } from "node:test";

import {
  createClient,
  detectFeatures,
  NucleusFeatureError,
  NucleusTransactionError,
  NucleusNotFoundError,
} from "./index.js";

import type {
  Transport,
  TransactionTransport,
  QueryResult,
  IsolationLevel,
  NucleusFeatures,
} from "./types.js";

import { requireNucleus } from "./helpers.js";

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

// =========================================================================
// Mock Transport — simulates pgwire responses
// =========================================================================

interface MockCall {
  method: string;
  sql?: string;
  params?: unknown[];
}

class WireTransport implements Transport {
  readonly calls: MockCall[] = [];
  private responses: Map<string, unknown> = new Map();
  private queryResponses: Map<string, unknown[]> = new Map();

  /** Register a fetchval response for SQL matching a prefix. */
  whenFetchval(sqlPrefix: string, result: unknown): void {
    this.responses.set(sqlPrefix, result);
  }

  /** Register a query response for SQL matching a prefix. */
  whenQuery(sqlPrefix: string, rows: unknown[]): void {
    this.queryResponses.set(sqlPrefix, rows);
  }

  async query<T = Record<string, unknown>>(sql: string, params: unknown[] = []): Promise<QueryResult<T>> {
    this.calls.push({ method: "query", sql, params });
    for (const [prefix, rows] of this.queryResponses) {
      if (sql.includes(prefix)) {
        const r = rows as T[];
        return { rows: r, rowCount: r.length };
      }
    }
    return { rows: [], rowCount: 0 };
  }

  async execute(sql: string, params: unknown[] = []): Promise<number> {
    this.calls.push({ method: "execute", sql, params });
    return 1;
  }

  async fetchval<T = unknown>(sql: string, params: unknown[] = []): Promise<T | null> {
    this.calls.push({ method: "fetchval", sql, params });
    for (const [prefix, result] of this.responses) {
      if (sql.includes(prefix)) {
        return result as T;
      }
    }
    return null;
  }

  async beginTransaction(_isolation?: IsolationLevel): Promise<TransactionTransport> {
    this.calls.push({ method: "beginTransaction" });
    return new MockTx(this);
  }

  async close(): Promise<void> {
    this.calls.push({ method: "close" });
  }

  async ping(): Promise<void> {
    this.calls.push({ method: "ping" });
  }

  /** Return calls whose SQL contains the given substring. */
  sqlCalls(substring: string): MockCall[] {
    return this.calls.filter((c) => c.sql?.includes(substring));
  }

  reset(): void {
    this.calls.length = 0;
    this.responses.clear();
    this.queryResponses.clear();
  }
}

class MockTx implements TransactionTransport {
  private parent: WireTransport;
  private done = false;

  constructor(parent: WireTransport) {
    this.parent = parent;
  }

  async query<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<QueryResult<T>> {
    if (this.done) throw new NucleusTransactionError("Transaction finished");
    return this.parent.query<T>(sql, params);
  }

  async execute(sql: string, params?: unknown[]): Promise<number> {
    if (this.done) throw new NucleusTransactionError("Transaction finished");
    return this.parent.execute(sql, params);
  }

  async fetchval<T = unknown>(sql: string, params?: unknown[]): Promise<T | null> {
    if (this.done) throw new NucleusTransactionError("Transaction finished");
    return this.parent.fetchval<T>(sql, params);
  }

  async beginTransaction(): Promise<TransactionTransport> {
    throw new NucleusTransactionError("Nested transactions are not supported");
  }

  async commit(): Promise<void> { this.done = true; }
  async rollback(): Promise<void> { this.done = true; }
  async close(): Promise<void> { if (!this.done) await this.rollback(); }
  async ping(): Promise<void> { await this.query("SELECT 1"); }
}

// =========================================================================
// Feature helpers
// =========================================================================

function nucleusFeatures(overrides?: Partial<NucleusFeatures>): NucleusFeatures {
  return {
    isNucleus: true,
    hasKV: true, hasVector: true, hasTimeSeries: true, hasDocument: true,
    hasGraph: true, hasFTS: true, hasGeo: true, hasBlob: true,
    hasStreams: true, hasColumnar: true, hasDatalog: true,
    hasCDC: true, hasPubSub: true, version: "Nucleus 0.1.0",
    ...overrides,
  };
}

function pgFeatures(): NucleusFeatures {
  return {
    isNucleus: false,
    hasKV: false, hasVector: false, hasTimeSeries: false, hasDocument: false,
    hasGraph: false, hasFTS: false, hasGeo: false, hasBlob: false,
    hasStreams: false, hasColumnar: false, hasDatalog: false,
    hasCDC: false, hasPubSub: false, version: "PostgreSQL 16.0",
  };
}

// =========================================================================
// 1. Connection + Feature Detection
// =========================================================================

describe("Integration: connection and feature detection", () => {
  it("connects to Nucleus and detects all features via VERSION", async () => {
    const transport = new WireTransport();
    transport.whenFetchval("SELECT VERSION()", "Nucleus 0.1.0");

    const client = await createClient({ url: "http://localhost:5432", transport })
      .use(withSQL)
      .connect();

    assert.equal(client.features.isNucleus, true);
    assert.equal(client.features.hasKV, true);
    assert.equal(client.features.hasVector, true);
    assert.equal(client.features.version, "Nucleus 0.1.0");
    assert.equal(typeof client.sql.query, "function");
    await client.close();
  });

  it("connects to Postgres and disables all Nucleus features", async () => {
    const transport = new WireTransport();
    transport.whenFetchval("SELECT VERSION()", "PostgreSQL 16.2");

    const client = await createClient({ url: "http://localhost:5432", transport })
      .use(withSQL)
      .use(withKV)
      .connect();

    assert.equal(client.features.isNucleus, false);
    assert.equal(client.features.hasKV, false);
    assert.equal(typeof client.sql.query, "function"); // SQL always works
    await client.close();
  });

  it("detects granular features via NUCLEUS_FEATURES()", async () => {
    const transport = new WireTransport();
    transport.whenFetchval("SELECT VERSION()", "Nucleus 0.2.0");
    transport.whenFetchval("SELECT NUCLEUS_FEATURES()", JSON.stringify({
      kv: true, vector: true, timeseries: true, document: true,
      graph: false, fts: true, geo: false, blob: true,
      streams: true, columnar: false, datalog: false,
      cdc: true, pubsub: true,
    }));

    const features = await detectFeatures(transport);

    assert.equal(features.isNucleus, true);
    assert.equal(features.hasKV, true);
    assert.equal(features.hasVector, true);
    assert.equal(features.hasGraph, false); // explicitly disabled
    assert.equal(features.hasGeo, false);
    assert.equal(features.hasColumnar, false);
    assert.equal(features.hasDatalog, false);
    assert.equal(features.hasFTS, true);
    assert.equal(features.hasCDC, true);
  });

  it("falls back to all-enabled when NUCLEUS_FEATURES() is unavailable", async () => {
    const transport = new WireTransport();
    transport.whenFetchval("SELECT VERSION()", "Nucleus 0.1.0");
    // No response registered for NUCLEUS_FEATURES — fetchval returns null

    const features = await detectFeatures(transport);

    assert.equal(features.isNucleus, true);
    assert.equal(features.hasKV, true);
    assert.equal(features.hasVector, true);
    assert.equal(features.hasGraph, true);
  });

  it("ping verifies server reachability", async () => {
    const transport = new WireTransport();
    transport.whenFetchval("SELECT VERSION()", "Nucleus 0.1.0");

    const client = await createClient({ url: "http://localhost:5432", transport }).connect();
    await client.ping();

    const pingCalls = transport.calls.filter((c) => c.method === "ping");
    assert.equal(pingCalls.length, 1);
    await client.close();
  });
});

// =========================================================================
// 2. KV Model — SQL Function Interface
// =========================================================================

describe("Integration: KV model SQL functions", () => {
  let transport: WireTransport;
  let kv: ReturnType<typeof withKV.init>["kv"];

  beforeEach(() => {
    transport = new WireTransport();
    kv = withKV.init(transport, nucleusFeatures()).kv;
  });

  it("KV_GET sends correct SQL", async () => {
    transport.whenFetchval("KV_GET", "hello");
    const val = await kv.get("mykey");
    assert.equal(val, "hello");
    const calls = transport.sqlCalls("KV_GET");
    assert.equal(calls.length, 1);
    assert.deepEqual(calls[0].params, ["mykey"]);
  });

  it("KV_SET sends correct SQL with TTL", async () => {
    await kv.set("k", "v", { ttl: 300 });
    const calls = transport.sqlCalls("KV_SET");
    assert.equal(calls.length, 1);
    assert.ok(calls[0].params!.includes(300));
  });

  it("KV_SETNX returns boolean", async () => {
    transport.whenFetchval("KV_SETNX", true);
    const ok = await kv.setNX("k", "v");
    assert.equal(ok, true);
  });

  it("KV_DEL returns boolean", async () => {
    transport.whenFetchval("KV_DEL", true);
    const ok = await kv.delete("k");
    assert.equal(ok, true);
  });

  it("KV_EXISTS returns boolean", async () => {
    transport.whenFetchval("KV_EXISTS", true);
    const ok = await kv.exists("k");
    assert.equal(ok, true);
  });

  it("KV_INCR returns incremented value", async () => {
    transport.whenFetchval("KV_INCR", 42);
    const val = await kv.incr("counter", 5);
    assert.equal(val, 42);
  });

  it("KV_TTL returns remaining seconds", async () => {
    transport.whenFetchval("KV_TTL", 120);
    const ttl = await kv.ttl("k");
    assert.equal(ttl, 120);
  });

  it("KV_DBSIZE returns count", async () => {
    transport.whenFetchval("KV_DBSIZE", 999);
    const size = await kv.dbSize();
    assert.equal(size, 999);
  });

  it("KV_LPUSH returns list length", async () => {
    transport.whenFetchval("KV_LPUSH", 3);
    const len = await kv.lpush("list", "item");
    assert.equal(len, 3);
  });

  it("KV_HSET returns boolean", async () => {
    transport.whenFetchval("KV_HSET", true);
    const isNew = await kv.hset("hash", "field", "val");
    assert.equal(isNew, true);
  });

  it("KV_SADD returns boolean", async () => {
    transport.whenFetchval("KV_SADD", true);
    const isNew = await kv.sadd("set", "member");
    assert.equal(isNew, true);
  });

  it("KV_ZADD returns boolean", async () => {
    transport.whenFetchval("KV_ZADD", true);
    const isNew = await kv.zadd("zset", 1.5, "member");
    assert.equal(isNew, true);
  });

  it("KV_PFADD returns boolean", async () => {
    transport.whenFetchval("KV_PFADD", true);
    const changed = await kv.pfadd("hll", "element");
    assert.equal(changed, true);
  });

  it("throws NucleusFeatureError on plain Postgres", async () => {
    const pgKv = withKV.init(transport, pgFeatures()).kv;
    await assert.rejects(() => pgKv.get("key"), NucleusFeatureError);
  });
});

// =========================================================================
// 3. Vector Model — SQL Function Interface
// =========================================================================

describe("Integration: Vector model SQL functions", () => {
  let transport: WireTransport;
  let vector: ReturnType<typeof withVector.init>["vector"];

  beforeEach(() => {
    transport = new WireTransport();
    vector = withVector.init(transport, nucleusFeatures()).vector;
  });

  it("VECTOR_DISTANCE computes distance between vectors", async () => {
    transport.whenFetchval("VECTOR_DISTANCE", 0.15);
    const dist = await vector.distance([1, 0, 0], [0, 1, 0], "cosine");
    assert.equal(dist, 0.15);
    const calls = transport.sqlCalls("VECTOR_DISTANCE");
    assert.equal(calls.length, 1);
  });

  it("VECTOR_DIMS returns dimensionality", async () => {
    transport.whenFetchval("VECTOR_DIMS", 384);
    const dims = await vector.dims([1, 2, 3]);
    assert.equal(dims, 384);
  });

  it("createCollection generates CREATE TABLE and INDEX", async () => {
    await vector.createCollection("embeddings", 768, "cosine");
    const creates = transport.sqlCalls("CREATE TABLE");
    assert.equal(creates.length, 1);
    assert.ok(creates[0].sql!.includes("VECTOR(768)"));
    const indexes = transport.sqlCalls("CREATE INDEX");
    assert.equal(indexes.length, 1);
    assert.ok(indexes[0].sql!.includes("cosine"));
  });

  it("search uses VECTOR_DISTANCE with ORDER BY", async () => {
    transport.whenQuery("VECTOR_DISTANCE", [
      { id: "a", metadata: '{"tag":"test"}', distance: 0.1 },
      { id: "b", metadata: '{"tag":"demo"}', distance: 0.3 },
    ]);
    const results = await vector.search("embeddings", [1, 0], { limit: 5, metric: "l2" });
    assert.equal(results.length, 2);
    assert.equal(results[0].distance, 0.1);
    assert.ok(results[0].score > 0);
  });

  it("insert uses VECTOR() constructor", async () => {
    await vector.insert("col", "id1", [1, 2, 3], { tag: "test" });
    const calls = transport.sqlCalls("INSERT INTO col");
    assert.equal(calls.length, 1);
    assert.ok(calls[0].sql!.includes("VECTOR"));
  });

  it("validates dimension bounds", async () => {
    await assert.rejects(() => vector.createCollection("bad", 0), /Invalid vector dimension/);
    await assert.rejects(() => vector.createCollection("bad", 99999), /Invalid vector dimension/);
  });

  it("validates distance metric", async () => {
    await assert.rejects(
      () => vector.createCollection("bad", 128, "invalid" as any),
      /Invalid distance metric/
    );
  });

  it("throws NucleusFeatureError on plain Postgres", async () => {
    const pgVector = withVector.init(transport, pgFeatures()).vector;
    await assert.rejects(() => pgVector.distance([1], [2]), NucleusFeatureError);
  });
});

// =========================================================================
// 4. TimeSeries Model — SQL Function Interface
// =========================================================================

describe("Integration: TimeSeries model SQL functions", () => {
  let transport: WireTransport;
  let ts: ReturnType<typeof withTimeSeries.init>["timeseries"];

  beforeEach(() => {
    transport = new WireTransport();
    ts = withTimeSeries.init(transport, nucleusFeatures()).timeseries;
  });

  it("TS_INSERT writes data points", async () => {
    const now = new Date();
    await ts.write("cpu_usage", [{ timestamp: now, value: 85.5 }]);
    const calls = transport.sqlCalls("TS_INSERT");
    assert.equal(calls.length, 1);
    assert.equal(calls[0].params![0], "cpu_usage");
    assert.equal(calls[0].params![1], now.getTime());
    assert.equal(calls[0].params![2], 85.5);
  });

  it("TS_LAST returns most recent value", async () => {
    transport.whenFetchval("TS_LAST", 72.3);
    const val = await ts.last("temperature");
    assert.equal(val, 72.3);
  });

  it("TS_COUNT returns point count", async () => {
    transport.whenFetchval("TS_COUNT", 5000);
    const count = await ts.count("metrics");
    assert.equal(count, 5000);
  });

  it("TS_RANGE_COUNT counts points in range", async () => {
    transport.whenFetchval("TS_RANGE_COUNT", 150);
    const count = await ts.rangeCount("cpu", new Date("2024-01-01"), new Date("2024-12-31"));
    assert.equal(count, 150);
  });

  it("TS_RANGE_AVG computes average in range", async () => {
    transport.whenFetchval("TS_RANGE_AVG", 42.5);
    const avg = await ts.rangeAvg("temp", new Date("2024-01-01"), new Date("2024-06-30"));
    assert.equal(avg, 42.5);
  });

  it("TS_RETENTION sets retention policy", async () => {
    transport.whenFetchval("TS_RETENTION", true);
    const ok = await ts.retention("logs", 90);
    assert.equal(ok, true);
  });

  it("TIME_BUCKET truncates timestamp", async () => {
    transport.whenFetchval("TIME_BUCKET", 1704067200000);
    const bucket = await ts.timeBucket("hour", new Date("2024-01-01T12:34:56Z"));
    assert.equal(bucket, 1704067200000);
  });

  it("validates aggregation function", async () => {
    await assert.rejects(
      () => ts.aggregate("m", new Date(), new Date(), "hour", "invalid" as any),
      /Invalid aggregation function/
    );
  });

  it("throws NucleusFeatureError on plain Postgres", async () => {
    const pgTs = withTimeSeries.init(transport, pgFeatures()).timeseries;
    await assert.rejects(() => pgTs.count("m"), NucleusFeatureError);
  });
});

// =========================================================================
// 5. Document Model — SQL Function Interface
// =========================================================================

describe("Integration: Document model SQL functions", () => {
  let transport: WireTransport;
  let doc: ReturnType<typeof withDocument.init>["document"];

  beforeEach(() => {
    transport = new WireTransport();
    doc = withDocument.init(transport, nucleusFeatures()).document;
  });

  it("DOC_INSERT inserts a document", async () => {
    transport.whenFetchval("DOC_INSERT", 42);
    const id = await doc.insert("users", { name: "Alice", age: 30 });
    assert.equal(id, 42);
    const calls = transport.sqlCalls("DOC_INSERT");
    assert.equal(calls.length, 1);
  });

  it("DOC_GET retrieves a document", async () => {
    transport.whenFetchval("DOC_GET", '{"name":"Alice","age":30}');
    const result = await doc.get(42);
    assert.deepEqual(result, { name: "Alice", age: 30 });
  });

  it("DOC_GET returns null for missing document", async () => {
    const result = await doc.get(999);
    assert.equal(result, null);
  });

  it("DOC_QUERY returns matching IDs", async () => {
    transport.whenFetchval("DOC_QUERY", "1,2,3");
    const ids = await doc.queryDocs({ status: "active" });
    assert.deepEqual(ids, [1, 2, 3]);
  });

  it("DOC_PATH extracts nested value", async () => {
    transport.whenFetchval("DOC_PATH", "Alice");
    const val = await doc.path(1, "user", "name");
    assert.equal(val, "Alice");
  });

  it("DOC_COUNT returns total documents", async () => {
    transport.whenFetchval("DOC_COUNT", 1500);
    const count = await doc.count();
    assert.equal(count, 1500);
  });

  it("throws NucleusFeatureError on plain Postgres", async () => {
    const pgDoc = withDocument.init(transport, pgFeatures()).document;
    await assert.rejects(() => pgDoc.count(), NucleusFeatureError);
  });
});

// =========================================================================
// 6. Graph Model — SQL Function Interface
// =========================================================================

describe("Integration: Graph model SQL functions", () => {
  let transport: WireTransport;
  let graph: ReturnType<typeof withGraph.init>["graph"];

  beforeEach(() => {
    transport = new WireTransport();
    graph = withGraph.init(transport, nucleusFeatures()).graph;
  });

  it("GRAPH_ADD_NODE creates a node", async () => {
    transport.whenFetchval("GRAPH_ADD_NODE", 1);
    const id = await graph.addNode(["Person"], { name: "Alice" });
    assert.equal(id, 1);
  });

  it("GRAPH_ADD_EDGE creates an edge", async () => {
    transport.whenFetchval("GRAPH_ADD_EDGE", 10);
    const id = await graph.addEdge(1, 2, "KNOWS", { since: 2020 });
    assert.equal(id, 10);
  });

  it("GRAPH_DELETE_NODE removes a node", async () => {
    transport.whenFetchval("GRAPH_DELETE_NODE", true);
    const ok = await graph.deleteNode(1);
    assert.equal(ok, true);
  });

  it("GRAPH_DELETE_EDGE removes an edge", async () => {
    transport.whenFetchval("GRAPH_DELETE_EDGE", true);
    const ok = await graph.deleteEdge(10);
    assert.equal(ok, true);
  });

  it("GRAPH_QUERY executes Cypher", async () => {
    transport.whenFetchval("GRAPH_QUERY", JSON.stringify({
      columns: ["n.name"],
      rows: [{ "n.name": "Alice" }],
    }));
    const result = await graph.query("MATCH (n:Person) RETURN n.name");
    assert.equal(result.columns.length, 1);
    assert.equal(result.rows.length, 1);
  });

  it("GRAPH_SHORTEST_PATH returns node path", async () => {
    transport.whenFetchval("GRAPH_SHORTEST_PATH", "[1,5,3,7]");
    const path = await graph.shortestPath(1, 7);
    assert.deepEqual(path, [1, 5, 3, 7]);
  });

  it("GRAPH_NEIGHBORS returns adjacent nodes", async () => {
    transport.whenFetchval("GRAPH_NEIGHBORS", JSON.stringify([
      { id: 2, labels: ["Person"], properties: { name: "Bob" } },
    ]));
    const neighbors = await graph.neighbors(1);
    assert.equal(neighbors.length, 1);
    assert.equal(neighbors[0].id, 2);
  });

  it("GRAPH_NODE_COUNT returns total nodes", async () => {
    transport.whenFetchval("GRAPH_NODE_COUNT", 42);
    assert.equal(await graph.nodeCount(), 42);
  });

  it("GRAPH_EDGE_COUNT returns total edges", async () => {
    transport.whenFetchval("GRAPH_EDGE_COUNT", 100);
    assert.equal(await graph.edgeCount(), 100);
  });

  it("throws NucleusFeatureError on plain Postgres", async () => {
    const pgGraph = withGraph.init(transport, pgFeatures()).graph;
    await assert.rejects(() => pgGraph.nodeCount(), NucleusFeatureError);
  });
});

// =========================================================================
// 7. FTS Model — SQL Function Interface
// =========================================================================

describe("Integration: FTS model SQL functions", () => {
  let transport: WireTransport;
  let fts: ReturnType<typeof withFTS.init>["fts"];

  beforeEach(() => {
    transport = new WireTransport();
    fts = withFTS.init(transport, nucleusFeatures()).fts;
  });

  it("FTS_INDEX indexes a document", async () => {
    transport.whenFetchval("FTS_INDEX", true);
    const ok = await fts.index(1, "Hello world this is a test");
    assert.equal(ok, true);
  });

  it("FTS_SEARCH returns results", async () => {
    transport.whenFetchval("FTS_SEARCH", JSON.stringify([
      { docId: 1, score: 0.95 },
      { docId: 5, score: 0.72 },
    ]));
    const results = await fts.search("hello world");
    assert.equal(results.length, 2);
    assert.equal(results[0].docId, 1);
  });

  it("FTS_FUZZY_SEARCH supports fuzzy matching", async () => {
    transport.whenFetchval("FTS_FUZZY_SEARCH", JSON.stringify([
      { docId: 1, score: 0.8 },
    ]));
    const results = await fts.search("helo", { fuzzyDistance: 2 });
    assert.equal(results.length, 1);
    const calls = transport.sqlCalls("FTS_FUZZY_SEARCH");
    assert.equal(calls.length, 1);
  });

  it("FTS_REMOVE removes a document from index", async () => {
    transport.whenFetchval("FTS_REMOVE", true);
    const ok = await fts.remove(1);
    assert.equal(ok, true);
  });

  it("FTS_DOC_COUNT returns indexed document count", async () => {
    transport.whenFetchval("FTS_DOC_COUNT", 500);
    assert.equal(await fts.docCount(), 500);
  });

  it("FTS_TERM_COUNT returns indexed term count", async () => {
    transport.whenFetchval("FTS_TERM_COUNT", 12000);
    assert.equal(await fts.termCount(), 12000);
  });

  it("throws NucleusFeatureError on plain Postgres", async () => {
    const pgFts = withFTS.init(transport, pgFeatures()).fts;
    await assert.rejects(() => pgFts.search("query"), NucleusFeatureError);
  });
});

// =========================================================================
// 8. Geo Model — SQL Function Interface
// =========================================================================

describe("Integration: Geo model SQL functions", () => {
  let transport: WireTransport;
  let geo: ReturnType<typeof withGeo.init>["geo"];

  beforeEach(() => {
    transport = new WireTransport();
    geo = withGeo.init(transport, nucleusFeatures()).geo;
  });

  it("GEO_DISTANCE computes haversine distance", async () => {
    transport.whenFetchval("GEO_DISTANCE", 5572);
    const dist = await geo.distance(
      { lat: 40.7128, lon: -74.006 },
      { lat: 51.5074, lon: -0.1278 },
    );
    assert.equal(dist, 5572);
  });

  it("GEO_WITHIN checks radius containment", async () => {
    transport.whenFetchval("GEO_WITHIN", true);
    const ok = await geo.within(
      { lat: 40.7128, lon: -74.006 },
      { lat: 40.7138, lon: -74.005 },
      1000,
    );
    assert.equal(ok, true);
  });

  it("GEO_AREA computes polygon area", async () => {
    transport.whenFetchval("GEO_AREA", 500000);
    const area = await geo.area([
      { lat: 0, lon: 0 }, { lat: 1, lon: 0 }, { lat: 0, lon: 1 },
    ]);
    assert.equal(area, 500000);
  });

  it("GEO_AREA rejects fewer than 3 points", async () => {
    await assert.rejects(
      () => geo.area([{ lat: 0, lon: 0 }, { lat: 1, lon: 1 }]),
      /at least 3 points/
    );
  });

  it("throws NucleusFeatureError on plain Postgres", async () => {
    const pgGeo = withGeo.init(transport, pgFeatures()).geo;
    await assert.rejects(
      () => pgGeo.distance({ lat: 0, lon: 0 }, { lat: 1, lon: 1 }),
      NucleusFeatureError,
    );
  });
});

// =========================================================================
// 9. Blob Model — SQL Function Interface
// =========================================================================

describe("Integration: Blob model SQL functions", () => {
  let transport: WireTransport;
  let blob: ReturnType<typeof withBlob.init>["blob"];

  beforeEach(() => {
    transport = new WireTransport();
    blob = withBlob.init(transport, nucleusFeatures()).blob;
  });

  it("BLOB_STORE stores binary data", async () => {
    const data = new Uint8Array([0x48, 0x65, 0x6c, 0x6c, 0x6f]);
    await blob.put("images", "logo.png", data, { contentType: "image/png" });
    const calls = transport.sqlCalls("BLOB_STORE");
    assert.equal(calls.length, 1);
    assert.equal(calls[0].params![0], "images/logo.png");
  });

  it("BLOB_GET retrieves binary data", async () => {
    transport.whenFetchval("BLOB_GET", "48656c6c6f");
    transport.whenFetchval("BLOB_META", JSON.stringify({
      key: "images/logo.png", size: 5, content_type: "image/png",
      created_at: "2024-01-01T00:00:00Z",
    }));
    const result = await blob.get("images", "logo.png");
    assert.ok(result !== null);
    assert.equal(result!.data.length, 5);
    assert.deepEqual(Array.from(result!.data), [0x48, 0x65, 0x6c, 0x6c, 0x6f]);
  });

  it("BLOB_DELETE deletes a blob", async () => {
    transport.whenFetchval("BLOB_DELETE", true);
    const ok = await blob.delete("images", "logo.png");
    assert.equal(ok, true);
  });

  it("BLOB_COUNT returns total blobs", async () => {
    transport.whenFetchval("BLOB_COUNT", 150);
    assert.equal(await blob.blobCount(), 150);
  });

  it("BLOB_DEDUP_RATIO returns dedup ratio", async () => {
    transport.whenFetchval("BLOB_DEDUP_RATIO", 0.35);
    assert.equal(await blob.dedupRatio(), 0.35);
  });

  it("throws NucleusFeatureError on plain Postgres", async () => {
    const pgBlob = withBlob.init(transport, pgFeatures()).blob;
    await assert.rejects(() => pgBlob.blobCount(), NucleusFeatureError);
  });
});

// =========================================================================
// 10. PubSub Model — SQL Function Interface
// =========================================================================

describe("Integration: PubSub model SQL functions", () => {
  let transport: WireTransport;
  let pubsub: ReturnType<typeof withPubSub.init>["pubsub"];

  beforeEach(() => {
    transport = new WireTransport();
    pubsub = withPubSub.init(transport, nucleusFeatures()).pubsub;
  });

  it("PUBSUB_PUBLISH publishes a message", async () => {
    transport.whenFetchval("PUBSUB_PUBLISH", 3);
    const count = await pubsub.publish("events", "hello");
    assert.equal(count, 3);
  });

  it("PUBSUB_CHANNELS returns channel list", async () => {
    transport.whenFetchval("PUBSUB_CHANNELS", "events,notifications,alerts");
    const channels = await pubsub.channels();
    assert.equal(channels, "events,notifications,alerts");
  });

  it("PUBSUB_SUBSCRIBERS returns subscriber count", async () => {
    transport.whenFetchval("PUBSUB_SUBSCRIBERS", 5);
    const count = await pubsub.subscribers("events");
    assert.equal(count, 5);
  });

  it("throws NucleusFeatureError on plain Postgres", async () => {
    const pgPubSub = withPubSub.init(transport, pgFeatures()).pubsub;
    await assert.rejects(() => pgPubSub.publish("ch", "msg"), NucleusFeatureError);
  });
});

// =========================================================================
// 11. Streams Model — SQL Function Interface
// =========================================================================

describe("Integration: Streams model SQL functions", () => {
  let transport: WireTransport;
  let streams: ReturnType<typeof withStreams.init>["streams"];

  beforeEach(() => {
    transport = new WireTransport();
    streams = withStreams.init(transport, nucleusFeatures()).streams;
  });

  it("STREAM_XADD appends an entry", async () => {
    transport.whenFetchval("STREAM_XADD", "1704067200000-0");
    const id = await streams.xadd("events", { type: "click", page: "/home" });
    assert.equal(id, "1704067200000-0");
  });

  it("STREAM_XLEN returns entry count", async () => {
    transport.whenFetchval("STREAM_XLEN", 42);
    assert.equal(await streams.xlen("events"), 42);
  });

  it("STREAM_XRANGE returns entries in range", async () => {
    transport.whenFetchval("STREAM_XRANGE", JSON.stringify([
      { id: "100-0", fields: { type: "click" } },
      { id: "200-0", fields: { type: "scroll" } },
    ]));
    const entries = await streams.xrange("events", 0, 300, 10);
    assert.equal(entries.length, 2);
    assert.equal(entries[0].id, "100-0");
  });

  it("STREAM_XGROUP_CREATE creates consumer group", async () => {
    transport.whenFetchval("STREAM_XGROUP_CREATE", true);
    const ok = await streams.xgroupCreate("events", "workers", 0);
    assert.equal(ok, true);
  });

  it("STREAM_XACK acknowledges entry", async () => {
    transport.whenFetchval("STREAM_XACK", true);
    const ok = await streams.xack("events", "workers", 100, 0);
    assert.equal(ok, true);
  });

  it("throws NucleusFeatureError on plain Postgres", async () => {
    const pgStreams = withStreams.init(transport, pgFeatures()).streams;
    await assert.rejects(() => pgStreams.xlen("s"), NucleusFeatureError);
  });
});

// =========================================================================
// 12. Columnar Model — SQL Function Interface
// =========================================================================

describe("Integration: Columnar model SQL functions", () => {
  let transport: WireTransport;
  let columnar: ReturnType<typeof withColumnar.init>["columnar"];

  beforeEach(() => {
    transport = new WireTransport();
    columnar = withColumnar.init(transport, nucleusFeatures()).columnar;
  });

  it("COLUMNAR_INSERT inserts a row", async () => {
    transport.whenFetchval("COLUMNAR_INSERT", true);
    const ok = await columnar.insert("analytics", { page: "/home", views: 100 });
    assert.equal(ok, true);
  });

  it("COLUMNAR_COUNT returns row count", async () => {
    transport.whenFetchval("COLUMNAR_COUNT", 1_000_000);
    assert.equal(await columnar.count("analytics"), 1_000_000);
  });

  it("COLUMNAR_SUM returns sum", async () => {
    transport.whenFetchval("COLUMNAR_SUM", 500_000);
    assert.equal(await columnar.sum("analytics", "views"), 500_000);
  });

  it("COLUMNAR_AVG returns average", async () => {
    transport.whenFetchval("COLUMNAR_AVG", 42.5);
    assert.equal(await columnar.avg("analytics", "views"), 42.5);
  });

  it("validates table name identifier", async () => {
    await assert.rejects(() => columnar.count("bad;table"), /Invalid/);
  });

  it("throws NucleusFeatureError on plain Postgres", async () => {
    const pgCol = withColumnar.init(transport, pgFeatures()).columnar;
    await assert.rejects(() => pgCol.count("t"), NucleusFeatureError);
  });
});

// =========================================================================
// 13. Datalog Model — SQL Function Interface
// =========================================================================

describe("Integration: Datalog model SQL functions", () => {
  let transport: WireTransport;
  let datalog: ReturnType<typeof withDatalog.init>["datalog"];

  beforeEach(() => {
    transport = new WireTransport();
    datalog = withDatalog.init(transport, nucleusFeatures()).datalog;
  });

  it("DATALOG_ASSERT adds a fact", async () => {
    transport.whenFetchval("DATALOG_ASSERT", true);
    const ok = await datalog.assert("parent(alice, bob)");
    assert.equal(ok, true);
  });

  it("DATALOG_RETRACT removes a fact", async () => {
    transport.whenFetchval("DATALOG_RETRACT", true);
    const ok = await datalog.retract("parent(alice, bob)");
    assert.equal(ok, true);
  });

  it("DATALOG_RULE defines a rule", async () => {
    transport.whenFetchval("DATALOG_RULE", true);
    const ok = await datalog.rule("grandparent(X,Z)", "parent(X,Y), parent(Y,Z)");
    assert.equal(ok, true);
  });

  it("DATALOG_QUERY evaluates a pattern", async () => {
    transport.whenFetchval("DATALOG_QUERY", "X=alice,Y=charlie");
    const result = await datalog.query("grandparent(X, Y)?");
    assert.ok(result.includes("alice"));
  });

  it("DATALOG_CLEAR clears knowledge base", async () => {
    transport.whenFetchval("DATALOG_CLEAR", true);
    const ok = await datalog.clear();
    assert.equal(ok, true);
  });

  it("DATALOG_IMPORT_GRAPH imports graph data", async () => {
    transport.whenFetchval("DATALOG_IMPORT_GRAPH", 42);
    const count = await datalog.importGraph();
    assert.equal(count, 42);
  });

  it("throws NucleusFeatureError on plain Postgres", async () => {
    const pgDatalog = withDatalog.init(transport, pgFeatures()).datalog;
    await assert.rejects(() => pgDatalog.query("test?"), NucleusFeatureError);
  });
});

// =========================================================================
// 14. CDC Model — SQL Function Interface
// =========================================================================

describe("Integration: CDC model SQL functions", () => {
  let transport: WireTransport;
  let cdc: ReturnType<typeof withCDC.init>["cdc"];

  beforeEach(() => {
    transport = new WireTransport();
    cdc = withCDC.init(transport, nucleusFeatures()).cdc;
  });

  it("CDC_READ returns events from offset", async () => {
    transport.whenFetchval("CDC_READ", '[{"op":"INSERT","table":"users"}]');
    const raw = await cdc.read(0);
    assert.ok(raw.includes("INSERT"));
  });

  it("CDC_COUNT returns event count", async () => {
    transport.whenFetchval("CDC_COUNT", 500);
    assert.equal(await cdc.count(), 500);
  });

  it("CDC_TABLE_READ returns table-specific events", async () => {
    transport.whenFetchval("CDC_TABLE_READ", '[{"op":"UPDATE"}]');
    const raw = await cdc.tableRead("users", 0);
    assert.ok(raw.includes("UPDATE"));
  });

  it("throws NucleusFeatureError on plain Postgres", async () => {
    const pgCdc = withCDC.init(transport, pgFeatures()).cdc;
    await assert.rejects(() => pgCdc.count(), NucleusFeatureError);
  });
});

// =========================================================================
// 15. Granular Feature Gating
// =========================================================================

describe("Integration: granular feature gating", () => {
  it("allows KV when hasKV=true even if other features disabled", () => {
    const features = nucleusFeatures({ hasVector: false, hasGraph: false });
    assert.doesNotThrow(() => requireNucleus(features, "KV"));
  });

  it("blocks KV when hasKV=false on Nucleus", () => {
    const features = nucleusFeatures({ hasKV: false });
    assert.throws(() => requireNucleus(features, "KV"), NucleusFeatureError);
  });

  it("blocks Vector when hasVector=false on Nucleus", () => {
    const features = nucleusFeatures({ hasVector: false });
    assert.throws(() => requireNucleus(features, "Vector"), NucleusFeatureError);
  });

  it("blocks Graph when hasGraph=false on Nucleus", () => {
    const features = nucleusFeatures({ hasGraph: false });
    assert.throws(() => requireNucleus(features, "Graph"), NucleusFeatureError);
  });

  it("blocks all features on plain Postgres", () => {
    const features = pgFeatures();
    const models = ["KV", "Vector", "TimeSeries", "Document", "Graph", "FTS",
      "Geo", "Blob", "Streams", "Columnar", "Datalog", "CDC", "PubSub"];
    for (const model of models) {
      assert.throws(() => requireNucleus(features, model), NucleusFeatureError);
    }
  });
});

// =========================================================================
// 16. Full Plugin Composition Flow
// =========================================================================

describe("Integration: full plugin composition", () => {
  it("composes all 14 models in a single client", async () => {
    const transport = new WireTransport();
    transport.whenFetchval("SELECT VERSION()", "Nucleus 0.1.0");

    const client = await createClient({ url: "http://localhost:5432", transport })
      .use(withSQL)
      .use(withKV)
      .use(withVector)
      .use(withTimeSeries)
      .use(withDocument)
      .use(withGraph)
      .use(withFTS)
      .use(withGeo)
      .use(withBlob)
      .use(withPubSub)
      .use(withStreams)
      .use(withColumnar)
      .use(withDatalog)
      .use(withCDC)
      .connect();

    // Verify all models are accessible
    assert.equal(typeof client.sql.query, "function");
    assert.equal(typeof client.kv.get, "function");
    assert.equal(typeof client.vector.search, "function");
    assert.equal(typeof client.timeseries.write, "function");
    assert.equal(typeof client.document.insert, "function");
    assert.equal(typeof client.graph.addNode, "function");
    assert.equal(typeof client.fts.search, "function");
    assert.equal(typeof client.geo.distance, "function");
    assert.equal(typeof client.blob.put, "function");
    assert.equal(typeof client.pubsub.publish, "function");
    assert.equal(typeof client.streams.xadd, "function");
    assert.equal(typeof client.columnar.insert, "function");
    assert.equal(typeof client.datalog.assert, "function");
    assert.equal(typeof client.cdc.read, "function");

    await client.close();
  });

  it("SQL plugin works on both Nucleus and plain Postgres", async () => {
    const transport = new WireTransport();
    transport.whenFetchval("SELECT VERSION()", "PostgreSQL 16.2");
    transport.whenQuery("SELECT 1", [{ val: 1 }]);

    const client = await createClient({ url: "http://localhost:5432", transport })
      .use(withSQL)
      .connect();

    const rows = await client.sql.query("SELECT 1 AS val");
    assert.deepEqual(rows, [{ val: 1 }]);
    await client.close();
  });

  it("KV operations fail gracefully on Postgres", async () => {
    const transport = new WireTransport();
    transport.whenFetchval("SELECT VERSION()", "PostgreSQL 16.2");

    const client = await createClient({ url: "http://localhost:5432", transport })
      .use(withSQL)
      .use(withKV)
      .connect();

    // SQL still works
    const sql = client.sql;
    assert.equal(typeof sql.query, "function");

    // KV throws feature error
    await assert.rejects(() => client.kv.get("key"), NucleusFeatureError);
    await client.close();
  });

  it("transactions work through SQL model", async () => {
    const transport = new WireTransport();
    transport.whenFetchval("SELECT VERSION()", "Nucleus 0.1.0");

    const client = await createClient({ url: "http://localhost:5432", transport })
      .use(withSQL)
      .connect();

    const result = await client.sql.transaction(async (tx) => {
      await tx.execute("INSERT INTO users (name) VALUES ($1)", "Alice");
      return "committed";
    });
    assert.equal(result, "committed");

    // Verify beginTransaction was called
    const txCalls = transport.calls.filter((c) => c.method === "beginTransaction");
    assert.equal(txCalls.length, 1);

    await client.close();
  });
});

// =========================================================================
// 17. SQL Model detailed tests
// =========================================================================

describe("Integration: SQL model detailed", () => {
  let transport: WireTransport;
  let sql: ReturnType<typeof withSQL.init>["sql"];

  beforeEach(() => {
    transport = new WireTransport();
    sql = withSQL.init(transport, nucleusFeatures()).sql;
  });

  it("queryOne throws NucleusNotFoundError when no rows", async () => {
    await assert.rejects(() => sql.queryOne("SELECT * FROM missing"), NucleusNotFoundError);
  });

  it("queryOneOrNull returns null when no rows", async () => {
    const row = await sql.queryOneOrNull("SELECT * FROM missing");
    assert.equal(row, null);
  });

  it("executeBatch runs multiple statements", async () => {
    const counts = await sql.executeBatch([
      { sql: "INSERT INTO a VALUES (1)" },
      { sql: "INSERT INTO b VALUES (2)" },
      { sql: "INSERT INTO c VALUES (3)" },
    ]);
    assert.equal(counts.length, 3);
    const execCalls = transport.calls.filter((c) => c.method === "execute");
    assert.equal(execCalls.length, 3);
  });

  it("fetchval returns null when no rows", async () => {
    const val = await sql.fetchval("SELECT missing()");
    assert.equal(val, null);
  });
});
