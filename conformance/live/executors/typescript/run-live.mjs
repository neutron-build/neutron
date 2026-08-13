#!/usr/bin/env node
// TypeScript executor for the Nucleus live data-model conformance spec.
//
// Reads ../../spec.json, runs every case against a live engine through the
// in-repo `@neutron-build/nucleus` client, and prints one JSON result document
// to stdout. It asserts nothing a mock could assert: only that a call reaches
// the engine, is accepted over the wire, and comes back with the right value.
//
//     NEUTRON_TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:55432/postgres \
//         node run-live.mjs
//
// Exit codes: 0 all cases behaved as the spec says, 1 otherwise. An `xfail`
// case that PASSES is a failure — otherwise a fix lands and the note explaining
// why the case is expected to fail quietly becomes a lie.

import { readFileSync, existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { randomBytes } from 'node:crypto';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const SPEC = path.resolve(HERE, '..', '..', 'spec.json');
const UNSUPPORTED = path.join(HERE, 'unsupported.json');
const REPO = path.resolve(HERE, '..', '..', '..', '..');
// The client is loaded from the in-repo build, not from a published package:
// this suite exists to test THIS tree.
const CLIENT = path.join(REPO, 'typescript', 'packages', 'neutron-nucleus', 'dist', 'index.js');
const DATABASE_URL = process.env.NEUTRON_TEST_DATABASE_URL;

// Time-series timestamps in the spec are millisecond offsets from this instant.
// A fixed base keeps the cases deterministic and comparable across SDKs.
const TS_BASE = Date.UTC(2026, 7, 11, 12, 0, 0);

const FIXTURE_RE = /@([A-Za-z_][A-Za-z0-9_]*)/g;

/** The SDK has no surface for this op. */
class Unsupported extends Error {}

// ── argument resolution ─────────────────────────────────────────────────────

function resolve(value, fixtures, bound) {
  if (typeof value === 'string') {
    if (value.startsWith('$')) {
      const name = value.slice(1);
      if (!(name in bound)) throw new Error(`step references $${name} before it was bound`);
      return bound[name];
    }
    return value.replace(FIXTURE_RE, (_m, name) => {
      if (!(name in fixtures)) fixtures[name] = `${name}_${randomBytes(5).toString('hex')}`;
      return fixtures[name];
    });
  }
  if (Array.isArray(value)) return value.map((v) => resolve(v, fixtures, bound));
  if (value !== null && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, resolve(v, fixtures, bound)]));
  }
  return value;
}

// ── expectation vocabulary ──────────────────────────────────────────────────

const isPlainObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);

/** Python truthiness, so `nonEmpty` means the same thing in both executors. */
function truthy(v) {
  if (v === null || v === undefined || v === false) return false;
  if (Array.isArray(v) || typeof v === 'string') return v.length > 0;
  if (v instanceof Map || v instanceof Set) return v.size > 0;
  if (typeof v === 'number') return v !== 0;
  if (isPlainObject(v)) return Object.keys(v).length > 0;
  return Boolean(v);
}

function sizeOf(v) {
  if (v === null || v === undefined) throw new Error(`expected a collection, got ${show(v)}`);
  if (Array.isArray(v) || typeof v === 'string') return v.length;
  if (v instanceof Map || v instanceof Set) return v.size;
  if (isPlainObject(v)) return Object.keys(v).length;
  throw new Error(`expected a collection, got ${show(v)}`);
}

function show(v) {
  if (typeof v === 'string') return JSON.stringify(v);
  if (v === undefined) return 'undefined';
  if (v instanceof Uint8Array) return `bytes(${Buffer.from(v).toString('hex')})`;
  try {
    return JSON.stringify(v);
  } catch {
    return String(v);
  }
}

function deepEquals(a, b) {
  if (a === b) return true;
  if (typeof a === 'number' || typeof b === 'number') {
    if (typeof a === 'number' && typeof b === 'number') return Math.abs(a - b) < 1e-9;
  }
  if (Array.isArray(a) && Array.isArray(b)) {
    return a.length === b.length && a.every((x, i) => deepEquals(x, b[i]));
  }
  if (isPlainObject(a) && isPlainObject(b)) {
    const ka = Object.keys(a);
    const kb = Object.keys(b);
    return ka.length === kb.length && ka.every((k) => k in b && deepEquals(a[k], b[k]));
  }
  return false;
}

const TYPE_CHECKS = {
  list: (v) => Array.isArray(v),
  map: (v) => isPlainObject(v) || v instanceof Map,
  string: (v) => typeof v === 'string',
  int: (v) => typeof v === 'number' && Number.isInteger(v),
  float: (v) => typeof v === 'number',
  bool: (v) => typeof v === 'boolean',
  bytes: (v) => v instanceof Uint8Array,
};

function typeName(v) {
  if (v === null) return 'null';
  if (v === undefined) return 'undefined';
  if (Array.isArray(v)) return 'list';
  if (v instanceof Uint8Array) return 'bytes';
  if (typeof v === 'number') return Number.isInteger(v) ? 'int' : 'float';
  if (isPlainObject(v)) return 'map';
  return typeof v;
}

function check(result, expect) {
  let actual = result;
  if ('key' in expect) {
    if (actual === null || actual === undefined) {
      throw new Error(`expected a map with key ${show(expect.key)}, got ${show(actual)}`);
    }
    actual = actual instanceof Map ? actual.get(expect.key) : actual[expect.key];
  }
  if ('index' in expect) {
    if (actual === null || actual === undefined) {
      throw new Error(`expected a list to index [${expect.index}], got ${show(actual)}`);
    }
    actual = actual[expect.index];
  }
  // "Parse a string result as JSON before comparing." A driver that already
  // decoded the column (node-postgres does this for json/jsonb) has satisfied
  // the intent, so a non-string passes straight through rather than being
  // reported as a JSON syntax error that hides the real value.
  if (expect.jsonDecode && typeof actual === 'string') actual = JSON.parse(actual);

  if (expect.notNull && (actual === null || actual === undefined)) {
    throw new Error('expected a value, got null');
  }
  if (expect.isNull && actual !== null && actual !== undefined) {
    throw new Error(`expected null, got ${show(actual)}`);
  }
  if (expect.nonEmpty && !truthy(actual)) {
    throw new Error(`expected a non-empty collection, got ${show(actual)}`);
  }
  if ('length' in expect) {
    const n = sizeOf(actual);
    if (n !== expect.length) {
      throw new Error(`expected ${expect.length} elements, got ${n}: ${show(actual)}`);
    }
  }
  if ('type' in expect) {
    const fn = TYPE_CHECKS[expect.type];
    if (!fn) throw new Error(`unknown type in spec: ${expect.type}`);
    if (!fn(actual)) {
      throw new Error(`expected ${expect.type}, got ${typeName(actual)}: ${show(actual)}`);
    }
  }
  if ('equals' in expect) {
    const want = expect.equals;
    if (!deepEquals(actual, want)) {
      throw new Error(`expected ${show(want)}, got ${show(actual)}`);
    }
  }
}

// ── op table ────────────────────────────────────────────────────────────────
//
// Maps spec op names onto the TypeScript SDK. One method per op, no cleverness.
// Where the TS client's parameter order or units differ from the spec's
// positional convention, the difference is adapted HERE and nowhere else, so
// the drift stays visible in one place.

class Ops {
  constructor(client, url, sdk) {
    this.c = client;
    this.url = url;
    this.sdk = sdk;
  }

  async call(op, args) {
    const fn = this[op.replace(/\./g, '_')];
    if (typeof fn !== 'function') throw new Unsupported(op);
    try {
      return await fn.apply(this, args);
    } catch (err) {
      // The SDK saying "not supported" IS the absence of a surface.
      if (err && err.name === 'NucleusNotSupportedError') throw new Unsupported(`${op}: ${err.message}`);
      throw err;
    }
  }

  // ── core ─────────────────────────────────────────────────────────────
  async features_isNucleus() {
    return this.c.features.isNucleus;
  }

  async connection_closeAndReconnect() {
    const probe = await this.sdk.createClient({ url: this.url }).use(this.sdk.withSQL).connect();
    await probe.sql.fetchval('SELECT 1');
    // Hung forever before N25: the server ignored Terminate and never closed
    // the socket. The race turns that hang into a failure.
    let timer;
    const timeout = new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error('close() did not return within 15s')), 15_000);
    });
    try {
      await Promise.race([probe.close(), timeout]);
    } finally {
      clearTimeout(timer);
    }
    return true;
  }

  // ── document ─────────────────────────────────────────────────────────
  async document_insert(coll, doc) {
    return this.c.document.insert(coll, doc);
  }

  async document_get(docId) {
    return this.c.document.get(Number(docId));
  }

  async document_getIn(coll, docId) {
    return this.c.document.getIn(coll, Number(docId));
  }

  // The TS client spells `get_path_in` as `pathIn`.
  async document_getPathIn(coll, docId, ...keys) {
    return this.c.document.pathIn(coll, Number(docId), ...keys);
  }

  async document_update(coll, filter, patch) {
    return this.c.document.update(coll, filter, patch);
  }

  async document_delete(coll, filter) {
    return this.c.document.delete(coll, filter);
  }

  async document_countIn(coll) {
    return this.c.document.countIn(coll);
  }

  async document_find(coll, filter) {
    return this.c.document.find(coll, filter);
  }

  async document_findOne(coll, filter) {
    return this.c.document.findOne(coll, filter);
  }

  // ── graph ────────────────────────────────────────────────────────────
  async graph_addNode(labels, props) {
    return this.c.graph.addNode(labels, props);
  }

  // Spec order is (edgeType, from, to); the TS client takes (from, to, edgeType).
  async graph_addEdge(edgeType, a, b) {
    return this.c.graph.addEdge(Number(a), Number(b), edgeType);
  }

  // The TS client takes (nodeId, edgeType?, direction).
  async graph_neighbors(nodeId, direction) {
    return this.c.graph.neighbors(Number(nodeId), undefined, direction);
  }

  async graph_shortestPath(a, b) {
    return this.c.graph.shortestPath(Number(a), Number(b));
  }

  async graph_nodeCount() {
    return this.c.graph.nodeCount();
  }

  async graph_edgeCount() {
    return this.c.graph.edgeCount();
  }

  async graph_deleteNode(nodeId) {
    return this.c.graph.deleteNode(Number(nodeId));
  }

  // ── key/value ────────────────────────────────────────────────────────
  async kv_set(key, value) {
    return this.c.kv.set(key, value);
  }
  async kv_get(key) {
    return this.c.kv.get(key);
  }
  async kv_exists(key) {
    return this.c.kv.exists(key);
  }
  async kv_delete(key) {
    return this.c.kv.delete(key);
  }
  async kv_expire(key, ttl) {
    return this.c.kv.expire(key, ttl);
  }
  async kv_ttl(key) {
    return this.c.kv.ttl(key);
  }
  async kv_incr(key, delta) {
    return this.c.kv.incr(key, delta);
  }
  async kv_rpush(key, value) {
    return this.c.kv.rpush(key, value);
  }
  async kv_llen(key) {
    return this.c.kv.llen(key);
  }
  async kv_lrange(key, start, stop) {
    return this.c.kv.lrange(key, start, stop);
  }
  async kv_lindex(key, index) {
    return this.c.kv.lindex(key, index);
  }
  async kv_zadd(key, score, member) {
    return this.c.kv.zadd(key, score, member);
  }
  async kv_zrange(key, start, stop) {
    return this.c.kv.zrange(key, start, stop);
  }
  async kv_hset(key, field, value) {
    return this.c.kv.hset(key, field, value);
  }
  async kv_hget(key, field) {
    return this.c.kv.hget(key, field);
  }
  async kv_hexists(key, field) {
    return this.c.kv.hexists(key, field);
  }
  async kv_hgetall(key) {
    return this.c.kv.hgetall(key);
  }
  async kv_hlen(key) {
    return this.c.kv.hlen(key);
  }
  async kv_hdel(key, field) {
    return this.c.kv.hdel(key, field);
  }
  async kv_sadd(key, member) {
    return this.c.kv.sadd(key, member);
  }
  async kv_srem(key, member) {
    return this.c.kv.srem(key, member);
  }
  async kv_smembers(key) {
    return this.c.kv.smembers(key);
  }

  // ── time series ──────────────────────────────────────────────────────
  async timeseries_write(measurement, points) {
    return this.c.timeseries.write(
      measurement,
      points.map((p) => ({ timestamp: new Date(TS_BASE + p.t), value: Number(p.v) })),
    );
  }

  async timeseries_count(measurement) {
    return this.c.timeseries.count(measurement);
  }

  async timeseries_last(measurement) {
    return this.c.timeseries.last(measurement);
  }

  async timeseries_query(measurement, startMs, endMs) {
    return this.c.timeseries.query(measurement, new Date(TS_BASE + startMs), new Date(TS_BASE + endMs));
  }

  // The TS client takes a NAMED bucket interval, not a width in milliseconds,
  // so a window it has no name for cannot be requested at all.
  async timeseries_aggregate(measurement, startMs, endMs, windowMs) {
    const NAMED_MS = {
      second: 1_000,
      minute: 60_000,
      hour: 3_600_000,
      day: 86_400_000,
      week: 604_800_000,
      month: 2_592_000_000,
    };
    const interval = Object.keys(NAMED_MS).find((k) => NAMED_MS[k] === windowMs);
    if (!interval) {
      throw new Unsupported(
        `timeseries.aggregate: TimeSeriesModel.aggregate takes a named interval ` +
          `(${Object.keys(NAMED_MS).join('/')}); a ${windowMs}ms window cannot be expressed`,
      );
    }
    return this.c.timeseries.aggregate(
      measurement,
      new Date(TS_BASE + startMs),
      new Date(TS_BASE + endMs),
      interval,
      'avg',
    );
  }

  // ── streams ──────────────────────────────────────────────────────────
  async streams_xadd(stream, fields) {
    return this.c.streams.xadd(stream, fields);
  }
  async streams_xlen(stream) {
    return this.c.streams.xlen(stream);
  }
  async streams_xrange(stream, start, end, count) {
    return this.c.streams.xrange(stream, start, end, count);
  }
  async streams_xread(stream, after, count) {
    return this.c.streams.xread(stream, after, count);
  }
  async streams_xgroupCreate(stream, group, start) {
    return this.c.streams.xgroupCreate(stream, group, start);
  }
  async streams_xreadgroup(stream, group, consumer, count) {
    return this.c.streams.xreadGroup(stream, group, consumer, count);
  }
  // xadd returns one 'ms-seq' string; xack's signature is (stream, group,
  // idMs, idSeq). The natural round trip is made verbatim — whether the two
  // halves compose is the assertion.
  async streams_xack(stream, group, entryId) {
    return this.c.streams.xack(stream, group, entryId);
  }

  // ── blobs ────────────────────────────────────────────────────────────
  async blob_put(bucket, key, payloadB64) {
    return this.c.blob.put(bucket, key, new Uint8Array(Buffer.from(payloadB64, 'base64')));
  }

  async blob_get(bucket, key) {
    const out = await this.c.blob.get(bucket, key);
    if (out === null || out === undefined) return null;
    return Buffer.from(out.data).toString('base64');
  }

  async blob_getMeta(bucket, key) {
    return this.c.blob.meta(bucket, key);
  }

  async blob_exists(bucket, key) {
    return this.c.blob.exists(bucket, key);
  }

  async blob_delete(bucket, key) {
    return this.c.blob.delete(bucket, key);
  }

  // ── cdc ──────────────────────────────────────────────────────────────
  async cdc_read(afterSeq, limit) {
    return this.c.cdc.read(afterSeq, limit);
  }
  async cdc_count() {
    return this.c.cdc.count();
  }

  // ── datalog ──────────────────────────────────────────────────────────
  async datalog_assertFact(fact) {
    return this.c.datalog.assert(fact);
  }
  async datalog_query(query) {
    return this.c.datalog.query(query);
  }
  // The TS client's clear() takes no predicate — it clears the whole KB.
  async datalog_clear(_predicate) {
    return this.c.datalog.clear();
  }

  // ── full-text search ─────────────────────────────────────────────────
  // The engine keeps ONE global index; both clients drop the index name and
  // join the field values into a single text blob.
  async fts_indexDoc(_index, docId, fields) {
    return this.c.fts.index(Number(docId), Object.values(fields).join(' '));
  }

  async fts_search(_index, query, limit) {
    return this.c.fts.search(query, { limit });
  }

  // ── vector ───────────────────────────────────────────────────────────
  async vector_createCollection(coll, dim) {
    return this.c.vector.createCollection(coll, dim);
  }

  async vector_insert(coll, vecId, values) {
    return this.c.vector.insert(coll, vecId, values);
  }

  async vector_search(coll, values, k) {
    return this.c.vector.search(coll, values, { limit: k });
  }

  // ── raw sql ──────────────────────────────────────────────────────────
  async sql_queryScalar(query, params) {
    return this.c.sql.fetchval(query, ...params);
  }

  async sql_execute(query, params) {
    return this.c.sql.execute(query, ...params);
  }

  async sql_begin() {
    return this.c.sql.execute('BEGIN');
  }

  async sql_rollback() {
    return this.c.sql.execute('ROLLBACK');
  }
}

// ── runner ──────────────────────────────────────────────────────────────────

async function runCase(kase, client, url, sdk) {
  const fixtures = {};
  const bound = {};
  const ops = new Ops(client, url, sdk);

  for (const [i, step] of kase.steps.entries()) {
    const args = resolve(step.args ?? [], fixtures, bound);
    const result = await ops.call(step.op, args);
    if ('bind' in step) bound[step.bind] = result;
    if ('expect' in step) {
      try {
        check(result, step.expect);
      } catch (err) {
        throw new Error(`step ${i} (${step.op}): ${err.message}`);
      }
    }
  }
}

async function main() {
  if (!DATABASE_URL) {
    process.stderr.write(
      '::error::NEUTRON_TEST_DATABASE_URL is not set. This suite is only ' +
        'meaningful against a live engine; refusing to report a green run ' +
        'for zero executed cases.\n',
    );
    return 1;
  }
  if (!existsSync(CLIENT)) {
    process.stderr.write(
      `::error::no built client at ${CLIENT} — run ` +
        '`pnpm --filter @neutron-build/nucleus build` first.\n',
    );
    return 1;
  }

  const spec = JSON.parse(readFileSync(SPEC, 'utf8'));
  const declaredUnsupported = existsSync(UNSUPPORTED)
    ? (JSON.parse(readFileSync(UNSUPPORTED, 'utf8')).cases ?? {})
    : {};

  const sdk = await import(pathToFileURL(CLIENT).href);
  const client = await sdk
    .createClient({ url: DATABASE_URL })
    .use(sdk.withSQL)
    .use(sdk.withKV)
    .use(sdk.withDocument)
    .use(sdk.withGraph)
    .use(sdk.withTimeSeries)
    .use(sdk.withStreams)
    .use(sdk.withBlob)
    .use(sdk.withCDC)
    .use(sdk.withDatalog)
    .use(sdk.withFTS)
    .use(sdk.withVector)
    .connect();

  const results = [];
  try {
    for (const kase of spec.cases) {
      const entry = { id: kase.id, model: kase.model };
      const expectedFail = 'xfail' in kase;
      try {
        await runCase(kase, client, DATABASE_URL, sdk);
        entry.status = expectedFail ? 'xpass' : 'pass';
        if (expectedFail) {
          entry.detail =
            'case is marked xfail but passed — the underlying bug is fixed and ' +
            'the xfail note is now false';
        }
      } catch (err) {
        if (err instanceof Unsupported) {
          const reason = declaredUnsupported[kase.id];
          entry.status = reason ? 'unsupported' : 'fail';
          entry.detail =
            reason ??
            `op ${err.message} has no mapping and the case is not declared unsupported in unsupported.json`;
        } else {
          entry.status = expectedFail ? 'xfail' : 'fail';
          entry.detail = `${err?.name ?? 'Error'}: ${err?.message ?? String(err)}`;
        }
      }
      results.push(entry);
    }
  } finally {
    await client.close();
  }

  const doc = { sdk: 'typescript', specVersion: spec.specVersion, cases: results };
  process.stdout.write(JSON.stringify(doc, null, 2) + '\n');

  const bad = results.filter((r) => r.status === 'fail' || r.status === 'xpass');
  for (const r of bad) process.stderr.write(`::error::${r.id}: ${r.status} — ${r.detail ?? ''}\n`);
  const counts = {};
  for (const r of results) counts[r.status] = (counts[r.status] ?? 0) + 1;
  process.stderr.write(`typescript: ${JSON.stringify(counts)}\n`);
  return bad.length ? 1 : 0;
}

process.exitCode = await main();
