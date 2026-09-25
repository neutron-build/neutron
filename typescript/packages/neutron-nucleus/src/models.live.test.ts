// ---------------------------------------------------------------------------
// @neutron-build/nucleus — X04 live model battery (KV / Blob / Geo)
//
// V18 evidence for orm-program card X04. Runs ONLY against a real Nucleus
// engine (NEUTRON_TEST_DATABASE_URL must point at one; a plain PostgreSQL
// URL runs only the PostGIS-detection leg and skips the model bodies —
// KV_*/BLOB_*/GEO_* do not exist there). No mocked transports on this path:
// every assertion is against the engine's actual answers.
//
// Restart durability is driven in phases by the orchestrator:
//   X04_RESTART_PHASE=pre   — seed marker state + manifest
//   (engine stop + start on the same data dir)
//   X04_RESTART_PHASE=post  — verify marker state survived
// The default (unset) runs the main battery.
// ---------------------------------------------------------------------------

import assert from "node:assert/strict";
import { describe, it, before, after } from "node:test";
import type { TestContext } from "node:test";
import { randomBytes } from "node:crypto";
import { writeFileSync, readFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { PgTransport } from "./transport.js";
import { detectFeatures, detectPostGIS } from "./features.js";
import { withKV } from "./kv/index.js";
import type { KVModel } from "./kv/index.js";
import { withBlob } from "./blob/index.js";
import type { BlobModel } from "./blob/index.js";
import { withGeo } from "./geo/index.js";
import type { GeoModel } from "./geo/index.js";
import type { Transport } from "./types.js";

const url = process.env.NEUTRON_TEST_DATABASE_URL ?? "";
// KV collections cannot be KV_DELeted (separate engine map) — keys unique
// per run keep the battery idempotent against a persistent engine.
const RUN = process.env.X04_RUN_ID ?? Date.now().toString(36);
const PHASE = process.env.X04_RESTART_PHASE ?? "main";
const MANIFEST = join(tmpdir(), "x04-restart-state.json");

function live(): boolean {
  return url !== "";
}

// Hand oracles — written from the formulas (haversine R=6,371,000 m,
// shoelace over (lon,lat)), NOT from engine code.
const R_EARTH = 6_371_000;
function haversine(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const toRad = (d: number) => (d * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const h =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * R_EARTH * Math.asin(Math.sqrt(h));
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Transport wrapper that aborts `ac` right after the first BLOB_TAG passes
 *  through — a test-side observer for deterministic mid-tag-loop cancellation.
 *  Every statement still executes on the real engine. */
class AbortAfterFirstTagTransport {
  fired = false;
  constructor(
    private readonly inner: Transport,
    private readonly ac: AbortController,
  ) {}
  async query(sql: string, params?: unknown[], opts?: { signal?: AbortSignal }) {
    return this.inner.query(sql, params, opts);
  }
  async execute(sql: string, params?: unknown[], opts?: { signal?: AbortSignal }) {
    if (sql.startsWith("SELECT BLOB_TAG") && !this.fired) {
      this.fired = true;
      this.ac.abort();
    }
    return this.inner.execute(sql, params, opts);
  }
  async fetchval(sql: string, params?: unknown[], opts?: { signal?: AbortSignal }) {
    if (sql.startsWith("SELECT BLOB_TAG") && !this.fired) {
      this.fired = true;
      this.ac.abort();
    }
    return this.inner.fetchval(sql, params, opts);
  }
  async ping() { return this.inner.ping(); }
  async close() { return this.inner.close(); }
  async beginTransaction() { throw new Error("not used"); }
}

async function isNucleusUrl(t: Transport): Promise<boolean> {
  const feats = await detectFeatures(t);
  return feats.isNucleus;
}

const mainGate = { skip: !live() && "NEUTRON_TEST_DATABASE_URL not set" };

describe("X04 live battery (real Nucleus engine)", mainGate, () => {
  let t: PgTransport;
  let nucleus = false;

  before(async () => {
    t = new PgTransport(url);
    if (live()) nucleus = await isNucleusUrl(t);
  });

  after(async () => {
    if (live()) await t.close().catch(() => {});
  });

  // Always runs (works on PG too): honest PostGIS detection.
  it("detectPostGIS reports the connected server's real state", async () => {
    const status = await detectPostGIS(t);
    if (!nucleus) {
      // Local dev PG 17.11 has no PostGIS package — expected 'absent'.
      console.log(`[x04] PostGIS on control server: ${JSON.stringify(status)}`);
      assert.ok(["absent", "available", "installed", "unknown"].includes(status.state));
    } else {
      // Nucleus: catalog tables may not exist; the probe must answer SOMETHING
      // honest rather than throw.
      console.log(`[x04] PostGIS on Nucleus: ${JSON.stringify(status)}`);
      assert.ok(["absent", "available", "installed", "unknown"].includes(status.state));
    }
  });

  // Runtime skip: whether the URL points at Nucleus is learned async in
  // before(), so describe-level options (evaluated at registration) cannot
  // see it. Each model test skips itself through the context instead.
  function requireEngine(t: TestContext): void {
    if (!nucleus) t.skip("engine is not Nucleus — KV/Blob/Geo functions absent");
  }

  describe("model bodies (require Nucleus)", () => {
    let kv: KVModel;
    let blob: BlobModel;
    let geo: GeoModel;

    before(async () => {
      const feats = await detectFeatures(t);
      kv = withKV.init(t, feats).kv;
      blob = withBlob.init(t, feats).blob;
      geo = withGeo.init(t, feats).geo;
      // Shared permission fixture: an RLS-active table + a non-superuser role.
      await t.execute("DROP TABLE IF EXISTS x04_rls_guard").catch(() => {});
      await t.execute("CREATE TABLE x04_rls_guard (id INT)");
      await t.execute("ALTER TABLE x04_rls_guard ENABLE ROW LEVEL SECURITY");
      await t.execute("CREATE ROLE x04_rls_probe LOGIN").catch(() => {});
    });

    // The before() fixtures leak on cluster-shared servers (X04 MINOR-1: the
    // role survived every battery run on the shared PG). Teardown drops all
    // battery-owned objects loudly-but-best-effort: a teardown failure is
    // logged, never silent, and never fails the suite.
    after(async () => {
      if (!live()) return;
      // Battery-owned objects only. x04_geo_restart is deliberately absent:
      // it must survive pre -> restart -> post (both TZ post runs re-verify
      // it) and it only ever exists on the disposable engine data dir.
      const stmts = [
        "DROP TABLE IF EXISTS x04_rls_guard",
        "DROP TABLE IF EXISTS x04_geo_pts",
        "DROP TABLE IF EXISTS x04_geo_rls",
        "DROP ROLE IF EXISTS x04_rls_probe",
      ];
      for (const stmt of stmts) {
        await t.execute(stmt).catch((err: unknown) => {
          console.error(`[x04] battery teardown failed (${stmt}):`, err instanceof Error ? err.message : err);
        });
      }
    });

    // -----------------------------------------------------------------
    // KV
    // -----------------------------------------------------------------

    it("kv: string/typed round-trips incl. empty, quotes, unicode", async (ctx) => {
      requireEngine(ctx);
      const k = "x04_rt";
      await kv.set(k, "plain");
      assert.equal(await kv.get(k), "plain");

      await kv.set(`${k}:empty`, "");
      assert.equal(await kv.get(`${k}:empty`), "");

      await kv.set(`${k}:q`, "it's a 'quoted' \"string\"");
      assert.equal(await kv.get(`${k}:q`), "it's a 'quoted' \"string\"");

      await kv.set(`${k}:uni`, "héllo → 世界 🌍");
      assert.equal(await kv.get(`${k}:uni`), "héllo → 世界 🌍");

      const obj = { n: 1, s: "x", arr: [1, 2, 3], nested: { b: true, nil: null } };
      await kv.setTyped(`${k}:json`, obj);
      assert.deepEqual(await kv.getTyped<typeof obj>(`${k}:json`), obj);
    });

    it("kv: incr integer semantics + non-integer error", async (ctx) => {
      requireEngine(ctx);
      const k = "x04_incr";
      await kv.delete(k);
      assert.equal(await kv.incr(k), 1);
      assert.equal(await kv.incr(k, 10), 11);
      assert.equal(await kv.incr(k, -3), 8);

      await kv.set(`${k}:s`, "5");
      assert.equal(await kv.incr(`${k}:s`), 6); // integer-formatted string parses

      await kv.set(`${k}:bad`, "not-a-number");
      await assert.rejects(kv.incr(`${k}:bad`));
    });

    it("kv: collections smoke (list/hash/set/zset)", async (ctx) => {
      requireEngine(ctx);
      const list = `x04_list_${RUN}`;
      const hash = `x04_hash_${RUN}`;
      const set = `x04_set_${RUN}`;
      const zset = `x04_zset_${RUN}`;
      await kv.rpush(list, "a");
      await kv.rpush(list, "b");
      await kv.lpush(list, "first");
      assert.deepEqual(await kv.lrange(list, 0, -1), ["first", "a", "b"]);
      assert.equal(await kv.llen(list), 3);
      assert.equal(await kv.lindex(list, 1), "a");

      await kv.hset(hash, "f1", "v1");
      await kv.hset(hash, "f2", "v2");
      assert.deepEqual(await kv.hgetall(hash), { f1: "v1", f2: "v2" });

      await kv.sadd(set, "m1");
      await kv.sadd(set, "m2");
      assert.deepEqual(await kv.smembers(set), ["m1", "m2"]);

      await kv.zadd(zset, 3, "three");
      await kv.zadd(zset, 1, "one");
      assert.deepEqual(await kv.zrange(zset, 0, -1), ["one", "three"]);
      assert.deepEqual(await kv.zrangeByScore(zset, 2, 5), ["three"]);
    });

    it("kv: TTL lifecycle — present, truncating ttl(), gone at expiry", async (ctx) => {
      requireEngine(ctx);
      const k = "x04_ttl";
      await kv.set(k, "expiring", { ttl: 2 });
      assert.equal(await kv.get(k), "expiring"); // not born-expired
      const t1 = await kv.ttl(k);
      assert.ok(t1 === 2 || t1 === 1, `ttl right after set should read 2 or 1 (truncation), got ${t1}`);

      await sleep(2300);
      assert.equal(await kv.get(k), null);
      assert.equal(await kv.ttl(k), -2);
      assert.equal(await kv.exists(k), false);
      const scanned = await kv.scan("x04_ttl*");
      assert.equal(scanned.includes(k), false);
    });

    it("kv: ttl()=0 does NOT mean expired (truncation boundary)", async (ctx) => {
      requireEngine(ctx);
      const k = "x04_ttl_zero";
      await kv.set(k, "still-here", { ttl: 1 });
      // remaining 0.999s truncates to 0 — the key must still be readable.
      const remaining = await kv.ttl(k);
      assert.ok(remaining === 1 || remaining === 0, `got ${remaining}`);
      assert.equal(await kv.get(k), "still-here");
      await sleep(1200);
      assert.equal(await kv.get(k), null);
    });

    it("kv: expire() adds a TTL; re-set without ttl clears it", async (ctx) => {
      requireEngine(ctx);
      const k = "x04_expire";
      await kv.set(k, "v");
      assert.equal(await kv.ttl(k), -1);
      assert.equal(await kv.expire(k, 30), true);
      const tt = await kv.ttl(k);
      assert.ok(tt === 30 || tt === 29, `got ${tt}`);
      await kv.set(k, "v2"); // no ttl -> clears
      assert.equal(await kv.ttl(k), -1);
    });

    it("kv: setNX+ttl lock cycle incl. re-acquire after expiry", async (ctx) => {
      requireEngine(ctx);
      const k = "x04_lock";
      await kv.delete(k);
      assert.equal(await kv.setNX(k, "holder-1", { ttl: 1 }), true);
      assert.equal(await kv.setNX(k, "holder-2"), false);
      await sleep(1100);
      assert.equal(await kv.setNX(k, "holder-3", { ttl: 1 }), true, "lock must be re-acquirable after expiry");
      // safe release: only the holder deletes
      assert.equal(await kv.cdel(k, "holder-2"), false);
      assert.equal(await kv.cdel(k, "holder-3"), true);
    });

    it("kv: cexpire renews only for the current holder", async (ctx) => {
      requireEngine(ctx);
      const k = "x04_lease";
      await kv.set(k, "me", { ttl: 1 });
      assert.equal(await kv.cexpire(k, "someone-else", 30), false);
      assert.equal(await kv.cexpire(k, "me", 30), true);
      const tt = await kv.ttl(k);
      assert.ok(tt === 30 || tt === 29, `renewed ttl, got ${tt}`);
      await kv.delete(k);
    });

    it("kv: namespace isolation live", async (ctx) => {
      requireEngine(ctx);
      const ns = kv.namespace("sess", { schema: "public", table: "x04_users" });
      await ns.set("theme", "dark", { ttl: 60 });
      assert.equal(await ns.get("theme"), "dark");
      assert.equal(await kv.get("theme"), null, "bare key must not see the scoped key");
      const scoped = await kv.scan("*");
      assert.ok(scoped.includes("public.x04_users:sess:theme"), JSON.stringify(scoped.slice(0, 20)));
      const inScope = await ns.scan("*");
      assert.deepEqual(inScope, ["theme"]);
      await ns.delete("theme");
    });

    it("kv+blob: RLS-active non-superuser is denied by the engine guard", async (ctx) => {
      requireEngine(ctx);
      // Dedicated transport so SET ROLE cannot poison the shared pool.
      // Engine specifics discovered while writing this: SET ROLE inside an
      // explicit transaction ABORTS the transaction on this build, and the
      // aborted state outlives ROLLBACK on a pooled client. So the guard is
      // exercised the way psql does it: one raw pinned connection, autocommit
      // SET ROLE, probe, RESET ROLE, disconnect.
      const pgMod = (await import("pg")).default;
      for (const denied of [
        "SELECT KV_GET($1)",
        "SELECT KV_SET($1, $2)",
        "SELECT BLOB_GET($1)",
        "SELECT BLOB_STORE($1, $2)",
      ]) {
        const client = new pgMod.Client({ connectionString: url });
        await client.connect();
        try {
          await client.query("SET ROLE x04_rls_probe");
          await assert.rejects(
            client.query(denied, denied.endsWith("$2)") ? ["x04", "v"] : ["x04"]),
            /row-level security/i,
          );
          await client.query("RESET ROLE");
        } finally {
          await client.end();
        }
      }
    });

    it("kv: pre-flight abort rejects without sending (real PgTransport)", async (ctx) => {
      requireEngine(ctx);
      const ac = new AbortController();
      ac.abort();
      await assert.rejects(kv.set("x04_never", "v", { signal: ac.signal }));
      assert.equal(await kv.exists("x04_never"), false);
    });

    // -----------------------------------------------------------------
    // Blob
    // -----------------------------------------------------------------

    it("blob: byte-exact round-trip incl. every byte value and 1 MiB random", async (ctx) => {
      requireEngine(ctx);
      const all = new Uint8Array(256).map((_, i) => i);
      await blob.put("x04b", "all256", all, { contentType: "application/x-test" });
      const got = await blob.get("x04b", "all256");
      assert.deepEqual([...got!.data], [...all]);
      assert.equal(got!.meta!.contentType, "application/x-test");
      assert.equal(got!.meta!.size, 256);

      const big = randomBytes(1024 * 1024);
      await blob.put("x04b", "mib", big);
      const gotBig = await blob.get("x04b", "mib");
      assert.deepEqual([...gotBig!.data], [...big]);
    });

    it("blob: empty blob round-trips as empty (not missing)", async (ctx) => {
      requireEngine(ctx);
      await blob.put("x04b", "empty", new Uint8Array(0), { contentType: "text/empty" });
      const got = await blob.get("x04b", "empty");
      assert.notEqual(got, null);
      assert.equal(got!.data.byteLength, 0);
      assert.equal(got!.meta!.size, 0);
      assert.equal(got!.meta!.contentType, "text/empty");
    });

    it("blob: ranges are byte-exact incl. EOF-adjacent and clamped", async (ctx) => {
      requireEngine(ctx);
      const data = new Uint8Array(512).map((_, i) => (i * 31) % 256);
      await blob.put("x04b", "range", data);

      const first = await blob.getRange("x04b", "range", 0, 1);
      assert.deepEqual([...first!.data], [data[0]]);

      const last = await blob.getRange("x04b", "range", 511, 1);
      assert.deepEqual([...last!.data], [data[511]]);

      const eofAdjacent = await blob.getRange("x04b", "range", 512, 10);
      assert.equal(eofAdjacent!.data.byteLength, 0);
      assert.equal(eofAdjacent!.total, 512);

      const clamped = await blob.getRange("x04b", "range", 500, 100);
      assert.deepEqual([...clamped!.data], [...data.slice(500)]);

      const zero = await blob.getRange("x04b", "range", 5, 0);
      assert.equal(zero!.data.byteLength, 0);
      assert.equal(zero!.total, 512);

      const mid = await blob.getRange("x04b", "range", 100, 50);
      assert.deepEqual([...mid!.data], [...data.slice(100, 150)]);

      await assert.rejects(blob.getRange("x04b", "range", 513, 1), /range not satisfiable/);
      assert.equal(await blob.getRange("x04b", "missing", 0, 1), null);
    });

    it("blob: openRead chunks concatenate to the source", async (ctx) => {
      requireEngine(ctx);
      const data = randomBytes(1000);
      await blob.put("x04b", "stream", data);
      const parts: Uint8Array[] = [];
      for await (const c of blob.openRead("x04b", "stream", { chunkBytes: 64 })) parts.push(c);
      const joined = new Uint8Array(1000);
      let at = 0;
      for (const p of parts) { joined.set(p, at); at += p.byteLength; }
      assert.deepEqual([...joined], [...data]);
      assert.equal(parts.length, 16);
      await assert.rejects(async () => {
        for await (const _ of blob.openRead("x04b", "nope")) void _;
      }, /not found/);
    });

    it("blob: putStream stores; abort mid-accumulation stores nothing", async (ctx) => {
      requireEngine(ctx);
      await blob.delete("x04b", "ps").catch(() => {});
      await blob.delete("x04b", "ps2").catch(() => {});
      const before = await blob.blobCount();
      async function* src(): AsyncIterable<Uint8Array> {
        yield new Uint8Array([1, 2, 3]);
        yield randomBytes(10);
      }
      const res = await blob.putStream("x04b", "ps", src(), { contentType: "application/x-test" });
      assert.equal(res.size, 13);
      assert.equal(await blob.blobCount(), before + 1);

      const ac = new AbortController();
      async function* aborting(): AsyncIterable<Uint8Array> {
        yield new Uint8Array([9]);
        ac.abort();
        yield new Uint8Array([9, 9]);
      }
      await assert.rejects(blob.putStream("x04b", "ps2", aborting(), { signal: ac.signal }), /aborted/);
      assert.equal(await blob.blobCount(), before + 1);
      assert.equal(await blob.get("x04b", "ps2"), null);
    });

    it("blob: abort during the metadata tag loop leaves NO partial blob", async (ctx) => {
      requireEngine(ctx);
      const feats = await detectFeatures(t);
      const ac = new AbortController();
      const watcher = new AbortAfterFirstTagTransport(t, ac);
      const blobWatched = withBlob.init(watcher as unknown as Transport, feats).blob;

      const before = await blob.blobCount();
      await assert.rejects(
        blobWatched.put("x04b", "tagabort", new Uint8Array([1, 2, 3]), {
          contentType: "text/plain",
          metadata: { a: "1", b: "2" },
          signal: ac.signal,
        }),
        /aborted/,
      );
      assert.equal(await blob.get("x04b", "tagabort"), null, "canceled put must leave no blob");
      assert.equal(await blob.blobCount(), before);
    });

    it("blob: identity-bound bucket + list scoping", async (ctx) => {
      requireEngine(ctx);
      const b = blob.bucket("uploads", { schema: "public", table: "x04_users" });
      await b.put("f1", new Uint8Array([1]), { contentType: "a/b" });
      assert.deepEqual(await b.list(""), ["f1"]);
      assert.deepEqual(await blob.list("uploads", ""), []); // scope is part of the key
      assert.equal((await b.get("f1"))!.meta!.contentType, "a/b");
      await b.delete("f1");
    });

    // -----------------------------------------------------------------
    // Geo
    // -----------------------------------------------------------------

    it("geo: predicates match hand oracles (haversine, inclusive within)", async (ctx) => {
      requireEngine(ctx);
      const SF = { lat: 37.7749, lon: -122.4194 };
      const LA = { lat: 34.0522, lon: -118.2437 };
      const d = await geo.distance(SF, LA);
      const oracle = haversine(SF.lat, SF.lon, LA.lat, LA.lon);
      assert.ok(Math.abs(d - oracle) / oracle < 1e-12, `engine ${d} vs oracle ${oracle}`);
      assert.ok(d > 559_000 && d < 560_000);
      assert.equal(await geo.distance(SF, SF), 0);

      const oneDeg = haversine(0, 0, 0, 1);
      assert.equal(await geo.within({ lat: 0, lon: 0 }, { lat: 0, lon: 1 }, oneDeg), true);
      assert.equal(await geo.within({ lat: 0, lon: 0 }, { lat: 0, lon: 1 }, oneDeg * 0.999), false);

      const area = await geo.area([
        { lat: 0, lon: 0 }, { lat: 0, lon: 4 }, { lat: 3, lon: 4 }, { lat: 3, lon: 0 },
      ]);
      assert.ok(Math.abs(area - 12) < 1e-9, `square degrees, got ${area}`);

      const square: [number, number][] = [[0, 0], [0, 4], [4, 4], [4, 0]];
      assert.equal(await geo.containsPoint(square, { lat: 2, lon: 2 }), true);
      assert.equal(await geo.containsPoint(square, { lat: 0, lon: 2 }), false, "edge midpoint is boundary");
      assert.equal(await geo.containsPoint(square, { lat: 0, lon: 0 }), false, "vertex is boundary");
      assert.equal(await geo.containsPoint(square, { lat: -1, lon: 2 }), false);
    });

    it("geo: layer journey — table, inserts, nearest/bbox/polygon with exact membership", async (ctx) => {
      requireEngine(ctx);
      await t.execute("DROP TABLE IF EXISTS x04_geo_pts");
      await t.execute(
        "CREATE TABLE x04_geo_pts (id BIGINT GENERATED ALWAYS AS IDENTITY, lat DOUBLE PRECISION, lon DOUBLE PRECISION, properties TEXT)",
      );

      const bound = geo.layer({ schema: "public", table: "x04_geo_pts" });
      await bound.insert(0.25, 0.25, { name: "inner" });     // ~39 km from origin
      await bound.insert(1.0, 1.0, { name: "ring" });        // ~157 km
      await bound.insert(10.0, 10.0, { name: "far" });       // far outside
      await bound.insert(1.0, 0.0, { name: "edge" });        // ON the bbox lat boundary and the square's lon edge

      // nearestTo: membership + order + distance property
      // ("edge" at (1,0) is ~111 km — inside the 200 km radius, between
      // "inner" ~39 km and "ring" ~157 km.)
      const near = await bound.nearestTo({ lat: 0, lon: 0 }, 200_000, 10);
      const names = near.map((f) => f.properties.name);
      assert.deepEqual(names, ["inner", "edge", "ring"]);
      const oracleInner = haversine(0, 0, 0.25, 0.25);
      const distInner = Number(near[0].properties.distance);
      assert.ok(Math.abs(distInner - oracleInner) / oracleInner < 1e-10);
      const oracleEdge = haversine(0, 0, 1, 0);
      assert.ok(Math.abs(Number(near[1].properties.distance) - oracleEdge) / oracleEdge < 1e-10);

      const limited = await bound.nearestTo({ lat: 0, lon: 0 }, 200_000, 1);
      assert.equal(limited.length, 1);

      // withinBBox: inclusive bounds
      const bbox = await bound.withinBBox(0, 0, 1, 1);
      assert.deepEqual(bbox.map((f) => f.properties.name).sort(), ["edge", "inner", "ring"]);

      // withinPolygon: boundary excluded — "edge" sits ON the polygon border
      const poly: [number, number][] = [[0, 0], [0, 2], [2, 2], [2, 0]];
      const inPoly = await bound.withinPolygon(poly);
      assert.deepEqual(inPoly.map((f) => f.properties.name).sort(), ["inner", "ring"]);

      await t.execute("DROP TABLE IF EXISTS x04_geo_pts");
    });

    it("geo: pure functions remain available to an RLS-active role (no store)", async (ctx) => {
      requireEngine(ctx);
      const pgMod = (await import("pg")).default;
      const client = new pgMod.Client({ connectionString: url });
      await client.connect();
      try {
        await client.query("SET ROLE x04_rls_probe");
        const r = await client.query("SELECT GEO_DISTANCE(0,0,0,1)");
        const d = Number(r.rows[0][Object.keys(r.rows[0])[0]]);
        assert.ok(typeof d === "number" && d > 111_000 && d < 112_000);
        await client.query("RESET ROLE");
      } finally {
        await client.end();
      }
    });

    it("geo: layer reads under an RLS-active table hit the relational guard", async (ctx) => {
      requireEngine(ctx);
      // An RLS-enabled table WITHOUT a policy: the relational path must deny
      // (or the engine must document otherwise) — recorded honestly either way.
      await t.execute("DROP TABLE IF EXISTS x04_geo_rls");
      await t.execute("CREATE TABLE x04_geo_rls (id BIGINT GENERATED ALWAYS AS IDENTITY, lat DOUBLE PRECISION, lon DOUBLE PRECISION, properties TEXT)");
      await t.execute("ALTER TABLE x04_geo_rls ENABLE ROW LEVEL SECURITY");
      await t.execute("INSERT INTO x04_geo_rls (lat, lon, properties) VALUES (1, 1, '{}')");

      const pgMod = (await import("pg")).default;
      const client = new pgMod.Client({ connectionString: url });
      await client.connect();
      let observed: string;
      try {
        await client.query("SET ROLE x04_rls_probe");
        try {
          const rows = await client.query("SELECT id FROM x04_geo_rls");
          observed = rows.rows.length === 0 ? "empty-result (RLS filtered)" : `unexpected rows: ${rows.rows.length}`;
        } catch (err) {
          observed = `error: ${err instanceof Error ? err.message : String(err)}`;
        }
        await client.query("RESET ROLE");
      } finally {
        await client.end();
      }
      console.log(`[x04] RLS layer read observation: ${observed}`);
      assert.ok(observed.startsWith("empty-result") || observed.startsWith("error"),
        `RLS-active layer read must not leak rows to a non-superuser, saw: ${observed}`);
      await t.execute("DROP TABLE IF EXISTS x04_geo_rls");
    });
  });

  // -------------------------------------------------------------------
  // Restart phases (orchestrated externally; engine restarts between)
  // -------------------------------------------------------------------

  describe("restart phase", () => {
    it(PHASE === "pre" ? "seeds marker state" : PHASE === "post" ? "verifies marker state" : "no-op (main run)", async (ctx) => {
      requireEngine(ctx);
      if (PHASE === "main") return;
      const feats = await detectFeatures(t);
      const kv = withKV.init(t, feats).kv;
      const blob = withBlob.init(t, feats).blob;

      if (PHASE === "pre") {
        const payload = randomBytes(2048);
        await kv.set("x04_restart:stable", "survives");
        await kv.set("x04_restart:tll", "expiring-later", { ttl: 120 });
        await blob.put("x04_restart", "blob", payload, { contentType: "application/x-restart" });
        await t.execute("DROP TABLE IF EXISTS x04_geo_restart");
        await t.execute("CREATE TABLE x04_geo_restart (id BIGINT GENERATED ALWAYS AS IDENTITY, lat DOUBLE PRECISION, lon DOUBLE PRECISION, properties TEXT)");
        await t.execute(`INSERT INTO x04_geo_restart (lat, lon, properties) VALUES (37.5, -122.1, '{"name":"beacon"}')`);
        writeFileSync(MANIFEST, JSON.stringify({
          at: Date.now(),
          blobHex: Buffer.from(payload).toString("hex"),
          blobCount: await blob.blobCount(),
        }));
        console.log(`[x04] restart pre-state seeded at ${new Date().toISOString()}`);
        return;
      }

      // post
      assert.ok(existsSync(MANIFEST), "run X04_RESTART_PHASE=pre first");
      const m = JSON.parse(readFileSync(MANIFEST, "utf8")) as { at: number; blobHex: string; blobCount: number };
      assert.equal(await kv.get("x04_restart:stable"), "survives");

      const remaining = await kv.ttl("x04_restart:tll");
      const elapsed = (Date.now() - m.at) / 1000;
      assert.ok(remaining > 0, `TTL key must still be expiring after restart, ttl()=${remaining}`);
      assert.ok(remaining <= 120 - Math.floor(elapsed) + 2,
        `TTL after restart (${remaining}s) should track wall-clock elapsed (${elapsed.toFixed(0)}s), not reset to 120`);

      const got = await blob.get("x04_restart", "blob");
      assert.notEqual(got, null, "blob must survive restart");
      assert.deepEqual([...got!.data], [...Buffer.from(m.blobHex, "hex")], "blob bytes byte-exact after restart");
      assert.equal(got!.meta!.contentType, "application/x-restart");
      const tail = await blob.getRange("x04_restart", "blob", 2040, 8);
      assert.deepEqual([...tail!.data], [...Buffer.from(m.blobHex, "hex").subarray(2040, 2048)]);

      // Geo layer durability: rows are ordinary table rows.
      const geoRows = await t.query<{ id: string; lat: number | string; lon: number | string; properties: string }>(
        "SELECT id, lat, lon, properties FROM x04_geo_restart",
      );
      assert.equal(geoRows.rows.length, 1, "layer row must survive restart");
      assert.equal(Number(geoRows.rows[0].lat), 37.5);
      assert.equal(Number(geoRows.rows[0].lon), -122.1);
      const geo = withGeo.init(t, await detectFeatures(t)).geo;
      const after = await geo.nearestTo("x04_geo_restart", { lat: 37.5, lon: -122.1 }, 1000, 5);
      assert.equal(after.length, 1);
      assert.equal(after[0].properties.name, "beacon");
      console.log("[x04] restart post verified (kv + blob + geo)");
    });
  });
});
