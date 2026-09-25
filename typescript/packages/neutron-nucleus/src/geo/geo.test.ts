// ---------------------------------------------------------------------------
// @neutron-build/nucleus/geo — unit tests (X04)
//
// Wire-level checks plus HAND ORACLES implemented independently in JS:
// haversine with R = 6,371,000 m (the engine's constant), shoelace area, and
// OGC ray-cast containment with boundary exclusion. The oracle math here is
// deliberately written from the formulas, not from the engine's Rust code.
// ---------------------------------------------------------------------------

import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { withGeo } from "./index.js";
import type { GeoModel } from "./index.js";
import type { Transport, TransactionTransport, QueryResult, IsolationLevel } from "../types.js";

interface Call {
  method: "query" | "execute" | "fetchval";
  sql: string;
  params?: unknown[];
  opts?: { signal?: AbortSignal };
}

/** Transport speaking the GEO protocol surface with hand-oracle math. */
class GeoProtocolTransport implements Transport {
  readonly calls: Call[] = [];

  private checkAborted(opts?: { signal?: AbortSignal }): void {
    if (opts?.signal?.aborted) throw new DOMException("This operation was aborted", "AbortError");
  }

  async query<T = Record<string, unknown>>(sql: string, params: unknown[] = [], opts?: { signal?: AbortSignal }): Promise<QueryResult<T>> {
    this.checkAborted(opts);
    this.calls.push({ method: "query", sql, params, opts });
    return { rows: (this.rowsFor(sql, params) ?? []) as T[], rowCount: 0 };
  }

  async execute(sql: string, params: unknown[] = [], opts?: { signal?: AbortSignal }): Promise<number> {
    this.checkAborted(opts);
    this.calls.push({ method: "execute", sql, params, opts });
    return 1;
  }

  async fetchval<T = unknown>(sql: string, params: unknown[] = [], opts?: { signal?: AbortSignal }): Promise<T | null> {
    this.checkAborted(opts);
    this.calls.push({ method: "fetchval", sql, params, opts });
    const one = this.rowsFor(sql, params);
    return (one && one.length > 0 ? one[0] : null) as T | null;
  }

  async beginTransaction(_isolation?: IsolationLevel): Promise<TransactionTransport> {
    throw new Error("not needed");
  }
  async close(): Promise<void> {}
  async ping(): Promise<void> {}

  /** Next result rows for layer queries (tests stage rows explicitly). */
  stagedRows: Record<string, unknown>[] = [];

  private rowsFor(sql: string, params: unknown[]): unknown[] | null {
    if (sql.startsWith("SELECT GEO_DISTANCE(")) {
      const [lat1, lon1, lat2, lon2] = params.map(Number);
      return [haversine(lat1, lon1, lat2, lon2)];
    }
    if (sql.startsWith("SELECT GEO_DISTANCE_EUCLIDEAN")) {
      const [x1, y1, x2, y2] = params.map(Number);
      return [Math.hypot(x2 - x1, y2 - y1)];
    }
    if (sql.startsWith("SELECT GEO_WITHIN")) {
      const [lat1, lon1, lat2, lon2, r] = params.map(Number);
      return [haversine(lat1, lon1, lat2, lon2) <= r];
    }
    if (sql.startsWith("SELECT GEO_AREA")) {
      const flat = params.map(Number);
      const pts: Array<[number, number]> = [];
      for (let i = 0; i < flat.length; i += 2) pts.push([flat[i], flat[i + 1]]);
      return [shoelace(pts)];
    }
    if (sql.startsWith("SELECT ST_CONTAINS($1, ST_MAKEPOINT(")) {
      const [wkt, lon, lat] = params;
      return [contains(parseWktPolygon(String(wkt)), Number(lon), Number(lat))];
    }
    if (sql.startsWith("SELECT id, lat, lon, properties")) {
      return this.stagedRows;
    }
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
  hasGeo: true,
  hasBlob: false,
  hasStreams: false,
  hasColumnar: false,
  hasDatalog: false,
  hasCDC: false,
  hasPubSub: false,
  version: "Nucleus 1.0.2 (test)",
};

// --- hand oracles (written from the formulas) -------------------------------

const R = 6_371_000;

function haversine(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const toRad = (d: number) => (d * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const h =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(h));
}

function shoelace(pts: Array<[number, number]>): number {
  let sum = 0;
  for (let i = 0; i < pts.length; i++) {
    const [x1, y1] = pts[i];
    const [x2, y2] = pts[(i + 1) % pts.length];
    sum += x1 * y2 - x2 * y1;
  }
  return Math.abs(sum) / 2;
}

function onSegment(px: number, py: number, x1: number, y1: number, x2: number, y2: number): boolean {
  const cross = (x2 - x1) * (py - y1) - (y2 - y1) * (px - x1);
  if (Math.abs(cross) > 1e-12) return false;
  return (
    px >= Math.min(x1, x2) - 1e-12 && px <= Math.max(x1, x2) + 1e-12 &&
    py >= Math.min(y1, y2) - 1e-12 && py <= Math.max(y1, y2) + 1e-12
  );
}

function contains(ring: Array<[number, number]>, x: number, y: number): boolean {
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const [xi, yi] = ring[i];
    const [xj, yj] = ring[j];
    if (onSegment(x, y, xi, yi, xj, yj)) return false; // boundary NOT contained
    if (yi > y !== yj > y && x < ((xj - xi) * (y - yi)) / (yj - yi) + xi) inside = !inside;
  }
  return inside;
}

function parseWktPolygon(wkt: string): Array<[number, number]> {
  const body = wkt.slice(wkt.indexOf("((") + 2, wkt.lastIndexOf("))"));
  return body.split(",").map((pair) => pair.trim().split(/\s+/).map(Number) as [number, number]);
}

// -----------------------------------------------------------------------------

function makeGeo(t: GeoProtocolTransport): GeoModel {
  return withGeo.init(t, FEATURES).geo;
}

const SF = { lat: 37.7749, lon: -122.4194 };
const LA = { lat: 34.0522, lon: -118.2437 };

describe("geo pure predicates vs hand oracles", () => {
  it("distance matches an independent haversine (SF-LA ~559 km)", async () => {
    const t = new GeoProtocolTransport();
    const geo = makeGeo(t);
    const d = await geo.distance(SF, LA);
    const oracle = haversine(SF.lat, SF.lon, LA.lat, LA.lon);
    assert.ok(Math.abs(d - oracle) < 1e-6, `engine ${d} vs oracle ${oracle}`);
    assert.ok(d > 559_000 && d < 560_000, `SF-LA should be ~559 km, got ${d}`);
    assert.equal(await geo.distance(SF, SF), 0);
  });

  it("euclidean distance matches Math.hypot", async () => {
    const geo = makeGeo(new GeoProtocolTransport());
    const d = await geo.distanceEuclidean({ lat: 0, lon: 0 }, { lat: 3, lon: 4 });
    assert.equal(d, 5);
  });

  it("within is INCLUSIVE at the exact radius and false just outside", async () => {
    const geo = makeGeo(new GeoProtocolTransport());
    const exact = haversine(0, 0, 0, 1);
    assert.equal(await geo.within({ lat: 0, lon: 0 }, { lat: 0, lon: 1 }, exact), true);
    assert.equal(await geo.within({ lat: 0, lon: 0 }, { lat: 0, lon: 1 }, exact - 1), false);
  });

  it("area matches shoelace over (lon, lat) pairs; rejects < 3 points", async () => {
    const geo = makeGeo(new GeoProtocolTransport());
    const square = [
      { lat: 0, lon: 0 },
      { lat: 0, lon: 4 },
      { lat: 3, lon: 4 },
      { lat: 3, lon: 0 },
    ];
    // NOTE: engine GEO_AREA takes (x=lon, y=lat) pairs; client maps lon->x.
    const a = await geo.area(square);
    assert.ok(Math.abs(a - 12) < 1e-9, `expected 12 square degrees, got ${a}`);
    await assert.rejects(geo.area([{ lat: 0, lon: 0 }, { lat: 1, lon: 1 }]), /at least 3 points/);
  });

  it("containsPoint: interior yes; vertex, edge midpoint and exterior no", async () => {
    const geo = makeGeo(new GeoProtocolTransport());
    const square: [number, number][] = [
      [0, 0], // lat, lon
      [0, 4],
      [4, 4],
      [4, 0],
    ];
    assert.equal(await geo.containsPoint(square, { lat: 2, lon: 2 }), true);
    assert.equal(await geo.containsPoint(square, { lat: 0, lon: 0 }), false); // vertex
    assert.equal(await geo.containsPoint(square, { lat: 0, lon: 2 }), false); // edge midpoint
    assert.equal(await geo.containsPoint(square, { lat: 5, lon: 2 }), false); // exterior
  });
});

describe("geo layer queries", () => {
  it("nearestTo asks radius-filtered, distance-ordered SQL with params", async () => {
    const t = new GeoProtocolTransport();
    const geo = makeGeo(t);
    t.stagedRows = [
      { id: "1", lat: 1, lon: 1, properties: '{"n":"near"}', dist: 157.2 },
    ];
    const rows = await geo.nearestTo("pts", { lat: 0, lon: 0 }, 1000, 5);
    const call = t.calls.at(-1)!;
    assert.ok(call.sql.includes("FROM pts"));
    assert.ok(call.sql.includes("ORDER BY dist"));
    assert.deepEqual(call.params, [0, 0, 1000, 5]);
    assert.equal(rows[0].id, "1");
    assert.deepEqual(rows[0].properties, { n: "near", distance: 157.2 });
  });

  it("withinPolygon builds closed-ring WKT with lon-first coordinates", async () => {
    const t = new GeoProtocolTransport();
    const geo = makeGeo(t);
    t.stagedRows = [];
    await geo.withinPolygon("pts", [[0, 0], [0, 2], [2, 2]]);
    const wkt = t.calls.at(-1)!.params![0] as string;
    assert.equal(wkt, "POLYGON((0 0, 2 0, 2 2, 0 0))");
  });

  it("schema-qualified layers and the layer() identity binding", async () => {
    const t = new GeoProtocolTransport();
    const geo = makeGeo(t);
    t.stagedRows = [];
    await geo.withinBBox("pts", 0, 0, 1, 1, { schema: "app" });
    assert.ok(t.calls.at(-1)!.sql.includes("FROM app.pts"));

    const bound = geo.layer({ schema: "app", table: "sensors" });
    const before = t.calls.length;
    await bound.insert(1.5, 2.5, { kind: "temp" });
    assert.equal(t.calls.length - before, 1, "bind itself emits nothing; insert is one statement");
    assert.ok(t.calls.at(-1)!.sql.includes("INSERT INTO app.sensors"));
    assert.deepEqual(t.calls.at(-1)!.params, [1.5, 2.5, '{"kind":"temp"}']);
  });

  it("rejects invalid layer/schema identifiers before any SQL", async () => {
    const t = new GeoProtocolTransport();
    const geo = makeGeo(t);
    assert.throws(() => geo.layer({ schema: "a b", table: "t" }), /Invalid layer schema/);
    await assert.rejects(geo.nearestTo("bad table; DROP", { lat: 0, lon: 0 }, 1, 1), /Invalid layer name/);
    assert.equal(t.calls.length, 0);
  });

  it("threads abort signals; already-aborted rejects pre-flight", async () => {
    const t = new GeoProtocolTransport();
    const geo = makeGeo(t);
    const ac = new AbortController();
    ac.abort();
    await assert.rejects(geo.distance(SF, LA, { signal: ac.signal }));
    await assert.rejects(geo.nearestTo("pts", SF, 1, 1, { signal: ac.signal }));
    const ac2 = new AbortController();
    await geo.withinBBox("pts", 0, 0, 1, 1, { signal: ac2.signal });
    assert.equal(t.calls.at(-1)!.opts?.signal, ac2.signal);
  });
});
