// ---------------------------------------------------------------------------
// @neutron-build/nucleus/geo — Geospatial model plugin
// ---------------------------------------------------------------------------
// Engine reality (nucleus/docs/MODEL_SEMANTICS.md, verified live in X04):
// there is NO geospatial store. GEO_* / ST_* are PURE computations over their
// arguments; "layers" are ordinary SQL tables (id/lat/lon/properties) read
// through the relational path. There is no R-tree (USING RTREE is silently
// mapped to BTree by the engine) and no geography type — distances are
// haversine over degrees with Earth radius 6,371,000 m, areas are shoelace
// over (x=lon, y=lat) degree pairs, and ST_CONTAINS uses ray casting where
// boundary points are NOT contained (OGC).
//
// On plain PostgreSQL these functions do not exist; the model fails closed
// through requireNucleus, and detectPostGIS() reports whether a PostGIS
// alternative is even installed. No PostGIS adapter is implemented or
// advertised.
// ---------------------------------------------------------------------------

import type { Transport, NucleusPlugin, NucleusFeatures } from '../types.js';
import { requireNucleus, assertIdentifier } from '../helpers.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface GeoPoint {
  lat: number;
  lon: number;
}

export interface GeoFeature {
  id: string;
  lat: number;
  lon: number;
  properties: Record<string, unknown>;
}

/** Options for layer-backed queries. */
export interface GeoLayerOptions {
  /** Schema qualifying the layer table (default: unqualified/search path). */
  schema?: string;
  /** Abort the query. */
  signal?: AbortSignal;
}

/** Options for pure-computation predicates. */
export interface GeoCalcOptions {
  /** Abort the call. */
  signal?: AbortSignal;
}

/**
 * SQL identity of a layer table. A layer IS a SQL table — this binding is a
 * qualified-name reference used for queries only; it never emits DDL and
 * never pretends the layer is anything but a table.
 */
export interface GeoLayerIdentity {
  schema: string;
  table: string;
}

/** Layer-scoped geospatial queries (the table identity is pre-filled). */
export interface GeoLayer {
  /** Insert a feature (lat/lon/properties) into the layer table. */
  insert(lat: number, lon: number, props: Record<string, unknown>, opts?: GeoCalcOptions): Promise<void>;

  /** Find features within `radiusMeters` of a point, ordered by distance. */
  nearestTo(point: GeoPoint, radiusMeters: number, limit: number, opts?: GeoCalcOptions): Promise<GeoFeature[]>;

  /** Find features inside a bounding box (bounds inclusive). */
  withinBBox(
    minLat: number,
    minLon: number,
    maxLat: number,
    maxLon: number,
    opts?: GeoCalcOptions,
  ): Promise<GeoFeature[]>;

  /** Find features inside a polygon. Each coordinate is `[lat, lon]`.
   *  Boundary points are NOT contained (engine ST_CONTAINS semantics). */
  withinPolygon(polygon: [number, number][], opts?: GeoCalcOptions): Promise<GeoFeature[]>;
}

// ---------------------------------------------------------------------------
// GeoModel interface
// ---------------------------------------------------------------------------

export interface GeoModel {
  /**
   * Haversine distance in metres between two points (Earth radius
   * 6,371,000 m — the engine's constant, not a configurable spheroid).
   */
  distance(a: GeoPoint, b: GeoPoint, opts?: GeoCalcOptions): Promise<number>;

  /** Euclidean distance between two points interpreted as (lat=x, lon=y). */
  distanceEuclidean(a: GeoPoint, b: GeoPoint, opts?: GeoCalcOptions): Promise<number>;

  /** Check if `b` is within `radiusMeters` of `a` (INCLUSIVE: a point at
   *  exactly `radiusMeters` distance is within). */
  within(a: GeoPoint, b: GeoPoint, radiusMeters: number, opts?: GeoCalcOptions): Promise<boolean>;

  /**
   * Area of a polygon (minimum 3 points) by the shoelace formula over
   * (x=lon, y=lat) degree pairs — square DEGREES, not square metres. An
   * unclosed ring is closed automatically.
   */
  area(points: GeoPoint[], opts?: GeoCalcOptions): Promise<number>;

  /**
   * Point-in-polygon via the engine's ST_CONTAINS: true only for interior
   * points — vertices and edge points are NOT contained. Each polygon
   * coordinate is `[lat, lon]`.
   */
  containsPoint(polygon: [number, number][], point: GeoPoint, opts?: GeoCalcOptions): Promise<boolean>;

  /** Find features within a radius of a point (ordered by distance). */
  nearestTo(layer: string, point: GeoPoint, radiusMeters: number, limit: number, opts?: GeoLayerOptions): Promise<GeoFeature[]>;

  /** Find features inside a bounding box (bounds inclusive). */
  withinBBox(layer: string, minLat: number, minLon: number, maxLat: number, maxLon: number, opts?: GeoLayerOptions): Promise<GeoFeature[]>;

  /** Find features inside a polygon. Each coordinate is `[lat, lon]`.
   *  Boundary points are NOT contained. */
  withinPolygon(layer: string, polygon: [number, number][], opts?: GeoLayerOptions): Promise<GeoFeature[]>;

  /** Insert a geographic feature into a layer table. */
  insert(layer: string, lat: number, lon: number, props: Record<string, unknown>, opts?: GeoLayerOptions): Promise<void>;

  /**
   * Bind to a layer's SQL identity (a qualified table reference). Emits no
   * SQL at bind time — the binding only qualifies later queries.
   */
  layer(identity: GeoLayerIdentity): GeoLayer;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

function qualifiedLayer(layer: string, schema?: string): string {
  assertIdentifier(layer, 'layer name');
  if (schema == null) return layer;
  assertIdentifier(schema, 'layer schema');
  return `${schema}.${layer}`;
}

class GeoLayerImpl implements GeoLayer {
  constructor(
    private readonly model: GeoModelImpl,
    private readonly identity: GeoLayerIdentity,
  ) {}

  insert(lat: number, lon: number, props: Record<string, unknown>, opts?: GeoCalcOptions): Promise<void> {
    return this.model.insert(this.identity.table, lat, lon, props, { schema: this.identity.schema, signal: opts?.signal });
  }

  nearestTo(point: GeoPoint, radiusMeters: number, limit: number, opts?: GeoCalcOptions): Promise<GeoFeature[]> {
    return this.model.nearestTo(this.identity.table, point, radiusMeters, limit, { schema: this.identity.schema, signal: opts?.signal });
  }

  withinBBox(minLat: number, minLon: number, maxLat: number, maxLon: number, opts?: GeoCalcOptions): Promise<GeoFeature[]> {
    return this.model.withinBBox(this.identity.table, minLat, minLon, maxLat, maxLon, { schema: this.identity.schema, signal: opts?.signal });
  }

  withinPolygon(polygon: [number, number][], opts?: GeoCalcOptions): Promise<GeoFeature[]> {
    return this.model.withinPolygon(this.identity.table, polygon, { schema: this.identity.schema, signal: opts?.signal });
  }
}

class GeoModelImpl implements GeoModel {
  constructor(
    private readonly transport: Transport,
    private readonly features: NucleusFeatures,
  ) {}

  private require(): void {
    requireNucleus(this.features, 'Geo');
  }

  async distance(a: GeoPoint, b: GeoPoint, opts?: GeoCalcOptions): Promise<number> {
    this.require();
    return (
      (await this.transport.fetchval<number>('SELECT GEO_DISTANCE($1::float8, $2::float8, $3::float8, $4::float8)', [
        a.lat, a.lon, b.lat, b.lon,
      ], { signal: opts?.signal })) ?? 0
    );
  }

  async distanceEuclidean(a: GeoPoint, b: GeoPoint, opts?: GeoCalcOptions): Promise<number> {
    this.require();
    return (
      (await this.transport.fetchval<number>('SELECT GEO_DISTANCE_EUCLIDEAN($1::float8, $2::float8, $3::float8, $4::float8)', [
        a.lat, a.lon, b.lat, b.lon,
      ], { signal: opts?.signal })) ?? 0
    );
  }

  async within(a: GeoPoint, b: GeoPoint, radiusMeters: number, opts?: GeoCalcOptions): Promise<boolean> {
    this.require();
    return (
      (await this.transport.fetchval<boolean>('SELECT GEO_WITHIN($1::float8, $2::float8, $3::float8, $4::float8, $5::float8)', [
        a.lat, a.lon, b.lat, b.lon, radiusMeters,
      ], { signal: opts?.signal })) ?? false
    );
  }

  async area(points: GeoPoint[], opts?: GeoCalcOptions): Promise<number> {
    this.require();
    if (points.length < 3) throw new Error('GEO_AREA requires at least 3 points');

    const args: unknown[] = [];
    const placeholders: string[] = [];
    for (let i = 0; i < points.length; i++) {
      args.push(points[i].lon, points[i].lat);
      placeholders.push(`$${i * 2 + 1}::float8`, `$${i * 2 + 2}::float8`);
    }
    const sql = `SELECT GEO_AREA(${placeholders.join(', ')})`;
    return (await this.transport.fetchval<number>(sql, args, { signal: opts?.signal })) ?? 0;
  }

  async containsPoint(polygon: [number, number][], point: GeoPoint, opts?: GeoCalcOptions): Promise<boolean> {
    this.require();
    const wkt = polygonToWkt(polygon);
    return (
      (await this.transport.fetchval<boolean>('SELECT ST_CONTAINS($1, ST_MAKEPOINT($2::float8, $3::float8))', [
        wkt, point.lon, point.lat,
      ], { signal: opts?.signal })) ?? false
    );
  }

  async nearestTo(
    layer: string,
    point: GeoPoint,
    radiusMeters: number,
    limit: number,
    opts?: GeoLayerOptions,
  ): Promise<GeoFeature[]> {
    return this.nearestToQualified(qualifiedLayer(layer, opts?.schema), point, radiusMeters, limit, opts?.signal);
  }

  private async nearestToQualified(
    table: string,
    point: GeoPoint,
    radiusMeters: number,
    limit: number,
    signal?: AbortSignal,
  ): Promise<GeoFeature[]> {
    this.require();

    // Params over pgwire arrive as text; the engine's GEO_* functions demand
    // numeric values, so every numeric parameter is cast at the SQL boundary.
    const sql =
      `SELECT id, lat, lon, properties, GEO_DISTANCE($1::float8, $2::float8, lat, lon) AS dist ` +
      `FROM ${table} ` +
      `WHERE GEO_WITHIN($1::float8, $2::float8, lat, lon, $3::float8) ` +
      `ORDER BY dist LIMIT $4::int`;

    const result = await this.transport.query<{
      id: string; lat: number; lon: number; properties: string; dist: number;
    }>(sql, [point.lat, point.lon, radiusMeters, limit], { signal });

    return result.rows.map((r) => ({
      id: r.id,
      lat: Number(r.lat),
      lon: Number(r.lon),
      properties: {
        ...(typeof r.properties === 'string' ? JSON.parse(r.properties) : (r.properties ?? {})),
        distance: Number(r.dist),
      },
    }));
  }

  async withinBBox(
    layer: string,
    minLat: number,
    minLon: number,
    maxLat: number,
    maxLon: number,
    opts?: GeoLayerOptions,
  ): Promise<GeoFeature[]> {
    this.require();
    const table = qualifiedLayer(layer, opts?.schema);

    const sql =
      `SELECT id, lat, lon, properties FROM ${table} ` +
      `WHERE lat >= $1::float8 AND lat <= $3::float8 AND lon >= $2::float8 AND lon <= $4::float8`;

    const result = await this.transport.query<{
      id: string; lat: number; lon: number; properties: string;
    }>(sql, [minLat, minLon, maxLat, maxLon], { signal: opts?.signal });

    return result.rows.map((r) => ({
      id: r.id,
      lat: Number(r.lat),
      lon: Number(r.lon),
      properties: typeof r.properties === 'string' ? JSON.parse(r.properties) : (r.properties ?? {}),
    }));
  }

  async withinPolygon(layer: string, polygon: [number, number][], opts?: GeoLayerOptions): Promise<GeoFeature[]> {
    this.require();
    const table = qualifiedLayer(layer, opts?.schema);
    const wkt = polygonToWkt(polygon);

    // ST_MAKEPOINT(x, y) returns 'POINT(x y)' text, which ST_CONTAINS accepts.
    const sql =
      `SELECT id, lat, lon, properties FROM ${table} ` +
      `WHERE ST_CONTAINS($1, ST_MAKEPOINT(lon, lat))`;

    const result = await this.transport.query<{
      id: string; lat: number; lon: number; properties: string;
    }>(sql, [wkt], { signal: opts?.signal });

    return result.rows.map((r) => ({
      id: r.id,
      lat: Number(r.lat),
      lon: Number(r.lon),
      properties: typeof r.properties === 'string' ? JSON.parse(r.properties) : (r.properties ?? {}),
    }));
  }

  async insert(layer: string, lat: number, lon: number, props: Record<string, unknown>, opts?: GeoLayerOptions): Promise<void> {
    return this.insertTo(qualifiedLayer(layer, opts?.schema), lat, lon, props, opts?.signal);
  }

  private async insertTo(
    table: string,
    lat: number,
    lon: number,
    props: Record<string, unknown>,
    signal?: AbortSignal,
  ): Promise<void> {
    this.require();
    const propsJson = JSON.stringify(props);
    await this.transport.execute(
      `INSERT INTO ${table} (lat, lon, properties) VALUES ($1, $2, $3)`,
      [lat, lon, propsJson],
      { signal },
    );
  }

  layer(identity: GeoLayerIdentity): GeoLayer {
    assertIdentifier(identity.schema, 'layer schema');
    assertIdentifier(identity.table, 'layer table');
    // Reference only — no SQL at bind time.
    return new GeoLayerImpl(this, identity);
  }
}

/** Build `POLYGON((x y, ...))` WKT from [lat, lon] pairs, closing the ring. */
function polygonToWkt(polygon: [number, number][]): string {
  const coords = polygon.map(([lat, lon]) => [lon, lat]);
  if (
    coords.length > 0 &&
    (coords[0][0] !== coords[coords.length - 1][0] || coords[0][1] !== coords[coords.length - 1][1])
  ) {
    coords.push([...coords[0]]);
  }
  return `POLYGON((${coords.map(([x, y]) => `${x} ${y}`).join(', ')}))`;
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

/** Plugin: adds `.geo` to the client. */
export const withGeo: NucleusPlugin<{ geo: GeoModel }> = {
  name: 'geo',
  init(transport: Transport, features: NucleusFeatures) {
    return { geo: new GeoModelImpl(transport, features) };
  },
};
