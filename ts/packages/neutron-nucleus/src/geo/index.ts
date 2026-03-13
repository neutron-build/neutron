// ---------------------------------------------------------------------------
// @neutron/nucleus/geo — Geospatial model plugin
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

// ---------------------------------------------------------------------------
// GeoModel interface
// ---------------------------------------------------------------------------

export interface GeoModel {
  /** Haversine distance in metres between two points. */
  distance(a: GeoPoint, b: GeoPoint): Promise<number>;

  /** Euclidean distance between two points. */
  distanceEuclidean(a: GeoPoint, b: GeoPoint): Promise<number>;

  /** Check if `b` is within `radiusMeters` of `a`. */
  within(a: GeoPoint, b: GeoPoint, radiusMeters: number): Promise<boolean>;

  /** Calculate the area of a polygon (minimum 3 points). */
  area(points: GeoPoint[]): Promise<number>;

  /** Find features within a radius of a point. */
  nearestTo(layer: string, point: GeoPoint, radiusMeters: number, limit: number): Promise<GeoFeature[]>;

  /** Find features inside a bounding box. */
  withinBBox(layer: string, minLat: number, minLon: number, maxLat: number, maxLon: number): Promise<GeoFeature[]>;

  /** Find features inside a polygon. Each coordinate is `[lat, lon]`. */
  withinPolygon(layer: string, polygon: [number, number][]): Promise<GeoFeature[]>;

  /** Insert a geographic feature into a layer table. */
  insert(layer: string, lat: number, lon: number, props: Record<string, unknown>): Promise<void>;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

class GeoModelImpl implements GeoModel {
  constructor(
    private readonly transport: Transport,
    private readonly features: NucleusFeatures,
  ) {}

  private require(): void {
    requireNucleus(this.features, 'Geo');
  }

  async distance(a: GeoPoint, b: GeoPoint): Promise<number> {
    this.require();
    return (
      (await this.transport.fetchval<number>('SELECT GEO_DISTANCE($1, $2, $3, $4)', [
        a.lat, a.lon, b.lat, b.lon,
      ])) ?? 0
    );
  }

  async distanceEuclidean(a: GeoPoint, b: GeoPoint): Promise<number> {
    this.require();
    return (
      (await this.transport.fetchval<number>('SELECT GEO_DISTANCE_EUCLIDEAN($1, $2, $3, $4)', [
        a.lat, a.lon, b.lat, b.lon,
      ])) ?? 0
    );
  }

  async within(a: GeoPoint, b: GeoPoint, radiusMeters: number): Promise<boolean> {
    this.require();
    return (
      (await this.transport.fetchval<boolean>('SELECT GEO_WITHIN($1, $2, $3, $4, $5)', [
        a.lat, a.lon, b.lat, b.lon, radiusMeters,
      ])) ?? false
    );
  }

  async area(points: GeoPoint[]): Promise<number> {
    this.require();
    if (points.length < 3) throw new Error('GEO_AREA requires at least 3 points');

    const args: unknown[] = [];
    const placeholders: string[] = [];
    for (let i = 0; i < points.length; i++) {
      args.push(points[i].lon, points[i].lat);
      placeholders.push(`$${i * 2 + 1}`, `$${i * 2 + 2}`);
    }
    const sql = `SELECT GEO_AREA(${placeholders.join(', ')})`;
    return (await this.transport.fetchval<number>(sql, args)) ?? 0;
  }

  async nearestTo(
    layer: string,
    point: GeoPoint,
    radiusMeters: number,
    limit: number,
  ): Promise<GeoFeature[]> {
    this.require();
    assertIdentifier(layer, 'layer name');

    const sql =
      `SELECT id, lat, lon, properties, GEO_DISTANCE($1, $2, lat, lon) AS dist ` +
      `FROM ${layer} ` +
      `WHERE GEO_WITHIN($1, $2, lat, lon, $3) ` +
      `ORDER BY dist LIMIT $4`;

    const result = await this.transport.query<{
      id: string; lat: number; lon: number; properties: string; dist: number;
    }>(sql, [point.lat, point.lon, radiusMeters, limit]);

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
  ): Promise<GeoFeature[]> {
    this.require();
    assertIdentifier(layer, 'layer name');

    const sql =
      `SELECT id, lat, lon, properties FROM ${layer} ` +
      `WHERE lat >= $1 AND lat <= $3 AND lon >= $2 AND lon <= $4`;

    const result = await this.transport.query<{
      id: string; lat: number; lon: number; properties: string;
    }>(sql, [minLat, minLon, maxLat, maxLon]);

    return result.rows.map((r) => ({
      id: r.id,
      lat: Number(r.lat),
      lon: Number(r.lon),
      properties: typeof r.properties === 'string' ? JSON.parse(r.properties) : (r.properties ?? {}),
    }));
  }

  async withinPolygon(layer: string, polygon: [number, number][]): Promise<GeoFeature[]> {
    this.require();
    assertIdentifier(layer, 'layer name');

    // Build GeoJSON polygon ([lon, lat] coordinate order)
    const coords = polygon.map(([lat, lon]) => [lon, lat]);
    // Close the ring if not already closed
    if (
      coords.length > 0 &&
      (coords[0][0] !== coords[coords.length - 1][0] || coords[0][1] !== coords[coords.length - 1][1])
    ) {
      coords.push([...coords[0]]);
    }
    const geoJson = JSON.stringify({
      type: 'Polygon',
      coordinates: [coords],
    });

    const sql =
      `SELECT id, lat, lon, properties FROM ${layer} ` +
      `WHERE ST_CONTAINS(ST_GEOMFROMGEOJSON($1), ST_MAKEPOINT(lon, lat))`;

    const result = await this.transport.query<{
      id: string; lat: number; lon: number; properties: string;
    }>(sql, [geoJson]);

    return result.rows.map((r) => ({
      id: r.id,
      lat: Number(r.lat),
      lon: Number(r.lon),
      properties: typeof r.properties === 'string' ? JSON.parse(r.properties) : (r.properties ?? {}),
    }));
  }

  async insert(layer: string, lat: number, lon: number, props: Record<string, unknown>): Promise<void> {
    this.require();
    assertIdentifier(layer, 'layer name');
    const propsJson = JSON.stringify(props);
    await this.transport.execute(
      `INSERT INTO ${layer} (lat, lon, properties) VALUES ($1, $2, $3)`,
      [lat, lon, propsJson],
    );
  }
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
