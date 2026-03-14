import { describe, it, expect } from 'vitest'
import type { QueryResult } from '../../lib/types'

// Tests for GeoModule utility functions: parseGeoPoints, computeBounds

interface GeoPoint {
  id: string
  lat: number
  lon: number
  extra: Record<string, unknown>
}

function parseGeoPoints(result: QueryResult): GeoPoint[] {
  const cols = result.columns.map(c => c.toLowerCase())
  const latIdx = cols.findIndex(c => c === 'lat' || c === 'latitude' || c === 'y')
  const lonIdx = cols.findIndex(c => c === 'lon' || c === 'lng' || c === 'longitude' || c === 'x')
  const idIdx = cols.findIndex(c => c === 'id' || c === 'point_id' || c === 'gid')

  if (latIdx < 0 || lonIdx < 0) return []

  const points: GeoPoint[] = []
  for (let i = 0; i < result.rows.length; i++) {
    const row = result.rows[i] as unknown[]
    const lat = Number(row[latIdx])
    const lon = Number(row[lonIdx])
    if (isNaN(lat) || isNaN(lon)) continue

    const id = idIdx >= 0 ? String(row[idIdx]) : String(i)
    const extra: Record<string, unknown> = {}
    for (let c = 0; c < result.columns.length; c++) {
      if (c !== latIdx && c !== lonIdx && c !== idIdx) {
        extra[result.columns[c]] = row[c]
      }
    }
    points.push({ id, lat, lon, extra })
  }
  return points
}

function computeBounds(points: GeoPoint[]): { minLat: number; maxLat: number; minLon: number; maxLon: number } {
  let minLat = Infinity, maxLat = -Infinity
  let minLon = Infinity, maxLon = -Infinity
  for (const p of points) {
    if (p.lat < minLat) minLat = p.lat
    if (p.lat > maxLat) maxLat = p.lat
    if (p.lon < minLon) minLon = p.lon
    if (p.lon > maxLon) maxLon = p.lon
  }
  const latPad = Math.max((maxLat - minLat) * 0.1, 0.001)
  const lonPad = Math.max((maxLon - minLon) * 0.1, 0.001)
  return {
    minLat: minLat - latPad,
    maxLat: maxLat + latPad,
    minLon: minLon - lonPad,
    maxLon: maxLon + lonPad,
  }
}

describe('GeoModule — parseGeoPoints', () => {
  it('should return empty for results without lat/lon columns', () => {
    const result: QueryResult = {
      columns: ['name', 'value'],
      rows: [['A', 1]],
      rowCount: 1,
      duration: 0,
    }
    expect(parseGeoPoints(result)).toEqual([])
  })

  it('should parse lat/lon columns', () => {
    const result: QueryResult = {
      columns: ['id', 'lat', 'lon'],
      rows: [['p1', 37.7749, -122.4194]],
      rowCount: 1,
      duration: 0,
    }
    const points = parseGeoPoints(result)
    expect(points.length).toBe(1)
    expect(points[0].id).toBe('p1')
    expect(points[0].lat).toBeCloseTo(37.7749)
    expect(points[0].lon).toBeCloseTo(-122.4194)
  })

  it('should recognize latitude/longitude column names', () => {
    const result: QueryResult = {
      columns: ['latitude', 'longitude'],
      rows: [[40.7128, -74.006]],
      rowCount: 1,
      duration: 0,
    }
    const points = parseGeoPoints(result)
    expect(points.length).toBe(1)
  })

  it('should recognize x/y column names', () => {
    const result: QueryResult = {
      columns: ['y', 'x'],
      rows: [[51.5074, -0.1278]],
      rowCount: 1,
      duration: 0,
    }
    const points = parseGeoPoints(result)
    expect(points.length).toBe(1)
    expect(points[0].lat).toBeCloseTo(51.5074)
  })

  it('should recognize lng column name', () => {
    const result: QueryResult = {
      columns: ['lat', 'lng'],
      rows: [[48.8566, 2.3522]],
      rowCount: 1,
      duration: 0,
    }
    const points = parseGeoPoints(result)
    expect(points.length).toBe(1)
  })

  it('should use row index as id when no id column', () => {
    const result: QueryResult = {
      columns: ['lat', 'lon'],
      rows: [[1.0, 2.0], [3.0, 4.0]],
      rowCount: 2,
      duration: 0,
    }
    const points = parseGeoPoints(result)
    expect(points[0].id).toBe('0')
    expect(points[1].id).toBe('1')
  })

  it('should recognize point_id and gid columns', () => {
    const result: QueryResult = {
      columns: ['point_id', 'lat', 'lon'],
      rows: [['pid-1', 10, 20]],
      rowCount: 1,
      duration: 0,
    }
    expect(parseGeoPoints(result)[0].id).toBe('pid-1')
  })

  it('should skip rows with NaN coordinates', () => {
    const result: QueryResult = {
      columns: ['lat', 'lon'],
      rows: [[37.7, -122.4], ['invalid', -74], [40.7, 'invalid']],
      rowCount: 3,
      duration: 0,
    }
    const points = parseGeoPoints(result)
    expect(points.length).toBe(1)
  })

  it('should extract extra columns', () => {
    const result: QueryResult = {
      columns: ['id', 'lat', 'lon', 'name', 'category'],
      rows: [['p1', 37.0, -122.0, 'Cafe', 'food']],
      rowCount: 1,
      duration: 0,
    }
    const points = parseGeoPoints(result)
    expect(points[0].extra).toEqual({ name: 'Cafe', category: 'food' })
  })
})

describe('GeoModule — computeBounds', () => {
  it('should compute bounds with 10% padding', () => {
    const points: GeoPoint[] = [
      { id: '1', lat: 37.0, lon: -122.5, extra: {} },
      { id: '2', lat: 38.0, lon: -121.5, extra: {} },
    ]
    const bounds = computeBounds(points)
    // Range is 1.0 lat, 1.0 lon. Padding = 0.1
    expect(bounds.minLat).toBeCloseTo(36.9)
    expect(bounds.maxLat).toBeCloseTo(38.1)
    expect(bounds.minLon).toBeCloseTo(-122.6)
    expect(bounds.maxLon).toBeCloseTo(-121.4)
  })

  it('should use minimum padding of 0.001 degrees for single point', () => {
    const points: GeoPoint[] = [
      { id: '1', lat: 37.0, lon: -122.0, extra: {} },
    ]
    const bounds = computeBounds(points)
    // Range is 0, so padding is 0.001
    expect(bounds.minLat).toBeCloseTo(36.999)
    expect(bounds.maxLat).toBeCloseTo(37.001)
    expect(bounds.minLon).toBeCloseTo(-122.001)
    expect(bounds.maxLon).toBeCloseTo(-121.999)
  })

  it('should handle points at same location', () => {
    const points: GeoPoint[] = [
      { id: '1', lat: 0, lon: 0, extra: {} },
      { id: '2', lat: 0, lon: 0, extra: {} },
    ]
    const bounds = computeBounds(points)
    expect(bounds.minLat).toBeLessThan(0)
    expect(bounds.maxLat).toBeGreaterThan(0)
  })

  it('should handle negative coordinates', () => {
    const points: GeoPoint[] = [
      { id: '1', lat: -33.8688, lon: 151.2093, extra: {} },
      { id: '2', lat: -34.0, lon: 151.0, extra: {} },
    ]
    const bounds = computeBounds(points)
    expect(bounds.minLat).toBeLessThan(-34.0)
    expect(bounds.maxLat).toBeGreaterThan(-33.8688)
  })
})
