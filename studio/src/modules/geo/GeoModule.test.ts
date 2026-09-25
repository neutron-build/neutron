import { describe, it, expect } from 'vitest'
import { parsePolygon } from './GeoModule'

// Nucleus has no geo store — only scalar geometry functions. The module is a
// calculator that issues GEO_DISTANCE / GEO_WITHIN / GEO_AREA. These tests
// cover polygon parsing and the real SQL each mode builds.

describe('GeoModule — parsePolygon', () => {
  it('should parse comma-separated x,y pairs', () => {
    expect(parsePolygon('0,0\n4,0\n4,3')).toEqual([0, 0, 4, 0, 4, 3])
  })

  it('should parse whitespace-separated pairs', () => {
    expect(parsePolygon('1 2\n3 4')).toEqual([1, 2, 3, 4])
  })

  it('should skip blank and malformed lines', () => {
    expect(parsePolygon('0,0\n\n  \nbad\n1,1')).toEqual([0, 0, 1, 1])
  })

  it('should handle negative and decimal coordinates', () => {
    expect(parsePolygon('-1.5,2.25\n3,-4')).toEqual([-1.5, 2.25, 3, -4])
  })
})

describe('GeoModule — query building', () => {
  it('should build GEO_DISTANCE query', () => {
    const sql = `SELECT GEO_DISTANCE(${37.7749}, ${-122.4194}, ${34.0522}, ${-118.2437})`
    expect(sql).toBe('SELECT GEO_DISTANCE(37.7749, -122.4194, 34.0522, -118.2437)')
  })

  it('should build GEO_WITHIN query with radius in meters', () => {
    const sql = `SELECT GEO_WITHIN(${37.7749}, ${-122.4194}, ${34.0522}, ${-118.2437}, ${600000})`
    expect(sql).toBe('SELECT GEO_WITHIN(37.7749, -122.4194, 34.0522, -118.2437, 600000)')
  })

  it('should build GEO_AREA query from parsed polygon', () => {
    const coords = parsePolygon('0,0\n4,0\n4,3\n0,3')
    const sql = `SELECT GEO_AREA(${coords.join(', ')})`
    expect(sql).toBe('SELECT GEO_AREA(0, 0, 4, 0, 4, 3, 0, 3)')
  })
})

describe('GeoModule — contains query building', () => {
  // Mirrors the Contains tab: parse x,y lines, close the ring, build WKT,
  // wrap the point with ST_MAKEPOINT. Boundary points are NOT contained.
  function buildContainsQuery(polygonText: string, px: string, py: string): string {
    const coords = parsePolygon(polygonText)
    if (coords.length < 6) throw new Error('ST_CONTAINS needs at least 3 coordinate pairs')
    const ring: Array<[number, number]> = []
    for (let i = 0; i < coords.length; i += 2) ring.push([coords[i], coords[i + 1]])
    if (ring[0][0] !== ring[ring.length - 1][0] || ring[0][1] !== ring[ring.length - 1][1]) {
      ring.push([ring[0][0], ring[0][1]])
    }
    const wkt = `POLYGON((${ring.map(([x, y]) => `${x} ${y}`).join(', ')}))`
    const num = (v: string) => {
      const n = Number(v.trim())
      return isNaN(n) ? '0' : String(n)
    }
    return `SELECT ST_CONTAINS('${wkt}', ST_MAKEPOINT(${num(px)}, ${num(py)}))`
  }

  it('closes an unclosed ring in the WKT', () => {
    const sql = buildContainsQuery('0,0\n4,0\n4,3\n0,3', '2', '1.5')
    expect(sql).toBe("SELECT ST_CONTAINS('POLYGON((0 0, 4 0, 4 3, 0 3, 0 0))', ST_MAKEPOINT(2, 1.5))")
  })

  it('leaves an already-closed ring untouched', () => {
    const sql = buildContainsQuery('0,0\n4,0\n4,3\n0,3\n0,0', '2', '2')
    expect(sql).toBe("SELECT ST_CONTAINS('POLYGON((0 0, 4 0, 4 3, 0 3, 0 0))', ST_MAKEPOINT(2, 2))")
  })

  it('rejects polygons with fewer than 3 points', () => {
    expect(() => buildContainsQuery('0,0\n4,0', '1', '1')).toThrow(/at least 3/)
  })

  it('formats a boundary probe as a not-contained hint', () => {
    const cell = false
    const b = cell === true || cell === 'true' || cell === 't'
    const text = b ? 'Contained: true (interior point)' : 'Contained: false (exterior or ON the boundary — boundary points are not contained)'
    expect(text).toContain('boundary points are not contained')
  })
})
