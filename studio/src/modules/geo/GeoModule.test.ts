import { describe, it, expect } from 'vitest'
import { parsePolygon } from './GeoModule'

// Nucleus has no geo store — only scalar geometry functions. The module is a
// calculator that issues GEO_DISTANCE / GEO_WITHIN / GEO_AREA. These tests
// cover the production polygon parser; component transport tests cover dispatch.

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
