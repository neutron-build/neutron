import { describe, it, expect } from 'vitest'
import { formatTTL, ttlFromEngine } from './KVModule'

// Production TTL helpers. Rendering/filtering/dispatch use actual component tests.

describe('KVModule — formatTTL', () => {
  it('should format seconds under 60 as seconds', () => {
    expect(formatTTL(0)).toBe('0s')
    expect(formatTTL(1)).toBe('1s')
    expect(formatTTL(59)).toBe('59s')
  })

  it('should format 60-3599 as minutes', () => {
    expect(formatTTL(60)).toBe('1m')
    expect(formatTTL(120)).toBe('2m')
    expect(formatTTL(90)).toBe('1m')
    expect(formatTTL(3599)).toBe('59m')
  })

  it('should format 3600+ as hours', () => {
    expect(formatTTL(3600)).toBe('1h')
    expect(formatTTL(7200)).toBe('2h')
    expect(formatTTL(86400)).toBe('24h')
  })
})

it('maps real engine TTL sentinels to null without losing zero', () => {
  expect([3563, 0, -1, -2, null].map(ttlFromEngine)).toEqual([3563, 0, null, null, null])
})
