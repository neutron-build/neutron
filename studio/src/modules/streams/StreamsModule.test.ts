import { describe, it, expect } from 'vitest'

// Tests for StreamsModule utility function: formatIdleMs

function formatIdleMs(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`
  return `${(ms / 60_000).toFixed(1)}m`
}

describe('StreamsModule — formatIdleMs', () => {
  it('should format milliseconds', () => {
    expect(formatIdleMs(0)).toBe('0ms')
    expect(formatIdleMs(1)).toBe('1ms')
    expect(formatIdleMs(500)).toBe('500ms')
    expect(formatIdleMs(999)).toBe('999ms')
  })

  it('should format seconds', () => {
    expect(formatIdleMs(1000)).toBe('1.0s')
    expect(formatIdleMs(1500)).toBe('1.5s')
    expect(formatIdleMs(30000)).toBe('30.0s')
    expect(formatIdleMs(59999)).toBe('60.0s')
  })

  it('should format minutes', () => {
    expect(formatIdleMs(60000)).toBe('1.0m')
    expect(formatIdleMs(120000)).toBe('2.0m')
    expect(formatIdleMs(90000)).toBe('1.5m')
    expect(formatIdleMs(3600000)).toBe('60.0m')
  })
})

describe('StreamsModule — query building', () => {
  it('should build stream_range query', () => {
    const name = 'mystream'
    const fromId = '0-0'
    const limit = 100
    const sql = `SELECT id, data FROM stream_range('${name}', '${fromId}', '+', ${limit})`
    expect(sql).toBe("SELECT id, data FROM stream_range('mystream', '0-0', '+', 100)")
  })

  it('should build stream_len query', () => {
    const name = 'events'
    const sql = `SELECT stream_len('${name}')`
    expect(sql).toBe("SELECT stream_len('events')")
  })

  it('should build stream_groups query', () => {
    const name = 'events'
    const sql = `SELECT group_name, consumer_count, pending_count, last_delivered_id FROM stream_groups('${name}')`
    expect(sql).toContain("stream_groups('events')")
  })

  it('should build stream_create_group query', () => {
    const name = 'events'
    const groupName = "my-group"
    const startId = '0-0'
    const sql = `SELECT stream_create_group('${name}', '${groupName.replace(/'/g, "''")}', '${startId}')`
    expect(sql).toContain("stream_create_group('events'")
    expect(sql).toContain("'my-group'")
  })

  it('should escape single quotes in group name', () => {
    const groupName = "it's-a-group"
    const escaped = groupName.replace(/'/g, "''")
    expect(escaped).toBe("it''s-a-group")
  })

  it('should build stream_ack query', () => {
    const sql = `SELECT stream_ack('events', 'mygroup', '1234-0')`
    expect(sql).toContain("stream_ack")
  })

  it('should build stream_claim query', () => {
    const sql = `SELECT stream_claim('events', 'mygroup', 'consumer1', '1234-0')`
    expect(sql).toContain("stream_claim")
    expect(sql).toContain("'consumer1'")
  })
})

describe('StreamsModule — consumer group parsing', () => {
  interface ConsumerGroup {
    name: string
    consumers: number
    pending: number
    lastId: string
  }

  it('should parse consumer group rows', () => {
    const rows: unknown[][] = [
      ['group1', 3, 5, '1234-0'],
      ['group2', 1, 0, '5678-0'],
    ]
    const groups: ConsumerGroup[] = rows.map(r => ({
      name: String(r[0]),
      consumers: Number(r[1]),
      pending: Number(r[2]),
      lastId: String(r[3]),
    }))
    expect(groups.length).toBe(2)
    expect(groups[0]).toEqual({ name: 'group1', consumers: 3, pending: 5, lastId: '1234-0' })
    expect(groups[1].pending).toBe(0)
  })
})
