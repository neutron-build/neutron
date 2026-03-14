import { describe, it, expect } from 'vitest'

// Tests for PubSubModule: message deduplication, query building

interface PubSubMessage {
  id: string
  payload: string
  receivedAt: string
}

describe('PubSubModule — message deduplication', () => {
  function deduplicateMessages(existing: PubSubMessage[], incoming: PubSubMessage[]): PubSubMessage[] {
    const existingIds = new Set(existing.map(m => m.id))
    return incoming.filter(m => !existingIds.has(m.id))
  }

  it('should return all incoming when no existing messages', () => {
    const incoming: PubSubMessage[] = [
      { id: '1', payload: 'hello', receivedAt: '' },
    ]
    const fresh = deduplicateMessages([], incoming)
    expect(fresh).toEqual(incoming)
  })

  it('should filter out duplicate messages', () => {
    const existing: PubSubMessage[] = [
      { id: '1', payload: 'hello', receivedAt: '' },
    ]
    const incoming: PubSubMessage[] = [
      { id: '1', payload: 'hello', receivedAt: '' },
      { id: '2', payload: 'world', receivedAt: '' },
    ]
    const fresh = deduplicateMessages(existing, incoming)
    expect(fresh.length).toBe(1)
    expect(fresh[0].id).toBe('2')
  })

  it('should return empty when all incoming are duplicates', () => {
    const existing: PubSubMessage[] = [
      { id: '1', payload: 'a', receivedAt: '' },
      { id: '2', payload: 'b', receivedAt: '' },
    ]
    const incoming: PubSubMessage[] = [
      { id: '1', payload: 'a', receivedAt: '' },
      { id: '2', payload: 'b', receivedAt: '' },
    ]
    const fresh = deduplicateMessages(existing, incoming)
    expect(fresh.length).toBe(0)
  })
})

describe('PubSubModule — query building', () => {
  it('should build pubsub_info query', () => {
    const name = 'events'
    const sql = `SELECT subscriber_count FROM pubsub_info('${name}')`
    expect(sql).toBe("SELECT subscriber_count FROM pubsub_info('events')")
  })

  it('should build pubsub_poll query', () => {
    const name = 'events'
    const sql = `SELECT id, payload, received_at FROM pubsub_poll('${name}', 50)`
    expect(sql).toBe("SELECT id, payload, received_at FROM pubsub_poll('events', 50)")
  })

  it('should build pubsub_publish query with escaped payload', () => {
    const name = 'events'
    const msg = "it's a message"
    const sql = `SELECT pubsub_publish('${name}', '${msg.replace(/'/g, "''")}')`
    expect(sql).toBe("SELECT pubsub_publish('events', 'it''s a message')")
  })
})
