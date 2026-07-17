import { describe, it, expect } from 'vitest'
import { parseChannels } from './PubSubModule'

// Nucleus pub/sub over SQL is publish-only. PUBSUB_CHANNELS() takes no args and
// returns a comma-separated list; PUBSUB_SUBSCRIBERS(channel) and
// PUBSUB_PUBLISH(channel, message) return counts. There is no SQL poll.

describe('PubSubModule — parseChannels', () => {
  it('should return empty for null', () => {
    expect(parseChannels(null)).toEqual([])
  })

  it('should split a comma-separated channel list', () => {
    expect(parseChannels('events,alerts,logs')).toEqual(['events', 'alerts', 'logs'])
  })

  it('should trim whitespace and drop empties', () => {
    expect(parseChannels(' a , b ,, c ')).toEqual(['a', 'b', 'c'])
  })

  it('should return empty for an empty string', () => {
    expect(parseChannels('')).toEqual([])
  })
})

describe('PubSubModule — query building', () => {
  it('should build PUBSUB_SUBSCRIBERS query for a channel', () => {
    const name = 'events'
    const sql = `SELECT PUBSUB_SUBSCRIBERS('${name.replace(/'/g, "''")}')`
    expect(sql).toBe("SELECT PUBSUB_SUBSCRIBERS('events')")
  })

  it('should build PUBSUB_CHANNELS query with no args', () => {
    expect(`SELECT PUBSUB_CHANNELS()`).toBe('SELECT PUBSUB_CHANNELS()')
  })

  it('should build PUBSUB_PUBLISH query with escaped payload', () => {
    const name = 'events'
    const msg = "it's a message"
    const sql = `SELECT PUBSUB_PUBLISH('${name}', '${msg.replace(/'/g, "''")}')`
    expect(sql).toBe("SELECT PUBSUB_PUBLISH('events', 'it''s a message')")
  })
})
