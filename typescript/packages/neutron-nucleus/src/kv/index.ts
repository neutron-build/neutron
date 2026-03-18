// ---------------------------------------------------------------------------
// @neutron/nucleus/kv — KV model plugin
// ---------------------------------------------------------------------------

import type { Transport, NucleusPlugin, NucleusFeatures } from '../types.js';
import { requireNucleus } from '../helpers.js';

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

export interface KVSetOptions {
  /** Time-to-live in seconds. */
  ttl?: number;
  /** Key namespace prefix (prepended as `namespace:key`). */
  namespace?: string;
}

// ---------------------------------------------------------------------------
// KVModel interface
// ---------------------------------------------------------------------------

export interface KVModel {
  // -- Base ------------------------------------------------------------------

  /** Get a raw string value. Returns `null` if the key does not exist. */
  get(key: string): Promise<string | null>;

  /** Get a value and JSON-parse it into `T`. Returns `null` if missing. */
  getTyped<T>(key: string): Promise<T | null>;

  /** Set a raw string value. */
  set(key: string, value: string, opts?: KVSetOptions): Promise<void>;

  /** JSON-stringify `value` and store it. */
  setTyped<T>(key: string, value: T, opts?: KVSetOptions): Promise<void>;

  /** Set the key only if it does not already exist. Returns `true` if set. */
  setNX(key: string, value: string): Promise<boolean>;

  /** Delete a key. Returns `true` if it existed. */
  delete(key: string): Promise<boolean>;

  /** Check whether a key exists. */
  exists(key: string): Promise<boolean>;

  /** Atomically increment a key's integer value. Returns the new value. */
  incr(key: string, amount?: number): Promise<number>;

  /** Get the remaining TTL in seconds. -1 = no TTL, -2 = missing key. */
  ttl(key: string): Promise<number>;

  /** Set a TTL on an existing key. Returns `true` if the key existed. */
  expire(key: string, seconds: number): Promise<boolean>;

  /** Return the total number of keys. */
  dbSize(): Promise<number>;

  /** Delete all keys. */
  flushDB(): Promise<void>;

  /** Scan keys matching a pattern. Returns matching key-value pairs. */
  scan(pattern: string, count?: number): Promise<Array<{ key: string; value: string }>>;

  // -- Lists -----------------------------------------------------------------

  /** Prepend a value to a list. Returns the new list length. */
  lpush(key: string, value: string): Promise<number>;

  /** Append a value to a list. Returns the new list length. */
  rpush(key: string, value: string): Promise<number>;

  /** Remove and return the first element of a list. */
  lpop(key: string): Promise<string | null>;

  /** Remove and return the last element of a list. */
  rpop(key: string): Promise<string | null>;

  /** Return elements between `start` and `stop` (inclusive). */
  lrange(key: string, start: number, stop: number): Promise<string[]>;

  /** Return the length of a list. */
  llen(key: string): Promise<number>;

  /** Return the element at `index`. */
  lindex(key: string, index: number): Promise<string | null>;

  // -- Hashes ----------------------------------------------------------------

  /** Set a field in a hash. Returns `true` if the field is new. */
  hset(key: string, field: string, value: string): Promise<boolean>;

  /** Get a hash field value. */
  hget(key: string, field: string): Promise<string | null>;

  /** Remove a field from a hash. Returns `true` if removed. */
  hdel(key: string, field: string): Promise<boolean>;

  /** Check if a field exists in a hash. */
  hexists(key: string, field: string): Promise<boolean>;

  /** Return all fields and values of a hash. */
  hgetall(key: string): Promise<Record<string, string>>;

  /** Return the number of fields in a hash. */
  hlen(key: string): Promise<number>;

  // -- Sets ------------------------------------------------------------------

  /** Add a member to a set. Returns `true` if it was new. */
  sadd(key: string, member: string): Promise<boolean>;

  /** Remove a member from a set. Returns `true` if removed. */
  srem(key: string, member: string): Promise<boolean>;

  /** Return all members of a set. */
  smembers(key: string): Promise<string[]>;

  /** Check if a member exists in a set. */
  sismember(key: string, member: string): Promise<boolean>;

  /** Return the number of members in a set. */
  scard(key: string): Promise<number>;

  // -- Sorted Sets -----------------------------------------------------------

  /** Add a member with a score to a sorted set. Returns `true` if new. */
  zadd(key: string, score: number, member: string): Promise<boolean>;

  /** Return members by rank range. */
  zrange(key: string, start: number, stop: number): Promise<string[]>;

  /** Return members with scores between `min` and `max`. */
  zrangeByScore(key: string, min: number, max: number): Promise<string[]>;

  /** Remove a member from a sorted set. Returns `true` if removed. */
  zrem(key: string, member: string): Promise<boolean>;

  /** Return the number of members in a sorted set. */
  zcard(key: string): Promise<number>;

  // -- HyperLogLog -----------------------------------------------------------

  /** Add an element to a HyperLogLog. Returns `true` if the internal state changed. */
  pfadd(key: string, element: string): Promise<boolean>;

  /** Return the approximate cardinality. */
  pfcount(key: string): Promise<number>;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

function resolveKey(key: string, namespace?: string): string {
  return namespace ? `${namespace}:${key}` : key;
}

class KVModelImpl implements KVModel {
  constructor(
    private readonly transport: Transport,
    private readonly features: NucleusFeatures,
  ) {}

  private require(): void {
    requireNucleus(this.features, 'KV');
  }

  // -- Base ------------------------------------------------------------------

  async get(key: string): Promise<string | null> {
    this.require();
    return this.transport.fetchval<string>('SELECT KV_GET($1)', [key]);
  }

  async getTyped<T>(key: string): Promise<T | null> {
    const raw = await this.get(key);
    if (raw === null) return null;
    return JSON.parse(raw) as T;
  }

  async set(key: string, value: string, opts?: KVSetOptions): Promise<void> {
    this.require();
    const k = resolveKey(key, opts?.namespace);
    if (opts?.ttl !== undefined) {
      await this.transport.execute('SELECT KV_SET($1, $2, $3)', [k, value, opts.ttl]);
    } else {
      await this.transport.execute('SELECT KV_SET($1, $2)', [k, value]);
    }
  }

  async setTyped<T>(key: string, value: T, opts?: KVSetOptions): Promise<void> {
    await this.set(key, JSON.stringify(value), opts);
  }

  async setNX(key: string, value: string): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT KV_SETNX($1, $2)', [key, value])) ?? false;
  }

  async delete(key: string): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT KV_DEL($1)', [key])) ?? false;
  }

  async exists(key: string): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT KV_EXISTS($1)', [key])) ?? false;
  }

  async incr(key: string, amount?: number): Promise<number> {
    this.require();
    if (amount !== undefined) {
      return (await this.transport.fetchval<number>('SELECT KV_INCR($1, $2)', [key, amount])) ?? 0;
    }
    return (await this.transport.fetchval<number>('SELECT KV_INCR($1)', [key])) ?? 0;
  }

  async ttl(key: string): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT KV_TTL($1)', [key])) ?? -2;
  }

  async expire(key: string, seconds: number): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT KV_EXPIRE($1, $2)', [key, seconds])) ?? false;
  }

  async dbSize(): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT KV_DBSIZE()')) ?? 0;
  }

  async flushDB(): Promise<void> {
    this.require();
    await this.transport.execute('SELECT KV_FLUSHDB()');
  }

  async scan(pattern: string, count = 100): Promise<Array<{ key: string; value: string }>> {
    this.require();
    const result = await this.transport.query<{ key: string; value: string }>(
      'SELECT KV_SCAN($1, $2)',
      [pattern, count],
    );
    return result.rows;
  }

  // -- Lists -----------------------------------------------------------------

  async lpush(key: string, value: string): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT KV_LPUSH($1, $2)', [key, value])) ?? 0;
  }

  async rpush(key: string, value: string): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT KV_RPUSH($1, $2)', [key, value])) ?? 0;
  }

  async lpop(key: string): Promise<string | null> {
    this.require();
    return this.transport.fetchval<string>('SELECT KV_LPOP($1)', [key]);
  }

  async rpop(key: string): Promise<string | null> {
    this.require();
    return this.transport.fetchval<string>('SELECT KV_RPOP($1)', [key]);
  }

  async lrange(key: string, start: number, stop: number): Promise<string[]> {
    this.require();
    const raw = await this.transport.fetchval<string>('SELECT KV_LRANGE($1, $2, $3)', [key, start, stop]);
    if (!raw) return [];
    return raw.split(',');
  }

  async llen(key: string): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT KV_LLEN($1)', [key])) ?? 0;
  }

  async lindex(key: string, index: number): Promise<string | null> {
    this.require();
    return this.transport.fetchval<string>('SELECT KV_LINDEX($1, $2)', [key, index]);
  }

  // -- Hashes ----------------------------------------------------------------

  async hset(key: string, field: string, value: string): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT KV_HSET($1, $2, $3)', [key, field, value])) ?? false;
  }

  async hget(key: string, field: string): Promise<string | null> {
    this.require();
    return this.transport.fetchval<string>('SELECT KV_HGET($1, $2)', [key, field]);
  }

  async hdel(key: string, field: string): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT KV_HDEL($1, $2)', [key, field])) ?? false;
  }

  async hexists(key: string, field: string): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT KV_HEXISTS($1, $2)', [key, field])) ?? false;
  }

  async hgetall(key: string): Promise<Record<string, string>> {
    this.require();
    const raw = await this.transport.fetchval<string>('SELECT KV_HGETALL($1)', [key]);
    if (!raw) return {};
    const result: Record<string, string> = {};
    for (const pair of raw.split(',')) {
      const eqIdx = pair.indexOf('=');
      if (eqIdx !== -1) {
        result[pair.slice(0, eqIdx)] = pair.slice(eqIdx + 1);
      }
    }
    return result;
  }

  async hlen(key: string): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT KV_HLEN($1)', [key])) ?? 0;
  }

  // -- Sets ------------------------------------------------------------------

  async sadd(key: string, member: string): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT KV_SADD($1, $2)', [key, member])) ?? false;
  }

  async srem(key: string, member: string): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT KV_SREM($1, $2)', [key, member])) ?? false;
  }

  async smembers(key: string): Promise<string[]> {
    this.require();
    const raw = await this.transport.fetchval<string>('SELECT KV_SMEMBERS($1)', [key]);
    if (!raw) return [];
    return raw.split(',');
  }

  async sismember(key: string, member: string): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT KV_SISMEMBER($1, $2)', [key, member])) ?? false;
  }

  async scard(key: string): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT KV_SCARD($1)', [key])) ?? 0;
  }

  // -- Sorted Sets -----------------------------------------------------------

  async zadd(key: string, score: number, member: string): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT KV_ZADD($1, $2, $3)', [key, score, member])) ?? false;
  }

  async zrange(key: string, start: number, stop: number): Promise<string[]> {
    this.require();
    const raw = await this.transport.fetchval<string>('SELECT KV_ZRANGE($1, $2, $3)', [key, start, stop]);
    if (!raw) return [];
    return raw.split(',');
  }

  async zrangeByScore(key: string, min: number, max: number): Promise<string[]> {
    this.require();
    const raw = await this.transport.fetchval<string>('SELECT KV_ZRANGEBYSCORE($1, $2, $3)', [key, min, max]);
    if (!raw) return [];
    return raw.split(',');
  }

  async zrem(key: string, member: string): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT KV_ZREM($1, $2)', [key, member])) ?? false;
  }

  async zcard(key: string): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT KV_ZCARD($1)', [key])) ?? 0;
  }

  // -- HyperLogLog -----------------------------------------------------------

  async pfadd(key: string, element: string): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT KV_PFADD($1, $2)', [key, element])) ?? false;
  }

  async pfcount(key: string): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT KV_PFCOUNT($1)', [key])) ?? 0;
  }
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

/** Plugin: adds `.kv` to the client. */
export const withKV: NucleusPlugin<{ kv: KVModel }> = {
  name: 'kv',
  init(transport: Transport, features: NucleusFeatures) {
    return { kv: new KVModelImpl(transport, features) };
  },
};
