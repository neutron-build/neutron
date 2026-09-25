// ---------------------------------------------------------------------------
// @neutron-build/nucleus/kv — KV model plugin
// ---------------------------------------------------------------------------

import type { Transport, NucleusPlugin, NucleusFeatures, QuerySignalOptions, SqlTableIdentity } from '../types.js';
import { requireNucleus, assertIdentifier } from '../helpers.js';

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

/**
 * Scope + cancellation options shared by every KV operation. `namespace` is a
 * client-side key-prefix convention — the engine keeps ONE global keyspace
 * (MODEL_SEMANTICS.md); it is not an isolation boundary and not a SQL object.
 */
export interface KVScopeOptions {
  /** Key namespace prefix (prepended as `namespace:key`). */
  namespace?: string;
  /** Abort the operation. See Transport cancellation semantics. */
  signal?: AbortSignal;
}

export interface KVSetOptions extends KVScopeOptions {
  /** Time-to-live in seconds. */
  ttl?: number;
}

/**
 * A KV namespace bound to a name (and optionally to a SQL table identity).
 * Every key is prefixed `<schema>.<table>:<name>:` (or `<name>:` without an
 * identity), so namespaced keys cannot collide with bare keys or with other
 * bindings. Global operations that cannot be scoped (dbSize, flushDB) are
 * intentionally absent.
 */
export type KVNamespace = Omit<KVModel, 'dbSize' | 'flushDB' | 'namespace'>;

// ---------------------------------------------------------------------------
// KVModel interface
// ---------------------------------------------------------------------------

export interface KVModel {
  // -- Base ------------------------------------------------------------------

  /** Get a raw string value. Returns `null` if the key does not exist. */
  get(key: string, opts?: KVScopeOptions): Promise<string | null>;

  /** Get a value and JSON-parse it into `T`. Returns `null` if missing. */
  getTyped<T>(key: string, opts?: KVScopeOptions): Promise<T | null>;

  /** Set a raw string value. */
  set(key: string, value: string, opts?: KVSetOptions): Promise<void>;

  /** JSON-stringify `value` and store it. */
  setTyped<T>(key: string, value: T, opts?: KVSetOptions): Promise<void>;

  /**
   * Set the key only if it does not already exist. Returns `true` if set.
   * With `ttl`, value and expiry commit atomically (Redis `SET NX EX`) —
   * the crash-safe lock acquire. `namespace` prefixes the key like every
   * other write, so namespaced callers get namespaced locks.
   */
  setNX(key: string, value: string, opts?: { ttl?: number } & KVScopeOptions): Promise<boolean>;

  /** Delete a key. Returns `true` if it existed. */
  delete(key: string, opts?: KVScopeOptions): Promise<boolean>;

  /**
   * Delete the key only if its current value equals `expected` — the safe
   * lock release (a holder whose lease expired cannot delete the next
   * holder's lock). Returns `true` if deleted.
   */
  cdel(key: string, expected: string, opts?: KVScopeOptions): Promise<boolean>;

  /**
   * Set the TTL only if the current value equals `expected` — the lease
   * renewal heartbeat. Returns `true` if renewed.
   */
  cexpire(key: string, expected: string, seconds: number, opts?: KVScopeOptions): Promise<boolean>;

  /** Check whether a key exists. */
  exists(key: string, opts?: KVScopeOptions): Promise<boolean>;

  /** Atomically increment a key's integer value. Returns the new value. */
  incr(key: string, amount?: number, opts?: KVScopeOptions): Promise<number>;

  /** Get the remaining TTL in seconds. -1 = no TTL, -2 = missing key. */
  ttl(key: string, opts?: KVScopeOptions): Promise<number>;

  /** Set a TTL on an existing key. Returns `true` if the key existed. */
  expire(key: string, seconds: number, opts?: KVScopeOptions): Promise<boolean>;

  /** Return the total number of keys. */
  dbSize(): Promise<number>;

  /** Delete all keys. */
  flushDB(): Promise<void>;

  /**
   * Create a namespace-bound view of this KV model. Keys written through the
   * returned object are scoped `<schema>.<table>:<name>:` (or `<name>:`
   * without `boundTo`). This is a resource-reference binding to a SQL
   * identity — it emits no DDL, creates no SQL columns, and makes no
   * cross-model atomicity claim.
   */
  namespace(name: string, boundTo?: SqlTableIdentity): KVNamespace;

  /** Scan keys matching a glob pattern (`*` wildcard). Returns matching keys, sorted. */
  scan(pattern: string, count?: number, opts?: KVScopeOptions): Promise<string[]>;

  // -- Lists -----------------------------------------------------------------

  /** Prepend a value to a list. Returns the new list length. */
  lpush(key: string, value: string, opts?: KVScopeOptions): Promise<number>;

  /** Append a value to a list. Returns the new list length. */
  rpush(key: string, value: string, opts?: KVScopeOptions): Promise<number>;

  /** Remove and return the first element of a list. */
  lpop(key: string, opts?: KVScopeOptions): Promise<string | null>;

  /** Remove and return the last element of a list. */
  rpop(key: string, opts?: KVScopeOptions): Promise<string | null>;

  /** Return elements between `start` and `stop` (inclusive). */
  lrange(key: string, start: number, stop: number, opts?: KVScopeOptions): Promise<string[]>;

  /** Return the length of a list. */
  llen(key: string, opts?: KVScopeOptions): Promise<number>;

  /** Return the element at `index`. */
  lindex(key: string, index: number, opts?: KVScopeOptions): Promise<string | null>;

  // -- Hashes ----------------------------------------------------------------

  /** Set a field in a hash. Returns `true` if the field is new. */
  hset(key: string, field: string, value: string, opts?: KVScopeOptions): Promise<boolean>;

  /** Get a hash field value. */
  hget(key: string, field: string, opts?: KVScopeOptions): Promise<string | null>;

  /** Remove a field from a hash. Returns `true` if removed. */
  hdel(key: string, field: string, opts?: KVScopeOptions): Promise<boolean>;

  /** Check if a field exists in a hash. */
  hexists(key: string, field: string, opts?: KVScopeOptions): Promise<boolean>;

  /** Return all fields and values of a hash. */
  hgetall(key: string, opts?: KVScopeOptions): Promise<Record<string, string>>;

  /** Return the number of fields in a hash. */
  hlen(key: string, opts?: KVScopeOptions): Promise<number>;

  // -- Sets ------------------------------------------------------------------

  /** Add a member to a set. Returns `true` if it was new. */
  sadd(key: string, member: string, opts?: KVScopeOptions): Promise<boolean>;

  /** Remove a member from a set. Returns `true` if removed. */
  srem(key: string, member: string, opts?: KVScopeOptions): Promise<boolean>;

  /** Return all members of a set. */
  smembers(key: string, opts?: KVScopeOptions): Promise<string[]>;

  /** Check if a member exists in a set. */
  sismember(key: string, member: string, opts?: KVScopeOptions): Promise<boolean>;

  /** Return the number of members in a set. */
  scard(key: string, opts?: KVScopeOptions): Promise<number>;

  // -- Sorted Sets -----------------------------------------------------------

  /** Add a member with a score to a sorted set. Returns `true` if new. */
  zadd(key: string, score: number, member: string, opts?: KVScopeOptions): Promise<boolean>;

  /** Return members by rank range. */
  zrange(key: string, start: number, stop: number, opts?: KVScopeOptions): Promise<string[]>;

  /** Return members with scores between `min` and `max`. */
  zrangeByScore(key: string, min: number, max: number, opts?: KVScopeOptions): Promise<string[]>;

  /** Remove a member from a sorted set. Returns `true` if removed. */
  zrem(key: string, member: string, opts?: KVScopeOptions): Promise<boolean>;

  /** Return the number of members in a sorted set. */
  zcard(key: string, opts?: KVScopeOptions): Promise<number>;

  // -- HyperLogLog -----------------------------------------------------------

  /** Add an element to a HyperLogLog. Returns `true` if the internal state changed. */
  pfadd(key: string, element: string, opts?: KVScopeOptions): Promise<boolean>;

  /** Return the approximate cardinality. */
  pfcount(key: string, opts?: KVScopeOptions): Promise<number>;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

function resolveKey(key: string, namespace?: string, prefix?: string): string {
  let k = namespace ? `${namespace}:${key}` : key;
  if (prefix) k = `${prefix}${k}`;
  return k;
}

class KVModelImpl implements KVModel {
  constructor(
    private readonly transport: Transport,
    private readonly features: NucleusFeatures,
    /** Fixed scope prefix for namespace-bound views; undefined on the root model. */
    private readonly scopePrefix?: string,
  ) {}

  private require(): void {
    requireNucleus(this.features, 'KV');
  }

  namespace(name: string, boundTo?: SqlTableIdentity): KVNamespace {
    if (boundTo != null) {
      assertIdentifier(boundTo.schema, 'namespace schema');
      assertIdentifier(boundTo.table, 'namespace table');
    }
    if (name.includes(':')) {
      throw new Error(
        `Invalid KV namespace name ${JSON.stringify(name)}: ':' would make scoped keys ambiguous`,
      );
    }
    const prefix = boundTo ? `${boundTo.schema}.${boundTo.table}:${name}:` : `${name}:`;
    // Pure client-side handle: binding emits no SQL at all (no DDL, no
    // catalog objects) — a resource reference, never a SQL column.
    const scoped = new KVModelImpl(this.transport, this.features, (this.scopePrefix ?? '') + prefix);
    return scoped as unknown as KVNamespace;
  }

  // -- Base ------------------------------------------------------------------

  async get(key: string, opts?: KVScopeOptions): Promise<string | null> {
    this.require();
    return this.transport.fetchval<string>('SELECT KV_GET($1)', [resolveKey(key, opts?.namespace, this.scopePrefix)], { signal: opts?.signal });
  }

  async getTyped<T>(key: string, opts?: KVScopeOptions): Promise<T | null> {
    const raw = await this.get(key, opts);
    if (raw === null) return null;
    return JSON.parse(raw) as T;
  }

  async set(key: string, value: string, opts?: KVSetOptions): Promise<void> {
    this.require();
    const k = resolveKey(key, opts?.namespace, this.scopePrefix);
    const q = { signal: opts?.signal };
    if (opts?.ttl !== undefined) {
      await this.transport.execute('SELECT KV_SET($1, $2, $3)', [k, value, opts.ttl], q);
    } else {
      await this.transport.execute('SELECT KV_SET($1, $2)', [k, value], q);
    }
  }

  async setTyped<T>(key: string, value: T, opts?: KVSetOptions): Promise<void> {
    await this.set(key, JSON.stringify(value), opts);
  }

  async setNX(key: string, value: string, opts?: { ttl?: number } & KVScopeOptions): Promise<boolean> {
    this.require();
    const k = resolveKey(key, opts?.namespace, this.scopePrefix);
    const q = { signal: opts?.signal };
    if (opts?.ttl !== undefined) {
      return (
        (await this.transport.fetchval<boolean>('SELECT KV_SETNX($1, $2, $3)', [k, value, opts.ttl], q)) ?? false
      );
    }
    return (await this.transport.fetchval<boolean>('SELECT KV_SETNX($1, $2)', [k, value], q)) ?? false;
  }

  async delete(key: string, opts?: KVScopeOptions): Promise<boolean> {
    this.require();
    return (
      (await this.transport.fetchval<boolean>('SELECT KV_DEL($1)', [resolveKey(key, opts?.namespace, this.scopePrefix)], { signal: opts?.signal }))
    ) ?? false;
  }

  async cdel(key: string, expected: string, opts?: KVScopeOptions): Promise<boolean> {
    this.require();
    return (
      (await this.transport.fetchval<boolean>('SELECT KV_CDEL($1, $2)', [resolveKey(key, opts?.namespace, this.scopePrefix), expected], { signal: opts?.signal }))
    ) ?? false;
  }

  async cexpire(key: string, expected: string, seconds: number, opts?: KVScopeOptions): Promise<boolean> {
    this.require();
    return (
      (await this.transport.fetchval<boolean>('SELECT KV_CEXPIRE($1, $2, $3)', [resolveKey(key, opts?.namespace, this.scopePrefix), expected, seconds], { signal: opts?.signal }))
    ) ?? false;
  }

  async exists(key: string, opts?: KVScopeOptions): Promise<boolean> {
    this.require();
    return (
      (await this.transport.fetchval<boolean>('SELECT KV_EXISTS($1)', [resolveKey(key, opts?.namespace, this.scopePrefix)], { signal: opts?.signal }))
    ) ?? false;
  }

  async incr(key: string, amount?: number, opts?: KVScopeOptions): Promise<number> {
    this.require();
    const k = resolveKey(key, opts?.namespace, this.scopePrefix);
    const q = { signal: opts?.signal };
    if (amount !== undefined) {
      return (await this.transport.fetchval<number>('SELECT KV_INCR($1, $2)', [k, amount], q)) ?? 0;
    }
    return (await this.transport.fetchval<number>('SELECT KV_INCR($1)', [k], q)) ?? 0;
  }

  /**
   * Remaining TTL in seconds. Engine semantics (verified live, X04): the
   * value TRUNCATES toward zero (Redis rounds up), so a key set with
   * ttl=10 usually reports 9 immediately, and ttl=1 reports 0 right after
   * the set — 0 therefore does NOT mean expired; only a read returning
   * null / -2 means gone.
   */
  async ttl(key: string, opts?: KVScopeOptions): Promise<number> {
    this.require();
    return (
      (await this.transport.fetchval<number>('SELECT KV_TTL($1)', [resolveKey(key, opts?.namespace, this.scopePrefix)], { signal: opts?.signal }))
    ) ?? -2;
  }

  async expire(key: string, seconds: number, opts?: KVScopeOptions): Promise<boolean> {
    this.require();
    return (
      (await this.transport.fetchval<boolean>('SELECT KV_EXPIRE($1, $2)', [resolveKey(key, opts?.namespace, this.scopePrefix), seconds], { signal: opts?.signal }))
    ) ?? false;
  }

  async dbSize(): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT KV_DBSIZE()')) ?? 0;
  }

  async flushDB(): Promise<void> {
    this.require();
    await this.transport.execute('SELECT KV_FLUSHDB()');
  }

  async scan(pattern: string, count = 100, opts?: KVScopeOptions): Promise<string[]> {
    this.require();
    const raw = await this.transport.fetchval<string>(
      'SELECT KV_KEYS($1)',
      [resolveKey(pattern, opts?.namespace, this.scopePrefix)],
      { signal: opts?.signal },
    );
    if (!raw) return [];
    const keys = JSON.parse(raw) as string[];
    // Strip every prefix this call added: the bound scope AND a per-call
    // namespace — the same contract the root model gives a per-call
    // namespace (X04 LOW: the two surfaces must not disagree).
    const strip = (this.scopePrefix ?? '') + (opts?.namespace ? `${opts.namespace}:` : '');
    return keys.slice(0, count).map((k) => (strip && k.startsWith(strip) ? k.slice(strip.length) : k));
  }

  // -- Lists -----------------------------------------------------------------

  async lpush(key: string, value: string, opts?: KVScopeOptions): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT KV_LPUSH($1, $2)', [resolveKey(key, opts?.namespace, this.scopePrefix), value], { signal: opts?.signal })) ?? 0;
  }

  async rpush(key: string, value: string, opts?: KVScopeOptions): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT KV_RPUSH($1, $2)', [resolveKey(key, opts?.namespace, this.scopePrefix), value], { signal: opts?.signal })) ?? 0;
  }

  async lpop(key: string, opts?: KVScopeOptions): Promise<string | null> {
    this.require();
    return this.transport.fetchval<string>('SELECT KV_LPOP($1)', [resolveKey(key, opts?.namespace, this.scopePrefix)], { signal: opts?.signal });
  }

  async rpop(key: string, opts?: KVScopeOptions): Promise<string | null> {
    this.require();
    return this.transport.fetchval<string>('SELECT KV_RPOP($1)', [resolveKey(key, opts?.namespace, this.scopePrefix)], { signal: opts?.signal });
  }

  async lrange(key: string, start: number, stop: number, opts?: KVScopeOptions): Promise<string[]> {
    this.require();
    const raw = await this.transport.fetchval<string>('SELECT KV_LRANGE($1, $2, $3)', [resolveKey(key, opts?.namespace, this.scopePrefix), start, stop], { signal: opts?.signal });
    if (!raw) return [];
    return JSON.parse(raw) as string[];
  }

  async llen(key: string, opts?: KVScopeOptions): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT KV_LLEN($1)', [resolveKey(key, opts?.namespace, this.scopePrefix)], { signal: opts?.signal })) ?? 0;
  }

  async lindex(key: string, index: number, opts?: KVScopeOptions): Promise<string | null> {
    this.require();
    return this.transport.fetchval<string>('SELECT KV_LINDEX($1, $2)', [resolveKey(key, opts?.namespace, this.scopePrefix), index], { signal: opts?.signal });
  }

  // -- Hashes ----------------------------------------------------------------

  async hset(key: string, field: string, value: string, opts?: KVScopeOptions): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT KV_HSET($1, $2, $3)', [resolveKey(key, opts?.namespace, this.scopePrefix), field, value], { signal: opts?.signal })) ?? false;
  }

  async hget(key: string, field: string, opts?: KVScopeOptions): Promise<string | null> {
    this.require();
    return this.transport.fetchval<string>('SELECT KV_HGET($1, $2)', [resolveKey(key, opts?.namespace, this.scopePrefix), field], { signal: opts?.signal });
  }

  async hdel(key: string, field: string, opts?: KVScopeOptions): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT KV_HDEL($1, $2)', [resolveKey(key, opts?.namespace, this.scopePrefix), field], { signal: opts?.signal })) ?? false;
  }

  async hexists(key: string, field: string, opts?: KVScopeOptions): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT KV_HEXISTS($1, $2)', [resolveKey(key, opts?.namespace, this.scopePrefix), field], { signal: opts?.signal })) ?? false;
  }

  async hgetall(key: string, opts?: KVScopeOptions): Promise<Record<string, string>> {
    this.require();
    const raw = await this.transport.fetchval<string>('SELECT KV_HGETALL($1)', [resolveKey(key, opts?.namespace, this.scopePrefix)], { signal: opts?.signal });
    if (!raw) return {};
    const result: Record<string, string> = {};
    for (const [field, value] of JSON.parse(raw) as Array<[string, string]>) {
      result[field] = value;
    }
    return result;
  }

  async hlen(key: string, opts?: KVScopeOptions): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT KV_HLEN($1)', [resolveKey(key, opts?.namespace, this.scopePrefix)], { signal: opts?.signal })) ?? 0;
  }

  // -- Sets ------------------------------------------------------------------

  async sadd(key: string, member: string, opts?: KVScopeOptions): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT KV_SADD($1, $2)', [resolveKey(key, opts?.namespace, this.scopePrefix), member], { signal: opts?.signal })) ?? false;
  }

  async srem(key: string, member: string, opts?: KVScopeOptions): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT KV_SREM($1, $2)', [resolveKey(key, opts?.namespace, this.scopePrefix), member], { signal: opts?.signal })) ?? false;
  }

  async smembers(key: string, opts?: KVScopeOptions): Promise<string[]> {
    this.require();
    const raw = await this.transport.fetchval<string>('SELECT KV_SMEMBERS($1)', [resolveKey(key, opts?.namespace, this.scopePrefix)], { signal: opts?.signal });
    if (!raw) return [];
    return JSON.parse(raw) as string[];
  }

  async sismember(key: string, member: string, opts?: KVScopeOptions): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT KV_SISMEMBER($1, $2)', [resolveKey(key, opts?.namespace, this.scopePrefix), member], { signal: opts?.signal })) ?? false;
  }

  async scard(key: string, opts?: KVScopeOptions): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT KV_SCARD($1)', [resolveKey(key, opts?.namespace, this.scopePrefix)], { signal: opts?.signal })) ?? 0;
  }

  // -- Sorted Sets -----------------------------------------------------------

  async zadd(key: string, score: number, member: string, opts?: KVScopeOptions): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT KV_ZADD($1, $2, $3)', [resolveKey(key, opts?.namespace, this.scopePrefix), score, member], { signal: opts?.signal })) ?? false;
  }

  async zrange(key: string, start: number, stop: number, opts?: KVScopeOptions): Promise<string[]> {
    this.require();
    const raw = await this.transport.fetchval<string>('SELECT KV_ZRANGE($1, $2, $3)', [resolveKey(key, opts?.namespace, this.scopePrefix), start, stop], { signal: opts?.signal });
    if (!raw) return [];
    return (JSON.parse(raw) as Array<[string, number]>).map(([member]) => member);
  }

  async zrangeByScore(key: string, min: number, max: number, opts?: KVScopeOptions): Promise<string[]> {
    this.require();
    const raw = await this.transport.fetchval<string>('SELECT KV_ZRANGEBYSCORE($1, $2, $3)', [resolveKey(key, opts?.namespace, this.scopePrefix), min, max], { signal: opts?.signal });
    if (!raw) return [];
    return (JSON.parse(raw) as Array<[string, number]>).map(([member]) => member);
  }

  async zrem(key: string, member: string, opts?: KVScopeOptions): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT KV_ZREM($1, $2)', [resolveKey(key, opts?.namespace, this.scopePrefix), member], { signal: opts?.signal })) ?? false;
  }

  async zcard(key: string, opts?: KVScopeOptions): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT KV_ZCARD($1)', [resolveKey(key, opts?.namespace, this.scopePrefix)], { signal: opts?.signal })) ?? 0;
  }

  // -- HyperLogLog -----------------------------------------------------------

  async pfadd(key: string, element: string, opts?: KVScopeOptions): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT KV_PFADD($1, $2)', [resolveKey(key, opts?.namespace, this.scopePrefix), element], { signal: opts?.signal })) ?? false;
  }

  async pfcount(key: string, opts?: KVScopeOptions): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT KV_PFCOUNT($1)', [resolveKey(key, opts?.namespace, this.scopePrefix)], { signal: opts?.signal })) ?? 0;
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
