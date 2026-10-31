// ---------------------------------------------------------------------------
// CacheClient backed by Nucleus KV model
// ---------------------------------------------------------------------------
//
// When the application connects to Nucleus (instead of Redis), this adapter
// bridges neutron-data's CacheClient interface to the KV model's SQL functions.
// ---------------------------------------------------------------------------

import { counterTTL } from "./counter.js";
import type { CacheClient, CounterCapabilities } from "./index.js";

/**
 * A KV-like interface matching the subset of @neutron-build/nucleus KVModel
 * needed by NucleusCacheClient.
 *
 * We define this locally to avoid a hard dependency on @neutron-build/nucleus
 * from neutron-data (it's a peer dependency).
 */
export interface NucleusKVLike {
  get(key: string): Promise<string | null>;
  set(key: string, value: string, opts?: { ttl?: number; namespace?: string }): Promise<void>;
  delete(key: string): Promise<boolean>;
  incr(key: string, amount?: number): Promise<number>;
  expire(key: string, seconds: number): Promise<boolean>;
  /** Atomic canonical-safe-integer validation and increment, before any mutation.
   * Missing/expired keys start at 1; malformed/unsafe values refuse unchanged. */
  incrChecked?(key: string): Promise<number>;
  /** Same checked increment plus initial expiry; preserve existing expiry and
   * attach one to nonexpiring keys. Unsupported by the bundled KVModel. */
  incrWithExpiry?(key: string, seconds: number): Promise<number>;
}

export interface NucleusCacheClientOptions {
  /** A KV model instance (from `@neutron-build/nucleus`). */
  kv: NucleusKVLike;
  /** Key prefix for all cache entries (default `"cache:"`). */
  prefix?: string;
  /** Native mode supports bundled KV_INCR; it does not validate existing stored
   * text atomically. Strict mode requires a provider's checked primitive. */
  counterMode?: 'native' | 'strict';
}

/**
 * CacheClient implementation backed by Nucleus KV.
 *
 * Stores simple cache operations directly in Nucleus. TTL increments
 * require a KV implementation exposing the atomic incrWithExpiry primitive;
 * clients with only separate incr/expire methods fail before mutation.
 */
export class NucleusCacheClient implements CacheClient {
  private readonly kv: NucleusKVLike;
  private readonly prefix: string;
  private readonly counterMode: 'native' | 'strict';
  readonly counterCapabilities: Readonly<CounterCapabilities>;

  constructor(options: NucleusCacheClientOptions) {
    this.kv = options.kv;
    this.prefix = options.prefix ?? "cache:";
    this.counterMode = options.counterMode ?? 'native';
    if (this.counterMode === 'strict' && !this.kv.incrChecked) throw new Error('Nucleus KV does not expose atomic checked increment');
    this.counterCapabilities = Object.freeze({ plain: this.counterMode === 'strict' ? 'checked' : 'native', atomicTTL: typeof this.kv.incrWithExpiry === 'function' });
  }

  private key(k: string): string {
    return `${this.prefix}${k}`;
  }

  async get(key: string): Promise<string | null> {
    return this.kv.get(this.key(key));
  }

  async set(key: string, value: string, ttlSec?: number): Promise<void> {
    const opts = ttlSec && ttlSec > 0 ? { ttl: ttlSec } : undefined;
    await this.kv.set(this.key(key), value, opts);
  }

  async del(key: string): Promise<void> {
    await this.kv.delete(this.key(key));
  }

  async incr(key: string, ttlSec?: number): Promise<number> {
    const ttl = counterTTL(ttlSec);
    const k = this.key(key);
    if (ttl !== undefined) {
      if (!this.kv.incrWithExpiry) {
        throw new Error("Nucleus KV does not expose atomic increment-with-expiry; incr(key, ttl) is unsupported by this adapter");
      }
      return this.kv.incrWithExpiry(k, ttl);
    }
    if (this.counterMode === 'strict') return this.kv.incrChecked!(k);
    const value = await this.kv.incr(k);
    // This is acknowledgement validation AFTER native mutation, not a claim of
    // checked storage semantics. An unsafe result requires reconciliation.
    if (!Number.isSafeInteger(value)) throw new Error('Native counter result is unsafe; increment may have completed, reconcile before retrying');
    return value;
  }
}

/**
 * Factory function matching the pattern of `createRedisCacheClient`.
 */
export function createNucleusCacheClient(options: NucleusCacheClientOptions): NucleusCacheClient {
  return new NucleusCacheClient(options);
}
