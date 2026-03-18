// ---------------------------------------------------------------------------
// CacheClient backed by Nucleus KV model
// ---------------------------------------------------------------------------
//
// When the application connects to Nucleus (instead of Redis), this adapter
// bridges neutron-data's CacheClient interface to the KV model's SQL functions.
// ---------------------------------------------------------------------------

import type { CacheClient } from "./index.js";

/**
 * A KV-like interface matching the subset of @neutron/nucleus KVModel
 * needed by NucleusCacheClient.
 *
 * We define this locally to avoid a hard dependency on @neutron/nucleus
 * from neutron-data (it's a peer dependency).
 */
export interface NucleusKVLike {
  get(key: string): Promise<string | null>;
  set(key: string, value: string, opts?: { ttl?: number; namespace?: string }): Promise<void>;
  delete(key: string): Promise<boolean>;
  incr(key: string, amount?: number): Promise<number>;
  expire(key: string, seconds: number): Promise<boolean>;
}

export interface NucleusCacheClientOptions {
  /** A KV model instance (from `@neutron/nucleus`). */
  kv: NucleusKVLike;
  /** Key prefix for all cache entries (default `"cache:"`). */
  prefix?: string;
}

/**
 * CacheClient implementation backed by Nucleus KV.
 *
 * This is a drop-in replacement for `MemoryCacheClient` or `RedisCacheClient`
 * that stores data directly in Nucleus, avoiding the need for a separate
 * Redis instance.
 */
export class NucleusCacheClient implements CacheClient {
  private readonly kv: NucleusKVLike;
  private readonly prefix: string;

  constructor(options: NucleusCacheClientOptions) {
    this.kv = options.kv;
    this.prefix = options.prefix ?? "cache:";
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
    const k = this.key(key);
    const next = await this.kv.incr(k);
    if (ttlSec && ttlSec > 0) {
      await this.kv.expire(k, ttlSec);
    }
    return next;
  }
}

/**
 * Factory function matching the pattern of `createRedisCacheClient`.
 */
export function createNucleusCacheClient(options: NucleusCacheClientOptions): NucleusCacheClient {
  return new NucleusCacheClient(options);
}
