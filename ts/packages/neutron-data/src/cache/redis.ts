import type { CacheClient } from "./index.js";
import { lazyImport } from "../internal/lazy-import.js";

export interface RedisCacheClientOptions {
  url?: string;
  keyPrefix?: string;
  connectTimeoutMs?: number;
}

interface RedisLikeClient {
  get(key: string): Promise<string | null>;
  set(key: string, value: string, mode?: "EX", ttlSec?: number): Promise<unknown>;
  del(key: string): Promise<unknown>;
  incr(key: string): Promise<number>;
  expire(key: string, ttlSec: number): Promise<unknown>;
  quit(): Promise<unknown>;
}

export class RedisCacheClient implements CacheClient {
  constructor(
    private readonly client: RedisLikeClient,
    private readonly keyPrefix: string
  ) {}

  async get(key: string): Promise<string | null> {
    return await this.client.get(this.key(key));
  }

  async set(key: string, value: string, ttlSec?: number): Promise<void> {
    const fullKey = this.key(key);
    if (typeof ttlSec === "number" && ttlSec > 0) {
      await this.client.set(fullKey, value, "EX", ttlSec);
      return;
    }
    await this.client.set(fullKey, value);
  }

  async del(key: string): Promise<void> {
    await this.client.del(this.key(key));
  }

  async incr(key: string, ttlSec?: number): Promise<number> {
    const fullKey = this.key(key);
    const value = await this.client.incr(fullKey);
    if (typeof ttlSec === "number" && ttlSec > 0 && value === 1) {
      await this.client.expire(fullKey, ttlSec);
    }
    return value;
  }

  async close(): Promise<void> {
    await this.client.quit();
  }

  private key(key: string): string {
    return this.keyPrefix ? `${this.keyPrefix}${key}` : key;
  }
}

export async function createRedisCacheClient(
  options: RedisCacheClientOptions = {}
): Promise<RedisCacheClient> {
  const redisModule = await lazyImport<{ default?: new (...args: unknown[]) => RedisLikeClient }>(
    "ioredis",
    "Install with `pnpm add ioredis` (or npm/yarn equivalent)"
  );

  const RedisCtor = redisModule.default;
  if (!RedisCtor) {
    throw new Error("Failed to resolve ioredis default export.");
  }

  const url = options.url || process.env.DRAGONFLY_URL || process.env.REDIS_URL || "redis://127.0.0.1:6379";
  const client = new RedisCtor(url, {
    lazyConnect: false,
    maxRetriesPerRequest: 3,
    connectTimeout: options.connectTimeoutMs ?? 10000,
  });

  return new RedisCacheClient(client, options.keyPrefix || "");
}

