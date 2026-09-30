import { randomUUID } from "node:crypto";
import {
  deserializeTransportData,
  serializeTransportData,
  type NeutronAppCacheStore,
  type NeutronAppResponseCacheEntry,
  type NeutronCacheStores,
  type NeutronLoaderCacheStore,
  type NeutronLoaderDataCacheEntry,
} from "@neutron-build/core";

export interface RedisNeutronCacheOptions {
  url?: string;
  keyPrefix?: string;
  connectTimeoutMs?: number;
}

export interface RedisLikeClient {
  /** Required: generation checks and mutations execute in one Redis script. */
  eval(script: string, numberOfKeys: number, ...args: Array<string | number>): Promise<unknown>;
  get(key: string): Promise<string | null>;
  set(key: string, value: string, mode?: "EX", ttlSec?: number): Promise<unknown>;
  del(...keys: string[]): Promise<unknown>;
  sadd(key: string, ...members: string[]): Promise<unknown>;
  smembers(key: string): Promise<string[]>;
  expire(key: string, ttlSec: number): Promise<unknown>;
  keys(pattern: string): Promise<string[]>;
  scan?(
    cursor: string,
    ...args: Array<string | number>
  ): Promise<[nextCursor: string, keys: string[]]>;
  /** Optional inspection helper. Cache index TTL changes run inside EVAL. */
  ttl?(key: string): Promise<number>;
  /** Optional legacy client helper; invalidation now runs entirely in EVAL. */
  rename?(source: string, destination: string): Promise<unknown>;
  quit(): Promise<unknown>;
}

export interface RedisNeutronCacheStores extends NeutronCacheStores {
  app: NeutronAppCacheStore;
  loader: NeutronLoaderCacheStore;
  close(): Promise<void>;
}

export function createRedisNeutronCacheStoresFromClient(
  client: RedisLikeClient,
  options: Pick<RedisNeutronCacheOptions, "keyPrefix"> = {}
): RedisNeutronCacheStores {
  if (typeof client.eval !== "function") throw new TypeError("Redis cache clients require atomic EVAL support");
  const keyPrefix = options.keyPrefix || "neutron:";
  const app = createAppCacheStore(client, keyPrefix);
  const loader = createLoaderCacheStore(client, keyPrefix);

  return {
    app,
    loader,
    close: async () => {
      await client.quit();
    },
  };
}

export async function createRedisNeutronCacheStores(
  options: RedisNeutronCacheOptions = {}
): Promise<RedisNeutronCacheStores> {
  const redisModule = await lazyImport<{ default?: new (...args: unknown[]) => RedisLikeClient }>(
    "ioredis",
    "Install with `pnpm add ioredis` (or npm/yarn equivalent)"
  );

  if (!redisModule.default) {
    throw new Error("Failed to resolve ioredis default export.");
  }

  const RedisCtor = redisModule.default;
  const url =
    options.url ||
    process.env.DRAGONFLY_URL ||
    process.env.REDIS_URL ||
    "redis://127.0.0.1:6379";
  const keyPrefix = options.keyPrefix || "neutron:";
  const client = new RedisCtor(url, {
    lazyConnect: false,
    maxRetriesPerRequest: 3,
    connectTimeout: options.connectTimeoutMs ?? 10000,
  });

  return createRedisNeutronCacheStoresFromClient(client, { keyPrefix });
}

// Opaque epochs prevent old tokens becoming valid after control-key eviction.
const INITIALIZE = `
local generation = redis.call('HGET', KEYS[1], 'generation')
local epoch = redis.call('HGET', KEYS[1], 'epoch')
if not generation or not epoch then
  generation = ARGV[1]
  epoch = ARGV[1]
  redis.call('HSET', KEYS[1], 'generation', generation, 'epoch', epoch)
end
`;
const GENERATION = INITIALIZE + `return generation`;
const READ = INITIALIZE + `
local raw = redis.call('GET', KEYS[2])
if not raw then return false end
local ok, entry = pcall(cjson.decode, raw)
if not ok or type(entry) ~= 'table' or entry.neutronCacheV ~= 1 or entry.epoch ~= epoch then return false end
return entry.payload
`;
const PUBLISH = INITIALIZE + `
if ARGV[2] ~= '' and ARGV[2] ~= generation then return 0 end
local ttl = tonumber(ARGV[4])
if ttl <= 0 then return 0 end
redis.call('SET', KEYS[2], cjson.encode({neutronCacheV=1, epoch=epoch, payload=ARGV[3]}), 'EX', ttl)
redis.call('SADD', KEYS[3], KEYS[2])
local desired = math.max(ttl, 60)
if redis.call('TTL', KEYS[3]) < desired then redis.call('EXPIRE', KEYS[3], desired) end
return 1
`;
const INVALIDATE = INITIALIZE + `
redis.call('HSET', KEYS[1], 'generation', ARGV[2])
for _, key in ipairs(redis.call('SMEMBERS', KEYS[2])) do redis.call('DEL', key) end
redis.call('DEL', KEYS[2])
return 1
`;
const CLEAR = INITIALIZE + `
redis.call('HSET', KEYS[1], 'generation', ARGV[2], 'epoch', ARGV[2])
return 1
`;

function createStore<T extends { expiresAt: number }>(
  client: RedisLikeClient, prefix: string, kind: "app" | "ldr", pathFromKey: (key: string) => string
) {
  const controlKey = `${prefix}control:${kind}`;
  const entryKey = (key: string) => `${prefix}${kind}:${key}`;
  const indexKey = (pathname: string) => `${prefix}idx:${kind}:${pathname}`;
  async function publish(key: string, entry: T, expected: string): Promise<boolean> {
    return Number(await client.eval(PUBLISH, 3, controlKey, entryKey(key), indexKey(pathFromKey(key)),
      randomUUID(), expected, serializeTransportData(entry), ttlFromExpiresAt(entry.expiresAt))) === 1;
  }
  return {
    async get(key: string): Promise<T | null> {
      const raw = await client.eval(READ, 2, controlKey, entryKey(key), randomUUID());
      if (typeof raw !== "string") return null;
      const entry = deserializeTransportData<T>(raw);
      // A newer writer may replace an expired read; never delete it here.
      return entry.expiresAt > Date.now() ? entry : null;
    },
    async getGeneration(): Promise<string> {
      return String(await client.eval(GENERATION, 1, controlKey, randomUUID()));
    },
    async setIfGeneration(key: string, entry: T, expected: string): Promise<boolean> {
      if (!expected) return false;
      return publish(key, entry, expected);
    },
    async set(key: string, entry: T): Promise<void> { await publish(key, entry, ""); },
    async deleteByPath(pathname: string): Promise<void> {
      const normalized = normalizePathname(pathname);
      if (!normalized) return;
      await client.eval(INVALIDATE, 2, controlKey, indexKey(normalized), randomUUID(), randomUUID());
    },
    async clear(): Promise<void> {
      // Logical O(1) clear: old payloads/indexes expire through their TTLs.
      // No scan/delete can race a fill admitted into the new epoch.
      await client.eval(CLEAR, 1, controlKey, randomUUID(), randomUUID());
    },
  };
}
function createAppCacheStore(client: RedisLikeClient, prefix: string): NeutronAppCacheStore {
  const store = createStore<NeutronAppResponseCacheEntry>(client, prefix, "app", extractAppPathFromKey);
  return { ...store, async get(key) {
    const entry = await store.get(key);
    if (!entry) return null;
    const stored: unknown = entry.body;
    return typeof stored === "string" ? { ...entry, body: new TextEncoder().encode(stored) } : entry;
  } };
}
function createLoaderCacheStore(client: RedisLikeClient, prefix: string): NeutronLoaderCacheStore {
  return createStore<NeutronLoaderDataCacheEntry>(client, prefix, "ldr", extractLoaderPathFromKey);
}

function extractAppPathFromKey(cacheKey: string): string {
  const fields = cacheKey.split("\n");
  if (fields.length >= 3) return fields[2];
  const separator = cacheKey.indexOf(":");
  if (separator === -1) {
    return "/";
  }

  const routePart = cacheKey.slice(separator + 1);
  const querySeparator = routePart.indexOf("?");
  if (querySeparator === -1) {
    return normalizePathname(routePart) ?? "/";
  }
  return normalizePathname(routePart.slice(0, querySeparator)) ?? "/";
}

function extractLoaderPathFromKey(cacheKey: string): string {
  const separator = cacheKey.indexOf("::");
  if (separator === -1) {
    return normalizePathname(cacheKey) ?? "/";
  }
  return normalizePathname(cacheKey.slice(0, separator)) ?? "/";
}

function normalizePathname(pathname: string): string | null {
  try {
    const decoded = decodeURIComponent(pathname || "/");
    if (!decoded.startsWith("/") || decoded.split("/").includes("..")) {
      return null;
    }
    if (decoded.length > 1 && decoded.endsWith("/")) {
      return decoded.slice(0, -1);
    }
    return decoded;
  } catch {
    return null;
  }
}

function ttlFromExpiresAt(expiresAt: number): number {
  const ttlMs = expiresAt - Date.now();
  if (ttlMs <= 0) {
    return 0;
  }
  return Math.max(1, Math.ceil(ttlMs / 1000));
}

type DynamicImporter = (specifier: string) => Promise<unknown>;

const dynamicImport = new Function(
  "specifier",
  "return import(specifier);"
) as DynamicImporter;

async function lazyImport<TModule>(
  specifier: string,
  installHint: string
): Promise<TModule> {
  try {
    return (await dynamicImport(specifier)) as TModule;
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    throw new Error(
      `Missing optional dependency "${specifier}". ${installHint}. Original error: ${reason}`
    );
  }
}
