import {
  deserializeTransportData,
  serializeTransportData,
  type NeutronAppCacheStore,
  type NeutronAppResponseCacheEntry,
  type NeutronCacheStores,
  type NeutronLoaderCacheStore,
  type NeutronLoaderDataCacheEntry,
} from "neutron";

export interface RedisNeutronCacheOptions {
  url?: string;
  keyPrefix?: string;
  connectTimeoutMs?: number;
}

export interface RedisLikeClient {
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

function createAppCacheStore(
  client: RedisLikeClient,
  keyPrefix: string
): NeutronAppCacheStore {
  return {
    async get(key) {
      const raw = await client.get(appEntryKey(keyPrefix, key));
      if (!raw) {
        return null;
      }

      const entry = deserializeTransportData<NeutronAppResponseCacheEntry>(raw);
      if (entry.expiresAt <= Date.now()) {
        await client.del(appEntryKey(keyPrefix, key));
        return null;
      }
      return entry;
    },
    async set(key, entry) {
      const ttlSec = ttlFromExpiresAt(entry.expiresAt);
      if (ttlSec <= 0) {
        await client.del(appEntryKey(keyPrefix, key));
        return;
      }

      const entryKey = appEntryKey(keyPrefix, key);
      const pathname = extractAppPathFromKey(key);
      const indexKey = appPathIndexKey(keyPrefix, pathname);
      const payload = serializeTransportData(entry);
      await client.set(entryKey, payload, "EX", ttlSec);
      await client.sadd(indexKey, entryKey);
      await client.expire(indexKey, Math.max(ttlSec, 60));
    },
    async deleteByPath(pathname) {
      await deleteIndexedPathKeys(client, appPathIndexKey(keyPrefix, pathname));
    },
    async clear() {
      await clearByPatterns(client, [
        `${keyPrefix}app:*`,
        `${keyPrefix}idx:app:*`,
      ]);
    },
  };
}

function createLoaderCacheStore(
  client: RedisLikeClient,
  keyPrefix: string
): NeutronLoaderCacheStore {
  return {
    async get(key) {
      const raw = await client.get(loaderEntryKey(keyPrefix, key));
      if (!raw) {
        return null;
      }

      const entry = deserializeTransportData<NeutronLoaderDataCacheEntry>(raw);
      if (entry.expiresAt <= Date.now()) {
        await client.del(loaderEntryKey(keyPrefix, key));
        return null;
      }
      return entry;
    },
    async set(key, entry) {
      const ttlSec = ttlFromExpiresAt(entry.expiresAt);
      if (ttlSec <= 0) {
        await client.del(loaderEntryKey(keyPrefix, key));
        return;
      }

      const entryKey = loaderEntryKey(keyPrefix, key);
      const pathname = extractLoaderPathFromKey(key);
      const indexKey = loaderPathIndexKey(keyPrefix, pathname);
      const payload = serializeTransportData(entry);
      await client.set(entryKey, payload, "EX", ttlSec);
      await client.sadd(indexKey, entryKey);
      await client.expire(indexKey, Math.max(ttlSec, 60));
    },
    async deleteByPath(pathname) {
      await deleteIndexedPathKeys(client, loaderPathIndexKey(keyPrefix, pathname));
    },
    async clear() {
      await clearByPatterns(client, [
        `${keyPrefix}ldr:*`,
        `${keyPrefix}idx:ldr:*`,
      ]);
    },
  };
}

async function deleteIndexedPathKeys(
  client: RedisLikeClient,
  indexKey: string
): Promise<void> {
  const members = await client.smembers(indexKey);
  if (members.length > 0) {
    await client.del(...members);
  }
  await client.del(indexKey);
}

async function clearByPatterns(
  client: RedisLikeClient,
  patterns: string[]
): Promise<void> {
  for (const pattern of patterns) {
    if (typeof client.scan === "function") {
      await clearByScan(client, pattern);
      continue;
    }

    const keys = await client.keys(pattern);
    await deleteKeysInChunks(client, keys);
  }
}

async function clearByScan(
  client: RedisLikeClient,
  pattern: string
): Promise<void> {
  if (!client.scan) {
    return;
  }

  let cursor = "0";
  do {
    const [nextCursor, keys] = await client.scan(
      cursor,
      "MATCH",
      pattern,
      "COUNT",
      500
    );
    await deleteKeysInChunks(client, keys);
    cursor = nextCursor;
  } while (cursor !== "0");
}

async function deleteKeysInChunks(
  client: RedisLikeClient,
  keys: string[]
): Promise<void> {
  if (keys.length === 0) {
    return;
  }

  const chunkSize = 500;
  for (let index = 0; index < keys.length; index += chunkSize) {
    const chunk = keys.slice(index, index + chunkSize);
    if (chunk.length > 0) {
      await client.del(...chunk);
    }
  }
}

function appEntryKey(prefix: string, key: string): string {
  return `${prefix}app:${key}`;
}

function loaderEntryKey(prefix: string, key: string): string {
  return `${prefix}ldr:${key}`;
}

function appPathIndexKey(prefix: string, pathname: string): string {
  return `${prefix}idx:app:${normalizePathname(pathname)}`;
}

function loaderPathIndexKey(prefix: string, pathname: string): string {
  return `${prefix}idx:ldr:${normalizePathname(pathname)}`;
}

function extractAppPathFromKey(cacheKey: string): string {
  const separator = cacheKey.indexOf(":");
  if (separator === -1) {
    return "/";
  }

  const routePart = cacheKey.slice(separator + 1);
  const querySeparator = routePart.indexOf("?");
  if (querySeparator === -1) {
    return normalizePathname(routePart);
  }
  return normalizePathname(routePart.slice(0, querySeparator));
}

function extractLoaderPathFromKey(cacheKey: string): string {
  const separator = cacheKey.indexOf("::");
  if (separator === -1) {
    return normalizePathname(cacheKey);
  }
  return normalizePathname(cacheKey.slice(0, separator));
}

function normalizePathname(pathname: string): string {
  try {
    const decoded = decodeURIComponent(pathname || "/");
    if (!decoded.startsWith("/") || decoded.includes("..")) {
      return "/";
    }
    if (decoded.length > 1 && decoded.endsWith("/")) {
      return decoded.slice(0, -1);
    }
    return decoded;
  } catch {
    return "/";
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
