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
  /**
   * Remaining TTL in seconds, -1 for a key with no expiry, -2 for a
   * missing key. Used to keep the pathname index alive at least as long as
   * its longest-lived member (audit neutron-14). Optional: without it the
   * index TTL is reset to the standard floor on every write.
   */
  ttl?(key: string): Promise<number>;
  /**
   * Atomically renames a key. Used to claim the pathname index during
   * invalidation so a concurrent writer cannot lose its index membership
   * (audit neutron-15). Optional: without it invalidation falls back to
   * the non-atomic SMEMBERS/DEL sequence.
   */
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
      await extendIndexTtl(client, indexKey, Math.max(ttlSec, 60));
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
      await extendIndexTtl(client, indexKey, Math.max(ttlSec, 60));
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

/**
 * Keeps the pathname index alive at least until the latest member expiry.
 *
 * EXPIRE always sets an absolute TTL, so a short-lived variant written
 * after a long-lived one used to shorten the shared index below the
 * long-lived entry's remaining life — after which deleteByPath could no
 * longer discover it (audit neutron-14). With a ttl() available the index
 * TTL is only ever extended, never shortened; -1 (no expiry) and -2
 * (missing) both compare below any positive desired value, so those states
 * also (re)arm expiry.
 */
async function extendIndexTtl(
  client: RedisLikeClient,
  indexKey: string,
  desiredSec: number
): Promise<void> {
  if (typeof client.ttl !== "function") {
    await client.expire(indexKey, desiredSec);
    return;
  }
  const currentSec = await client.ttl(indexKey);
  if (currentSec < desiredSec) {
    await client.expire(indexKey, desiredSec);
  }
}

let invalidationCounter = 0;

/**
 * Deletes every entry indexed under a pathname, then the index itself.
 *
 * When the client supports RENAME, the index is atomically claimed first:
 * writers that SADD after the rename repopulate a fresh index, so no live
 * entry can permanently lose its index membership while an invalidation is
 * in flight (audit neutron-15). Without RENAME the old SMEMBERS/DEL
 * sequence is used and a writer racing the final index deletion can be
 * orphaned until its own TTL.
 */
async function deleteIndexedPathKeys(
  client: RedisLikeClient,
  indexKey: string
): Promise<void> {
  if (typeof client.rename !== "function") {
    const members = await client.smembers(indexKey);
    if (members.length > 0) {
      await client.del(...members);
    }
    await client.del(indexKey);
    return;
  }

  const claimedKey = `${indexKey}:invalidated:${Date.now().toString(36)}:${invalidationCounter++}`;
  try {
    await client.rename(indexKey, claimedKey);
  } catch (error) {
    // RENAME fails with "no such key" when nothing is indexed under the
    // path — there is nothing to delete. Any other failure is real and
    // must surface.
    if (!String(error).includes("no such key")) {
      throw error;
    }
    return;
  }

  // Everything SADDed before the rename is in the claimed set; writers
  // after it are in the fresh index and stay invalidatable.
  const members = await client.smembers(claimedKey);
  if (members.length > 0) {
    await client.del(...members, claimedKey);
  } else {
    await client.del(claimedKey);
  }
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
