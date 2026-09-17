export interface NeutronAppResponseCacheEntry {
  status: number;
  statusText: string;
  headers: [string, string][];
  /**
   * Response body as raw bytes. The store is byte-exact by contract (TS-06):
   * the previous `string` body round-tripped through UTF-8 text and silently
   * transformed invalid-UTF-8/binary bytes while the copied headers still
   * described the original octets.
   */
  body: Uint8Array;
  expiresAt: number;
}

export interface NeutronLoaderDataCacheEntry {
  data: unknown;
  expiresAt: number;
}

export interface NeutronAppCacheStore {
  get(key: string): Promise<NeutronAppResponseCacheEntry | null>;
  set(key: string, entry: NeutronAppResponseCacheEntry): Promise<void>;
  deleteByPath(pathname: string): Promise<void>;
  clear(): Promise<void>;
}

export interface NeutronLoaderCacheStore {
  get(key: string): Promise<NeutronLoaderDataCacheEntry | null>;
  set(key: string, entry: NeutronLoaderDataCacheEntry): Promise<void>;
  deleteByPath(pathname: string): Promise<void>;
  clear(): Promise<void>;
}

export interface NeutronCacheStores {
  app?: NeutronAppCacheStore;
  loader?: NeutronLoaderCacheStore;
}

export interface MemoryAppCacheStoreOptions {
  maxEntries?: number;
}

export interface MemoryLoaderCacheStoreOptions {
  maxEntries?: number;
}

const DEFAULT_MEMORY_APP_CACHE_ENTRIES = 500;
const DEFAULT_MEMORY_LOADER_CACHE_ENTRIES = 4000;

export function createMemoryAppCacheStore(
  options: MemoryAppCacheStoreOptions = {}
): NeutronAppCacheStore {
  const cache = new Map<string, NeutronAppResponseCacheEntry>();
  const maxEntries = resolveMaxEntries(
    options.maxEntries,
    DEFAULT_MEMORY_APP_CACHE_ENTRIES
  );

  return {
    async get(key) {
      const entry = cache.get(key);
      if (!entry) {
        return null;
      }
      if (entry.expiresAt <= Date.now()) {
        cache.delete(key);
        return null;
      }
      // LRU: a read refreshes recency so eviction below removes the
      // least-recently-used entry, not the first-inserted one.
      cache.delete(key);
      cache.set(key, entry);
      return entry;
    },
    async set(key, entry) {
      if (!cache.has(key) && cache.size >= maxEntries) {
        const oldest = cache.keys().next().value;
        if (typeof oldest === "string") {
          cache.delete(oldest);
        }
      }
      cache.set(key, entry);
    },
    async deleteByPath(pathname) {
      const normalized = normalizeCachePathname(pathname);
      if (!normalized) {
        return;
      }

      // App-cache keys are `variant\norigin\npath\nsearch\n...` (see
      // buildAppCacheKey). Match the path field EXACTLY: the previous
      // `startsWith("html:/user")` prefix test also invalidated `/users` and
      // every other path sharing the prefix (TS-08).
      for (const key of cache.keys()) {
        const parts = key.split("\n");
        if (parts.length >= 3 && parts[2] === normalized) {
          cache.delete(key);
        }
      }
    },
    async clear() {
      cache.clear();
    },
  };
}

export function createMemoryLoaderCacheStore(
  options: MemoryLoaderCacheStoreOptions = {}
): NeutronLoaderCacheStore {
  const cache = new Map<string, NeutronLoaderDataCacheEntry>();
  const maxEntries = resolveMaxEntries(
    options.maxEntries,
    DEFAULT_MEMORY_LOADER_CACHE_ENTRIES
  );

  return {
    async get(key) {
      const entry = cache.get(key);
      if (!entry) {
        return null;
      }
      if (entry.expiresAt <= Date.now()) {
        cache.delete(key);
        return null;
      }
      // LRU: a read refreshes recency (see the app cache store above).
      cache.delete(key);
      cache.set(key, entry);
      // Ownership boundary (TS-20): hand the caller a clone so mutating a
      // nested value in the returned object cannot alter what another
      // request will read from the cache.
      return cloneLoaderEntry(entry);
    },
    async set(key, entry) {
      if (!cache.has(key) && cache.size >= maxEntries) {
        const oldest = cache.keys().next().value;
        if (typeof oldest === "string") {
          cache.delete(oldest);
        }
      }
      // Clone at ingress too: the caller keeps its reference after storing.
      cache.set(key, cloneLoaderEntry(entry));
    },
    async deleteByPath(pathname) {
      const normalized = normalizeCachePathname(pathname);
      if (!normalized) {
        return;
      }

      const prefix = `${normalized}::`;
      for (const key of cache.keys()) {
        if (key.startsWith(prefix)) {
          cache.delete(key);
        }
      }
    },
    async clear() {
      cache.clear();
    },
  };
}

/**
 * Clone stored loader data. Loader data must already be structured-clone
 * compatible — it is JSON-serialized into the document — so a failed clone
 * surfaces as a thrown error rather than silent cross-request aliasing.
 */
function cloneLoaderEntry(entry: NeutronLoaderDataCacheEntry): NeutronLoaderDataCacheEntry {
  return { expiresAt: entry.expiresAt, data: structuredClone(entry.data) };
}

function resolveMaxEntries(value: number | undefined, fallback: number): number {
  if (!Number.isFinite(value) || (value || 0) <= 0) {
    return fallback;
  }
  return Math.floor(value!);
}

/**
 * Canonical cache-path form: decoded, no trailing slash, `/`-rooted, `..`-free.
 * Both `deleteByPath` invalidation and loader-cache key construction must use
 * this — keys written from the raw (percent-encoded, trailing-slash) request
 * path are invisible to invalidation and survive until their TTL.
 *
 * Traversal is a whole SEGMENT equal to `..`, not a substring: `/a..b` and
 * `/v1.2..3` are legal paths and must stay invalidatable (TS-08). This now
 * matches the serving path's `normalizePathname` rule exactly.
 *
 * Returns null when the path cannot be safely normalized (undecodable, not
 * rooted, contains a `..` segment).
 */
export function normalizeCachePathname(pathname: string): string | null {
  let decoded: string;
  try {
    decoded = decodeURIComponent(pathname || "/");
  } catch {
    return null;
  }

  if (!decoded.startsWith("/") || decoded.split("/").includes("..")) {
    return null;
  }

  if (decoded.length > 1 && decoded.endsWith("/")) {
    return decoded.slice(0, -1);
  }

  return decoded;
}
