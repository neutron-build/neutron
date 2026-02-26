export interface NeutronAppResponseCacheEntry {
  status: number;
  statusText: string;
  headers: [string, string][];
  body: string;
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

      const htmlPrefix = `html:${normalized}`;
      const jsonPrefix = `json:${normalized}`;
      for (const key of cache.keys()) {
        if (key.startsWith(htmlPrefix) || key.startsWith(jsonPrefix)) {
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

function resolveMaxEntries(value: number | undefined, fallback: number): number {
  if (!Number.isFinite(value) || (value || 0) <= 0) {
    return fallback;
  }
  return Math.floor(value!);
}

function normalizeCachePathname(pathname: string): string | null {
  let decoded: string;
  try {
    decoded = decodeURIComponent(pathname || "/");
  } catch {
    return null;
  }

  if (!decoded.startsWith("/") || decoded.includes("..")) {
    return null;
  }

  if (decoded.length > 1 && decoded.endsWith("/")) {
    return decoded.slice(0, -1);
  }

  return decoded;
}
