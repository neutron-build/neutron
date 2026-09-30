// Request-level deduplication cache
// Inspired by SolidStart's cache() API + Next.js 16 cache tags

type CacheEntry = {
  promise: Promise<unknown>;
  expiresAt: number;
  tags?: string[];
  timer?: ReturnType<typeof setTimeout>;
};

/** Internal state for one request or the bounded browser cache. */
export class CacheScope {
  readonly entries = new Map<string, CacheEntry>();
  readonly tags = new Map<string, Set<string>>();
  constructor(readonly request = false) {}
  remove(key: string): void {
    const entry = this.entries.get(key);
    if (!entry) return;
    this.entries.delete(key);
    if (entry.timer) clearTimeout(entry.timer);
    for (const tag of entry.tags ?? []) {
      const keys = this.tags.get(tag);
      keys?.delete(key);
      if (!keys?.size) this.tags.delete(tag);
    }
  }
  clear(): void {
    for (const key of this.entries.keys()) this.remove(key);
  }
}

type CacheRuntime = {
  requestScope: () => CacheScope | undefined;
  functionId: number;
  sharedScope?: CacheScope;
};
// SSR module loaders can evaluate this module separately from the HTTP adapter.
// One realm-wide provider and identity counter connects those module graphs.
const runtimeKey = Symbol.for('@neutron-build/core/request-cache/runtime');
const realm = globalThis as unknown as Record<symbol, CacheRuntime | undefined>;
const runtime = realm[runtimeKey] ??= { requestScope: () => undefined, functionId: 0 };
// Explicit shared caches and their invalidators must also cross SSR/adapter
// module graphs. Initialize lazily for an already-installed runtime provider.
const sharedCache = runtime.sharedScope ??= new CacheScope();

/** Installed by the Node server adapter; keeps Node imports out of client bundles. */
export function installRequestCacheScope(provider: () => CacheScope | undefined): void {
  runtime.requestScope = provider;
}

function activeScope(shared = false): CacheScope | undefined {
  if (shared) return sharedCache;
  const request = runtime.requestScope();
  if (request) return request;
  return typeof window !== 'undefined' ? sharedCache : undefined;
}

export interface CacheOptions {
  /**
   * Time-to-live in milliseconds (default: 5000ms on client, request lifetime on server)
   */
  ttl?: number;

  /** Explicit process-wide caching for public data. Default server caching is request-local. */
  scope?: 'request' | 'shared';

  /** Entry limit for the current scope (default 1024, maximum 4096). */
  maxEntries?: number;

  /**
   * Cache key prefix for namespacing
   */
  keyPrefix?: string;

  /**
   * Tags for cache invalidation (Next.js 16 style)
   * Function receives same args as cached function and returns array of tags
   *
   * @example
   * ```typescript
   * const getUser = cache(async (id: string) => {
   *   return db.users.findById(id);
   * }, {
   *   tags: (id) => [`user:${id}`, 'users']
   * });
   *
   * // Later: invalidate specific user
   * revalidateTag(`user:${id}`);
   * ```
   */
  tags?: (...args: any[]) => string[];
}

/**
 * Creates a cached version of an async function that deduplicates identical calls.
 *
 * On the server, deduplicates for the entire request lifetime.
 * On the client, caches for 5 seconds (configurable).
 *
 * @example
 * ```typescript
 * const getUser = cache(async (id: string) => {
 *   return db.users.findById(id);
 * }, { keyPrefix: 'user' });
 *
 * // In loader - both calls return same promise
 * const user1 = await getUser('123');
 * const user2 = await getUser('123'); // Uses cached promise
 * ```
 */
export function cache<TArgs extends any[], TReturn>(
  fn: (...args: TArgs) => Promise<TReturn>,
  options: CacheOptions = {}
): (...args: TArgs) => Promise<TReturn> {
  const { ttl, keyPrefix = 'cache', tags: tagsFn, maxEntries = 1024 } = options;
  if (ttl !== undefined && (!Number.isFinite(ttl) || ttl < 0 || ttl > 2147483647)) {
    throw new RangeError('Cache ttl must be between 0 and 2147483647 milliseconds');
  }
  if (!Number.isInteger(maxEntries) || maxEntries < 1 || maxEntries > 4096) {
    throw new RangeError('Cache maxEntries must be an integer between 1 and 4096');
  }
  const identity = ++runtime.functionId;
  return (...args: TArgs): Promise<TReturn> => {
    const state = activeScope(options.scope === 'shared');
    // Outside a server request, no implicit process cache may retain private data.
    if (!state) return fn(...args);
    const key = `${keyPrefix}:${identity}:${JSON.stringify(args)}`;
    const now = Date.now();
    const existing = state.entries.get(key);
    if (existing && existing.expiresAt > now) return existing.promise as Promise<TReturn>;
    state.remove(key);
    const tags = tagsFn ? [...new Set(tagsFn(...args))] : undefined;
    const promise = fn(...args);
    const lifetime = ttl ?? (state.request ? Infinity : 5000);
    const entry: CacheEntry = { promise, expiresAt: now + lifetime, tags };
    while (state.entries.size >= maxEntries) state.remove(state.entries.keys().next().value!);
    state.entries.set(key, entry);
    for (const tag of tags ?? []) {
      if (!state.tags.has(tag)) state.tags.set(tag, new Set());
      state.tags.get(tag)!.add(key);
    }
    promise.catch(() => {
      if (state.entries.get(key) === entry) state.remove(key);
    });
    if (Number.isFinite(lifetime)) {
      entry.timer = setTimeout(() => {
        if (state.entries.get(key) === entry) state.remove(key);
      }, lifetime);
      // Explicit shared caches must not keep a Node process alive.
      (entry.timer as unknown as { unref?: () => void }).unref?.();
    }
    return promise;
  };
}

/**
 * Clears all cached entries
 */
export function clearCache(scope?: 'request' | 'shared'): void {
  (activeScope(scope === 'shared') ?? sharedCache).clear();
}

/**
 * Clears cached entries matching a key prefix
 */
export function clearCacheByPrefix(prefix: string, scope?: 'request' | 'shared'): void {
  const state = activeScope(scope === 'shared') ?? sharedCache;
  if (!state) return;
  for (const key of state.entries.keys()) {
    if (key.startsWith(prefix)) state.remove(key);
  }
}

/**
 * Clears the cache after each server request (for SSR)
 * The server adapter creates isolated request scopes automatically.
 */
export function resetRequestCache(): void {
  activeScope()?.clear();
}

/**
 * Invalidates all cache entries associated with a tag
 * Inspired by Next.js 16 revalidateTag
 *
 * @example
 * ```typescript
 * const getUser = cache(async (id: string) => {
 *   return db.users.findById(id);
 * }, {
 *   tags: (id) => [`user:${id}`, 'users']
 * });
 *
 * // After updating user
 * await db.users.update('123', newData);
 * revalidateTag('user:123'); // Invalidates only user 123's cache
 * ```
 */
export function revalidateTag(tag: string, scope?: 'request' | 'shared'): void {
  const state = activeScope(scope === 'shared') ?? sharedCache;
  if (!state) return;
  for (const key of [...(state.tags.get(tag) ?? [])]) state.remove(key);
}

/**
 * Invalidates all cache entries associated with multiple tags
 *
 * @example
 * ```typescript
 * revalidateTags(['user:123', 'posts:user:123']);
 * ```
 */
export function revalidateTags(tags: string[], scope?: 'request' | 'shared'): void {
  for (const tag of tags) {
    revalidateTag(tag, scope);
  }
}

/**
 * Gets all tags currently registered in the cache
 * Useful for debugging
 */
export function getCacheTags(scope?: 'request' | 'shared'): string[] {
  return Array.from((activeScope(scope === 'shared') ?? sharedCache).tags.keys());
}

/**
 * Gets all cache keys associated with a tag
 * Useful for debugging
 */
export function getCacheKeysByTag(tag: string, scope?: 'request' | 'shared'): string[] {
  const keys = (activeScope(scope === 'shared') ?? sharedCache).tags.get(tag);
  return keys ? Array.from(keys) : [];
}
