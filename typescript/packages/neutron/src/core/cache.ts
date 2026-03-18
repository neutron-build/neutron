// Request-level deduplication cache
// Inspired by SolidStart's cache() API + Next.js 16 cache tags

type CacheEntry<T> = {
  promise: Promise<T>;
  expiresAt: number;
  tags?: string[]; // NEW: Associated tags
};

const globalCache = new Map<string, CacheEntry<any>>();
const tagCache = new Map<string, Set<string>>(); // tag -> Set of cache keys

export interface CacheOptions {
  /**
   * Time-to-live in milliseconds (default: 5000ms on client, request lifetime on server)
   */
  ttl?: number;

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
  const { ttl = 5000, keyPrefix = 'cache', tags: tagsFn } = options;

  return (...args: TArgs): Promise<TReturn> => {
    // Generate cache key from function args
    const key = `${keyPrefix}:${JSON.stringify(args)}`;

    // Check if we have a valid cached entry
    const cached = globalCache.get(key);
    if (cached && cached.expiresAt > Date.now()) {
      return cached.promise;
    }

    // Create new promise and cache it
    const promise = fn(...args);

    // Generate tags for this cache entry
    const tags = tagsFn ? tagsFn(...args) : undefined;

    const expiresAt = Date.now() + ttl;
    globalCache.set(key, { promise, expiresAt, tags });

    // Register cache key with tags
    if (tags) {
      for (const tag of tags) {
        if (!tagCache.has(tag)) {
          tagCache.set(tag, new Set());
        }
        tagCache.get(tag)!.add(key);
      }
    }

    // Clean up expired entry after TTL
    setTimeout(() => {
      const entry = globalCache.get(key);
      if (entry && entry.expiresAt <= Date.now()) {
        globalCache.delete(key);
        // Clean up tag references
        if (entry.tags) {
          for (const tag of entry.tags) {
            tagCache.get(tag)?.delete(key);
            if (tagCache.get(tag)?.size === 0) {
              tagCache.delete(tag);
            }
          }
        }
      }
    }, ttl);

    return promise;
  };
}

/**
 * Clears all cached entries
 */
export function clearCache(): void {
  globalCache.clear();
}

/**
 * Clears cached entries matching a key prefix
 */
export function clearCacheByPrefix(prefix: string): void {
  for (const key of globalCache.keys()) {
    if (key.startsWith(prefix)) {
      globalCache.delete(key);
    }
  }
}

/**
 * Clears the cache after each server request (for SSR)
 * Call this in your server request handler
 */
export function resetRequestCache(): void {
  globalCache.clear();
  tagCache.clear();
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
export function revalidateTag(tag: string): void {
  const keys = tagCache.get(tag);
  if (!keys) return;

  // Delete all cache entries with this tag
  for (const key of keys) {
    const entry = globalCache.get(key);
    globalCache.delete(key);

    // Clean up other tag references for this key
    if (entry?.tags) {
      for (const t of entry.tags) {
        if (t !== tag) {
          tagCache.get(t)?.delete(key);
          if (tagCache.get(t)?.size === 0) {
            tagCache.delete(t);
          }
        }
      }
    }
  }

  // Delete tag mapping
  tagCache.delete(tag);
}

/**
 * Invalidates all cache entries associated with multiple tags
 *
 * @example
 * ```typescript
 * revalidateTags(['user:123', 'posts:user:123']);
 * ```
 */
export function revalidateTags(tags: string[]): void {
  for (const tag of tags) {
    revalidateTag(tag);
  }
}

/**
 * Gets all tags currently registered in the cache
 * Useful for debugging
 */
export function getCacheTags(): string[] {
  return Array.from(tagCache.keys());
}

/**
 * Gets all cache keys associated with a tag
 * Useful for debugging
 */
export function getCacheKeysByTag(tag: string): string[] {
  const keys = tagCache.get(tag);
  return keys ? Array.from(keys) : [];
}
