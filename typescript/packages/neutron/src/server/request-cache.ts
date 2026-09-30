import { AsyncLocalStorage } from 'node:async_hooks';
import { CacheScope, installRequestCacheScope } from '../core/cache.js';

const storageKey = Symbol.for('@neutron-build/core/request-cache/storage');
const realm = globalThis as unknown as Record<symbol, AsyncLocalStorage<CacheScope> | undefined>;
const scopes = realm[storageKey] ??= new AsyncLocalStorage<CacheScope>();
installRequestCacheScope(() => scopes.getStore());

/** Every HTTP request receives its own cache, including concurrent requests. */
export function runWithRequestCache<T>(callback: () => T): T {
  return scopes.run(new CacheScope(true), callback);
}
