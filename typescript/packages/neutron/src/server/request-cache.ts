import { AsyncLocalStorage } from 'node:async_hooks';
import { CacheScope, installRequestCacheScope } from '../core/cache.js';

const scopes = new AsyncLocalStorage<CacheScope>();
installRequestCacheScope(() => scopes.getStore());

/** Every HTTP request receives its own cache, including concurrent requests. */
export function runWithRequestCache<T>(callback: () => T): T {
  return scopes.run(new CacheScope(true), callback);
}
