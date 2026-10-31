import type { AtomicCachePublication } from './cache-store.js';

export const CACHE_OPERATION_TIMEOUT_MS = 2000;
export const CACHE_MAX_OPERATIONS = 8;
let outstanding = 0;
const unavailable = new WeakSet<object>();

/** A finite wait is not cancellation. Keep the aggregate slot until the
 * underlying operation settles; quarantine a timed-out store from new work.
 * Even a provider that ignores abort cannot create unbounded detached calls. */
export async function cacheOperation<T>(
  operation: (signal: AbortSignal, deadline: number) => Promise<T>,
  fallback: T,
  owner?: object,
  timeoutMs = CACHE_OPERATION_TIMEOUT_MS
): Promise<T> {
  if ((owner && unavailable.has(owner)) || outstanding >= CACHE_MAX_OPERATIONS) return fallback;
  outstanding++;
  const controller = new AbortController();
  const deadline = Date.now() + timeoutMs;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const pending = Promise.resolve().then(() => operation(controller.signal, deadline));
  // Attach both handlers, including after timeout: no unhandled rejection and
  // no early release of an outstanding backend operation's aggregate slot.
  const settled = pending.then(value => { outstanding--; return value; }, () => { outstanding--; return fallback; });
  try {
    return await Promise.race([settled, new Promise<T>(resolve => {
      timer = setTimeout(() => {
        if (owner) unavailable.add(owner);
        controller.abort();
        resolve(fallback);
      }, timeoutMs);
    })]);
  } finally { if (timer !== undefined) clearTimeout(timer); }
}

export function supportsBoundedPublication(store: Partial<AtomicCachePublication<unknown>>): boolean {
  return store.publicationDeadline === true && typeof store.setIfGeneration === 'function' && !unavailable.has(store);
}

/** Only providers promising an ATOMIC deadline+generation check may publish.
 * Abort is advisory; correctness depends on the backing transaction checking
 * the absolute deadline at the write, not a client Promise.race or precheck. */
export async function publishCacheEntry<T>(store: Partial<AtomicCachePublication<T>>, key: string, entry: T, generation: string): Promise<boolean> {
  if (!supportsBoundedPublication(store)) return false;
  return cacheOperation((signal, deadline) => store.setIfGeneration!(key, entry, generation, { signal, deadline }), false, store);
}
