/**
 * The one cache the navigation path reads.
 *
 * There used to be two. `incremental-prefetch.ts` kept its own module-local
 * Maps while `handleNavigation` read `window.__NEUTRON_PREFETCH_CACHE__`, so
 * every byte the prefetcher fetched was discarded and every navigation paid a
 * full round-trip anyway. Anything that wants to warm a navigation goes through
 * this module, so the two cannot drift apart again.
 *
 * Entries expire and are consumed on read. Both matter:
 *
 * - **Expiry**, because the only writer used to be the form-submit path in
 *   `hooks.ts`, which stored the post-mutation payload under the current URL
 *   with no TTL and no invalidation. Navigating away and back then served that
 *   frozen snapshot forever, with no network — a mutation on one tab was
 *   invisible until a hard reload.
 * - **Consume on read**, because a warmed entry answers exactly one
 *   navigation. Keeping it would make the second visit to a URL serve the
 *   first visit's data.
 */

// Type-only, so this does not create a runtime cycle with hooks.ts (which
// imports storePrefetch from here).
import type { LoaderData } from "./hooks.js";

interface PrefetchEntry {
  data: LoaderData;
  expiresAt: number;
}

/**
 * How long a warmed payload stays usable. Long enough to cover hover-then-click
 * and a viewport prefetch the user acts on shortly after, short enough that a
 * stale render is not a plausible outcome.
 */
export const PREFETCH_TTL_MS = 30_000;

/** Bounds memory on link-dense pages, where a viewport pass can warm dozens. */
const MAX_ENTRIES = 32;

const entries = new Map<string, PrefetchEntry>();

// The `Window.__NEUTRON_PREFETCH_CACHE__` global is declared once, in hooks.ts.

/** Mirror to the legacy global so external reads and devtools still work. */
function mirror(url: string, data: LoaderData | null): void {
  if (typeof window === "undefined") return;
  if (data === null) {
    if (window.__NEUTRON_PREFETCH_CACHE__) {
      delete window.__NEUTRON_PREFETCH_CACHE__[url];
    }
    return;
  }
  window.__NEUTRON_PREFETCH_CACHE__ = window.__NEUTRON_PREFETCH_CACHE__ || {};
  window.__NEUTRON_PREFETCH_CACHE__[url] = data;
}

/** Store a payload for `url`, replacing any existing entry. */
export function storePrefetch(
  url: string,
  data: LoaderData,
  ttlMs: number = PREFETCH_TTL_MS
): void {
  // Oldest-first eviction: insertion order is Map iteration order, and an
  // entry is only refreshed by delete-then-set below.
  entries.delete(url);
  while (entries.size >= MAX_ENTRIES) {
    const oldest = entries.keys().next();
    if (oldest.done) break;
    entries.delete(oldest.value);
    mirror(oldest.value, null);
  }
  entries.set(url, { data, expiresAt: Date.now() + ttlMs });
  mirror(url, data);
}

/**
 * Take the payload warmed for `url`, if it is still fresh.
 *
 * Returns null and drops the entry when it has expired, so an expired entry
 * costs one map lookup rather than a wrong render.
 */
export function takePrefetch(url: string): LoaderData | null {
  const entry = entries.get(url);
  if (!entry) return null;
  entries.delete(url);
  mirror(url, null);
  if (entry.expiresAt <= Date.now()) return null;
  return entry.data;
}

/** Whether a fresh entry exists, without consuming it. */
export function hasFreshPrefetch(url: string): boolean {
  const entry = entries.get(url);
  if (!entry) return false;
  if (entry.expiresAt <= Date.now()) {
    entries.delete(url);
    mirror(url, null);
    return false;
  }
  return true;
}

export function clearPrefetchCacheForUrl(url: string): void {
  entries.delete(url);
  mirror(url, null);
}

export function clearPrefetchCache(): void {
  for (const url of entries.keys()) mirror(url, null);
  entries.clear();
}
