import type { CacheClient } from "../cache/index.js";

export interface SlidingWindowOptions {
  limit: number;
  windowSec: number;
}

export interface SlidingWindowResult {
  allowed: boolean;
  remaining: number;
  retryAfterSec: number;
}

export async function enforceSlidingWindow(
  cache: CacheClient,
  key: string,
  options: SlidingWindowOptions
): Promise<SlidingWindowResult> {
  const limit = Math.max(1, Math.floor(options.limit));
  const windowSec = Math.max(1, Math.floor(options.windowSec));
  const windowMs = windowSec * 1000;
  const now = Date.now();
  const currentWindow = Math.floor(now / windowMs);
  const previousWindow = currentWindow - 1;
  const currentKey = `rl:${key}:${currentWindow}`;
  const previousKey = `rl:${key}:${previousWindow}`;

  // Use atomic increments where available (Redis-backed clients), then compute
  // weighted usage using current and previous windows.
  const currentCount = await cache.incr(currentKey, windowSec * 2);
  const previousRaw = await cache.get(previousKey);
  const previousParsed = previousRaw ? Number.parseInt(previousRaw, 10) : 0;
  const previousCount = Number.isFinite(previousParsed) ? previousParsed : 0;

  const currentWindowStartedAt = currentWindow * windowMs;
  const elapsedInWindow = now - currentWindowStartedAt;
  const previousWindowWeight = Math.max(0, (windowMs - elapsedInWindow) / windowMs);
  const used = currentCount + previousCount * previousWindowWeight;
  const remaining = Math.max(0, Math.floor(limit - used));
  const retryAfterSec = Math.max(1, Math.ceil((windowMs - elapsedInWindow) / 1000));

  return {
    allowed: used <= limit,
    remaining,
    retryAfterSec,
  };
}
