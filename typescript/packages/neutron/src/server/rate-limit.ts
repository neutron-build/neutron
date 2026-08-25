/**
 * Rate Limiting Middleware
 *
 * Provides configurable rate limiting to prevent abuse and DoS attacks.
 * Uses a sliding window algorithm with in-memory storage.
 */

import { createHash } from "node:crypto";
import type { MiddlewareFn } from "../core/types.js";

export interface RateLimitOptions {
  /**
   * Time window in milliseconds
   * @default 60000 (1 minute)
   */
  windowMs?: number;

  /**
   * Maximum number of requests per window
   * @default 100
   */
  maxRequests?: number;

  /**
   * Function to generate a unique key for each client.
   *
   * If omitted, the key is derived from a trusted client address (see
   * `trustProxy`). SECURITY: the default never reads `X-Forwarded-For` unless
   * `trustProxy` is enabled, because that header is attacker-controlled — using
   * it as the key lets a client mint a fresh bucket per request and bypass the
   * limit entirely.
   *
   * @example
   * ```ts
   * // Rate limit by authenticated user (most robust)
   * keyGenerator: (request) => request.context?.user?.id || "anonymous"
   * ```
   */
  keyGenerator?: (request: Request) => string;

  /**
   * Trust proxy-forwarded client-address headers (`X-Forwarded-For` /
   * `X-Real-IP`). Only enable this when the server sits behind a proxy you
   * control that overwrites these headers. Default: false.
   */
  trustProxy?: boolean;

  /**
   * Header carrying the forwarded client chain. Default: "x-forwarded-for".
   */
  forwardedHeader?: string;

  /**
   * Number of trusted proxies in front of this server. The client address is
   * read this many hops from the right of the forwarded chain. Default: 1.
   */
  trustedHops?: number;

  /**
   * Custom handler for rate limit exceeded
   * Defaults to returning 429 Too Many Requests
   */
  handler?: (request: Request) => Response | Promise<Response>;

  /**
   * Skip rate limiting for certain requests
   * @example
   * ```ts
   * skip: (request) => request.url.includes("/health")
   * ```
   */
  skip?: (request: Request) => boolean;
}

interface RateLimitRecord {
  count: number;
  resetAt: number;
}

/**
 * Creates rate limiting middleware
 *
 * @example
 * ```ts
 * import { rateLimitMiddleware } from "@neutron-build/core/server";
 *
 * // Global rate limit
 * export const middleware = [
 *   rateLimitMiddleware({
 *     windowMs: 60000, // 1 minute
 *     maxRequests: 100 // 100 requests per minute
 *   })
 * ];
 *
 * // Per-IP rate limit behind a trusted proxy
 * export const middleware = [
 *   rateLimitMiddleware({
 *     windowMs: 60000,
 *     maxRequests: 20,
 *     trustProxy: true // read client IP from X-Forwarded-For
 *   })
 * ];
 * ```
 */
let warnedGlobalKey = false;

/**
 * Derive a rate-limit key from a trustworthy client address.
 *
 * Order of preference:
 *   1. A client address the server adapter populated on the context
 *      (`context.clientAddress`) — the real socket peer, never spoofable.
 *   2. `X-Forwarded-For` / `X-Real-IP`, but only when `trustProxy` is enabled,
 *      reading `trustedHops` from the right of the chain.
 *
 * If none is available it falls back to a shared `"global"` bucket (so the
 * limiter still bounds total throughput) and warns once, because per-client
 * limiting is impossible without a trusted address.
 */
function resolveClientKey(
  request: Request,
  context: unknown,
  options: RateLimitOptions
): string {
  const ctxAddr = (context as { clientAddress?: unknown } | null | undefined)
    ?.clientAddress;
  if (typeof ctxAddr === "string" && ctxAddr.length > 0) {
    return ctxAddr;
  }

  if (options.trustProxy) {
    const header = (options.forwardedHeader || "x-forwarded-for").toLowerCase();
    const realIp = request.headers.get("x-real-ip");
    if (realIp) {
      return realIp.trim();
    }
    const forwarded = request.headers.get(header);
    if (forwarded) {
      const parts = forwarded
        .split(",")
        .map((value) => value.trim())
        .filter(Boolean);
      if (parts.length > 0) {
        const trustedHops = Math.max(1, options.trustedHops ?? 1);
        const index = parts.length - trustedHops;
        return parts[Math.max(0, index)]!;
      }
    }
  }

  if (!warnedGlobalKey && process.env.NODE_ENV !== "production") {
    warnedGlobalKey = true;
    console.warn(
      "[neutron] rateLimitMiddleware has no trusted client address; falling " +
        "back to a shared global limit. Set trustProxy (behind a trusted " +
        "proxy) or a keyGenerator for per-client limiting."
    );
  }
  return "global";
}

export function rateLimitMiddleware(
  options: RateLimitOptions = {}
): MiddlewareFn {
  const windowMs = options.windowMs ?? 60000; // 1 minute default
  const maxRequests = options.maxRequests ?? 100;
  const skip = options.skip || (() => false);
  const resolveKey = options.keyGenerator
    ? (request: Request, _context: unknown) => String(options.keyGenerator!(request))
    : (request: Request, context: unknown) => resolveClientKey(request, context, options);

  // Hard cap on distinct keys so an attacker who can still vary the key (e.g.
  // a misconfigured trusted proxy) cannot exhaust memory.
  const MAX_KEYS = 100_000;
  const requests = new Map<string, RateLimitRecord>();

  // Cleanup expired entries periodically to prevent memory leaks
  const cleanupInterval = setInterval(() => {
    const now = Date.now();
    for (const [key, record] of requests) {
      if (now >= record.resetAt) {
        requests.delete(key);
      }
    }
  }, windowMs);
  // The interval must not hold the event loop open for the process lifetime
  // of every middleware instance. Explicit teardown stays available via the
  // attached .cleanup().
  (cleanupInterval as unknown as { unref?: () => void }).unref?.();

  const middleware: MiddlewareFn = async (request, context, next) => {
    // Skip rate limiting if configured
    if (skip(request)) {
      return next();
    }

    // SECURITY: Validate and normalize keys to prevent memory exhaustion
    let key = resolveKey(request, context);
    const MAX_KEY_LENGTH = 256;
    if (key.length > MAX_KEY_LENGTH) {
      // Hash long keys to prevent memory issues
      key = createHash("sha256").update(key).digest("hex");
    }

    const now = Date.now();
    const record = requests.get(key);

    // No record or window expired - create new window
    if (!record || now >= record.resetAt) {
      if (!record && requests.size >= MAX_KEYS) {
        // At capacity: drop expired entries before admitting a new key.
        for (const [existingKey, existing] of requests) {
          if (now >= existing.resetAt) {
            requests.delete(existingKey);
          }
        }
      }
      requests.set(key, {
        count: 1,
        resetAt: now + windowMs,
      });
      return next();
    }

    // Rate limit exceeded
    if (record.count >= maxRequests) {
      if (options.handler) {
        return options.handler(request);
      }

      const retryAfter = Math.ceil((record.resetAt - now) / 1000);
      return new Response("Too Many Requests", {
        status: 429,
        headers: {
          "Content-Type": "text/plain",
          "Retry-After": String(retryAfter),
          "X-RateLimit-Limit": String(maxRequests),
          "X-RateLimit-Remaining": "0",
          "X-RateLimit-Reset": String(Math.ceil(record.resetAt / 1000)),
        },
      });
    }

    // Increment count and proceed
    record.count++;

    const response = await next();

    // Add rate limit headers to response
    const remaining = maxRequests - record.count;
    response.headers.set("X-RateLimit-Limit", String(maxRequests));
    response.headers.set("X-RateLimit-Remaining", String(Math.max(0, remaining)));
    response.headers.set(
      "X-RateLimit-Reset",
      String(Math.ceil(record.resetAt / 1000))
    );

    return response;
  };

  // SECURITY: Attach cleanup method to middleware for graceful shutdown
  // This allows the interval to be cleared when middleware is no longer needed
  (middleware as any).cleanup = () => clearInterval(cleanupInterval);

  return middleware;
}

/**
 * Create a rate limiter specifically for API endpoints
 *
 * @example
 * ```ts
 * // In your API route file
 * export const middleware = [
 *   apiRateLimit({ maxRequests: 10, windowMs: 60000 })
 * ];
 * ```
 */
export function apiRateLimit(
  options: Omit<RateLimitOptions, "keyGenerator"> & {
    keyGenerator?: RateLimitOptions["keyGenerator"];
  } = {}
): MiddlewareFn {
  return rateLimitMiddleware({
    windowMs: 60000, // 1 minute
    maxRequests: 30, // 30 requests per minute for APIs
    ...options,
  });
}

/**
 * Create a rate limiter for image optimization endpoint
 *
 * @example
 * ```ts
 * // Apply to image endpoint
 * export const middleware = [
 *   imageRateLimit({ maxRequests: 50 })
 * ];
 * ```
 */
export function imageRateLimit(
  options: Omit<RateLimitOptions, "keyGenerator"> & {
    keyGenerator?: RateLimitOptions["keyGenerator"];
  } = {}
): MiddlewareFn {
  return rateLimitMiddleware({
    windowMs: 60000, // 1 minute
    maxRequests: 50, // 50 image requests per minute
    ...options,
    skip:
      options.skip ||
      (() => {
        // Don't rate limit in development. Gating on a trusted env flag rather
        // than the request hostname, which an attacker can set via the Host
        // header to bypass the limit on a CPU-heavy endpoint.
        return process.env.NODE_ENV !== "production";
      }),
  });
}
