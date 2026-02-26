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
   * Function to generate a unique key for each client
   * Defaults to using X-Forwarded-For or a global key
   *
   * @example
   * ```ts
   * // Rate limit by IP address
   * keyGenerator: (request) => request.headers.get("x-forwarded-for") || "unknown"
   *
   * // Rate limit by user ID (requires auth)
   * keyGenerator: (request) => request.context?.user?.id || "anonymous"
   * ```
   */
  keyGenerator?: (request: Request) => string;

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
 * import { rateLimitMiddleware } from "neutron/server";
 *
 * // Global rate limit
 * export const middleware = [
 *   rateLimitMiddleware({
 *     windowMs: 60000, // 1 minute
 *     maxRequests: 100 // 100 requests per minute
 *   })
 * ];
 *
 * // Per-IP rate limit
 * export const middleware = [
 *   rateLimitMiddleware({
 *     windowMs: 60000,
 *     maxRequests: 20,
 *     keyGenerator: (req) => req.headers.get("x-forwarded-for") || "unknown"
 *   })
 * ];
 * ```
 */
export function rateLimitMiddleware(
  options: RateLimitOptions = {}
): MiddlewareFn {
  const windowMs = options.windowMs ?? 60000; // 1 minute default
  const maxRequests = options.maxRequests ?? 100;
  const keyGenerator =
    options.keyGenerator ||
    ((request: Request) =>
      request.headers.get("x-forwarded-for") || "global");
  const skip = options.skip || (() => false);

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

  // Allow cleanup to be stopped (for testing or graceful shutdown)
  if (typeof process !== "undefined") {
    process.on("SIGTERM", () => clearInterval(cleanupInterval));
  }

  const middleware: MiddlewareFn = async (request, context, next) => {
    // Skip rate limiting if configured
    if (skip(request)) {
      return next();
    }

    // SECURITY: Validate and normalize keys to prevent memory exhaustion
    let key = String(keyGenerator(request));
    const MAX_KEY_LENGTH = 256;
    if (key.length > MAX_KEY_LENGTH) {
      // Hash long keys to prevent memory issues
      key = createHash("sha256").update(key).digest("hex");
    }

    const now = Date.now();
    const record = requests.get(key);

    // No record or window expired - create new window
    if (!record || now >= record.resetAt) {
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
      ((request) => {
        // Don't rate limit local/dev requests
        const url = new URL(request.url);
        return url.hostname === "localhost" || url.hostname === "127.0.0.1";
      }),
  });
}
