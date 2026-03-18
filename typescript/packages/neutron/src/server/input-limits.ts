/**
 * Input Limits Middleware
 *
 * SECURITY: Protects against DoS attacks by enforcing limits on:
 * - Request body size
 * - Header sizes and counts
 * - URL length
 *
 * These limits prevent memory exhaustion and processing delays from
 * maliciously crafted requests.
 */

import type { MiddlewareFn } from "../core/types.js";

export interface InputLimitsOptions {
  /**
   * Maximum request body size in bytes
   * @default 10485760 (10MB)
   */
  maxRequestBodySize?: number;

  /**
   * Maximum size of individual header values in bytes
   * @default 16384 (16KB)
   */
  maxHeaderSize?: number;

  /**
   * Maximum number of headers allowed in a request
   * @default 100
   */
  maxHeaderCount?: number;

  /**
   * Maximum URL length in characters
   * @default 2048
   */
  maxUrlLength?: number;
}

const DEFAULT_LIMITS: Required<InputLimitsOptions> = {
  maxRequestBodySize: 10 * 1024 * 1024, // 10MB
  maxHeaderSize: 16 * 1024, // 16KB
  maxHeaderCount: 100,
  maxUrlLength: 2048,
};

/**
 * Creates middleware that enforces input validation limits
 *
 * @example
 * ```ts
 * import { inputLimitsMiddleware } from "neutron/server";
 *
 * export const middleware = inputLimitsMiddleware({
 *   maxRequestBodySize: 5 * 1024 * 1024, // 5MB
 *   maxUrlLength: 1024
 * });
 * ```
 */
export function inputLimitsMiddleware(options: InputLimitsOptions = {}): MiddlewareFn {
  const limits: Required<InputLimitsOptions> = {
    maxRequestBodySize: options.maxRequestBodySize ?? DEFAULT_LIMITS.maxRequestBodySize,
    maxHeaderSize: options.maxHeaderSize ?? DEFAULT_LIMITS.maxHeaderSize,
    maxHeaderCount: options.maxHeaderCount ?? DEFAULT_LIMITS.maxHeaderCount,
    maxUrlLength: options.maxUrlLength ?? DEFAULT_LIMITS.maxUrlLength,
  };

  return async (request, context, next) => {
    // Validate URL length
    if (request.url.length > limits.maxUrlLength) {
      return new Response("Request URL too long", {
        status: 414, // URI Too Long
        headers: { "Content-Type": "text/plain" },
      });
    }

    // Validate header count and sizes
    const headerValidation = validateHeaders(request.headers, limits);
    if (headerValidation.error) {
      return new Response(headerValidation.error, {
        status: 431, // Request Header Fields Too Large
        headers: { "Content-Type": "text/plain" },
      });
    }

    // Validate request body size for methods that may have a body
    const method = request.method.toUpperCase();
    if (method === "POST" || method === "PUT" || method === "PATCH") {
      const contentLength = request.headers.get("content-length");
      if (contentLength) {
        const bodySize = parseInt(contentLength, 10);
        if (!isNaN(bodySize) && bodySize > limits.maxRequestBodySize) {
          return new Response("Request body too large", {
            status: 413, // Payload Too Large
            headers: { "Content-Type": "text/plain" },
          });
        }
      }

      // SECURITY: For requests without Content-Length, we'll rely on the
      // underlying server (Node.js/Hono) to enforce limits. We can't easily
      // validate streaming bodies without consuming them, which would break
      // the request for downstream handlers.
      //
      // Most production deployments should configure body size limits at the
      // reverse proxy level (nginx, cloudflare, etc.) as a defense-in-depth measure.
    }

    return next();
  };
}

/**
 * Validates headers against size and count limits
 */
function validateHeaders(
  headers: Headers,
  limits: Required<InputLimitsOptions>
): { error?: string } {
  let count = 0;
  let oversizedHeader: string | null = null;

  // Iterate using forEach which is supported by Headers
  headers.forEach((value, name) => {
    count++;

    // Check individual header size
    const headerSize = name.length + value.length;
    if (headerSize > limits.maxHeaderSize && !oversizedHeader) {
      oversizedHeader = name;
    }
  });

  // Return error if found
  if (oversizedHeader) {
    return { error: `Header '${oversizedHeader}' exceeds maximum size` };
  }

  // Check header count
  if (count > limits.maxHeaderCount) {
    return { error: "Too many headers" };
  }

  return {};
}
