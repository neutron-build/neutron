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

  /**
   * Reject body-bearing requests (POST/PUT/PATCH) that do not declare a valid
   * Content-Length (e.g. chunked transfer). Enable for deployments NOT fronted
   * by a proxy that bounds request bodies. Off by default to avoid breaking
   * legitimate streaming clients.
   * @default false
   */
  rejectUnknownLength?: boolean;
}

const DEFAULT_LIMITS: Required<InputLimitsOptions> = {
  maxRequestBodySize: 10 * 1024 * 1024, // 10MB
  maxHeaderSize: 16 * 1024, // 16KB
  maxHeaderCount: 100,
  maxUrlLength: 2048,
  rejectUnknownLength: false,
};

/**
 * Creates middleware that enforces input validation limits
 *
 * @example
 * ```ts
 * import { inputLimitsMiddleware } from "@neutron-build/core/server";
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
    rejectUnknownLength: options.rejectUnknownLength ?? DEFAULT_LIMITS.rejectUnknownLength,
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
      const transferEncoding = request.headers.get("transfer-encoding");

      // SECURITY: A request that declares both Content-Length and
      // Transfer-Encoding is ambiguous and a classic request-smuggling vector —
      // reject it outright.
      if (contentLength && transferEncoding) {
        return new Response("Ambiguous request framing", {
          status: 400,
          headers: { "Content-Type": "text/plain" },
        });
      }

      if (contentLength) {
        const bodySize = Number(contentLength);
        if (!Number.isInteger(bodySize) || bodySize < 0) {
          return new Response("Invalid Content-Length", {
            status: 400,
            headers: { "Content-Type": "text/plain" },
          });
        }
        if (bodySize > limits.maxRequestBodySize) {
          return new Response("Request body too large", {
            status: 413, // Payload Too Large
            headers: { "Content-Type": "text/plain" },
          });
        }
      } else if (limits.rejectUnknownLength) {
        // No declared length (e.g. chunked). When the deployment isn't behind a
        // proxy that bounds bodies, opt into rejecting these.
        return new Response("Length Required", {
          status: 411,
          headers: { "Content-Type": "text/plain" },
        });
      }

      // SECURITY: For requests with a declared length we enforce the cap above.
      // True byte-accurate enforcement of streamed/chunked bodies has to happen
      // where the request stream is created (the server adapter) or at the
      // reverse proxy (nginx/cloudflare) — this middleware cannot replace the
      // request body that downstream handlers will read. Configure body limits
      // there as defense-in-depth, or enable `rejectUnknownLength`.
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

    // Check individual header size in bytes (multi-byte values undercount when
    // measured by string length / UTF-16 code units).
    const headerSize = Buffer.byteLength(name, "utf8") + Buffer.byteLength(value, "utf8");
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
