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
 *
 * TS-22 split of duties: this middleware keeps the Content-Length EARLY
 * check (cheap, before any body byte is read). The actual-byte cap for
 * streamed bodies — chunked requests with no Content-Length, or a lying
 * one — is enforced where the request stream is created: the server
 * adapter wraps the body with {@link capRequestBody} (see
 * `maxRequestBodyBytes` on `NeutronServerOptions`). The middleware cannot
 * substitute the Request body downstream handlers read; the adapter can.
 */

import type { MiddlewareFn } from "../core/types.js";

/**
 * Raised by the adapter's capped body stream when a request's ACTUAL body
 * bytes exceed the configured cap. Mapped to a 413 by the app-level error
 * handler — the same status the Content-Length early check produces.
 */
export class RequestBodyTooLargeError extends Error {
  constructor(readonly capBytes: number) {
    super(`Request body exceeded the ${capBytes}-byte cap while streaming`);
    this.name = "RequestBodyTooLargeError";
  }
}

/**
 * Wrap a request's body in a counting stream that errors with
 * {@link RequestBodyTooLargeError} past `capBytes` and cancels the source
 * (the sender stops being read). Bodyless methods pass through untouched.
 *
 * This is the server-adapter half of TS-22: it runs where the Request is
 * handed to the app, so downstream `request.text()`/`json()` reads observe
 * the cap on real bytes, declared length or not.
 */
export function capRequestBody(request: Request, capBytes: number): Request {
  const method = request.method.toUpperCase();
  if (method === "GET" || method === "HEAD" || method === "OPTIONS") {
    return request;
  }
  if (!request.body) {
    return request;
  }
  const source = request.body.getReader();
  let seen = 0;
  const capped = new ReadableStream<Uint8Array>({
    async pull(controller) {
      const { done, value } = await source.read();
      if (done) {
        controller.close();
        return;
      }
      seen += value.byteLength;
      if (seen > capBytes) {
        // Stop reading the sender and fail the consumer: the bytes already
        // buffered past the cap never reach the handler.
        await source.cancel().catch(() => {});
        controller.error(new RequestBodyTooLargeError(capBytes));
        return;
      }
      controller.enqueue(value);
    },
    cancel(reason) {
      source.cancel(reason).catch(() => {});
    },
  });
  return new Request(request.url, {
    method: request.method,
    headers: request.headers,
    body: capped,
    // A streaming body requires half-duplex in fetch-land Request
    // construction; without it Node's undici rejects the init. Older
    // lib.dom RequestInit typings predate the duplex option, hence the cast.
    duplex: "half",
  } as RequestInit);
}

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

    // Validate request body size for methods that may have a body. DELETE is
    // included (TS-22): a typed DELETE handler can bind a body, so a large
    // one deserved the same early rejection as POST/PUT/PATCH.
    const method = request.method.toUpperCase();
    if (
      method === "POST" ||
      method === "PUT" ||
      method === "PATCH" ||
      method === "DELETE"
    ) {
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
      // The ACTUAL-byte cap for streamed/undeclared bodies is enforced by the
      // server adapter (`maxRequestBodyBytes` → `capRequestBody`), which owns
      // the request stream — see the module docs. Configure both for
      // defense-in-depth, or enable `rejectUnknownLength`.
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
