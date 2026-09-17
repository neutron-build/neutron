/**
 * CSRF (Cross-Site Request Forgery) Protection Middleware
 *
 * Provides token-based CSRF protection for state-changing HTTP methods.
 * Tokens are stored in cookies and must be provided in request headers for validation.
 */

import { randomBytes, timingSafeEqual } from "node:crypto";
import { serializeCookie, getCookie } from "../core/cookies.js";
import type { MiddlewareFn } from "../core/types.js";

export interface CsrfOptions {
  /**
   * Name of the cookie that stores the CSRF token
   * @default "_csrf"
   */
  cookieName?: string;

  /**
   * Name of the HTTP header that must contain the CSRF token
   * @default "x-csrf-token"
   */
  headerName?: string;

  /**
   * HTTP methods that don't require CSRF validation
   * @default ["GET", "HEAD", "OPTIONS"]
   */
  ignoredMethods?: string[];

  /**
   * Cookie options
   */
  cookieOptions?: {
    /**
     * Cookie path
     * @default "/"
     */
    path?: string;

    /**
     * Cookie SameSite attribute
     * @default "Strict"
     */
    sameSite?: "Strict" | "Lax" | "None";

    /**
     * Whether to set Secure flag (HTTPS only)
     * @default true in production
     */
    secure?: boolean;
  };
}

/**
 * Creates CSRF protection middleware
 *
 * For safe methods (GET, HEAD, OPTIONS):
 * - Generates a new CSRF token
 * - Sets it in a cookie
 * - Makes it available in context for rendering in forms
 *
 * For unsafe methods (POST, PUT, DELETE, PATCH):
 * - Validates that the token in the cookie matches the token in the header
 * - Returns 403 Forbidden if validation fails
 *
 * The token travels in a HEADER (fetch/SPA) — the middleware does not read
 * form bodies. To submit from a native form, POST with the header via fetch.
 *
 * @example
 * ```ts
 * import { csrfMiddleware } from "@neutron-build/core/server";
 *
 * export const middleware = [
 *   sessionMiddleware(...),
 *   csrfMiddleware({
 *     cookieName: "_csrf",
 *     headerName: "x-csrf-token"
 *   })
 * ];
 * ```
 *
 * In your client code, echo the cookie token in the header:
 * ```ts
 * // token is readable from the _csrf cookie (not HttpOnly)
 * await fetch("/submit", {
 *   method: "POST",
 *   headers: { "x-csrf-token": readCookie("_csrf") },
 * });
 * ```
 */
export function csrfMiddleware(options: CsrfOptions = {}): MiddlewareFn {
  const cookieName = options.cookieName || "_csrf";
  const headerName = options.headerName || "x-csrf-token";
  // Normalize configured safe methods to uppercase (TS-23): the method is
  // compared as `method.toUpperCase()`, so a lowercase `["get"]` never
  // matched and GET requests were being token-checked instead of exempt.
  const ignoredMethods = new Set(
    (options.ignoredMethods || ["GET", "HEAD", "OPTIONS"]).map((m) =>
      m.toUpperCase()
    )
  );
  const cookieOpts = options.cookieOptions || {};
  const cookiePath = cookieOpts.path || "/";
  const cookieSameSite = cookieOpts.sameSite || "Strict";
  const cookieSecure =
    cookieOpts.secure !== undefined
      ? cookieOpts.secure
      : process.env.NODE_ENV === "production";

  return async (request, context, next) => {
    const method = request.method.toUpperCase();

    // For safe methods: reuse the existing token if present, otherwise mint one.
    if (ignoredMethods.has(method)) {
      const existing = getCookie(request, cookieName);
      const token = existing || randomBytes(32).toString("hex");
      context.csrfToken = token;

      const response = await next();

      // Only (re)set the cookie when we minted a new token. Regenerating on
      // every safe request churns the cookie and can race a token already
      // embedded in an in-flight form, causing spurious 403s on submit.
      // The response from `next()` can carry immutable headers (native
      // redirects, proxied responses) — rewrap instead of appending in
      // place (TS-17).
      if (!existing) {
        const cookieString = serializeCookie(cookieName, token, {
          path: cookiePath,
          httpOnly: true,
          secure: cookieSecure,
          sameSite: cookieSameSite,
        });
        return withSetCookie(response, cookieString);
      }

      return response;
    }

    // For unsafe methods: verify same-origin first (defense in depth — a
    // cross-site attacker's browser stamps Origin/Referer with its own site),
    // then validate the double-submit token in constant time.
    if (!isSameOrigin(request)) {
      return new Response("CSRF origin validation failed", {
        status: 403,
        headers: {
          "Content-Type": "text/plain",
        },
      });
    }

    // SECURITY: Use getCookie from cookies.ts for proper parsing (handles URL encoding, quotes, etc.)
    const cookieToken = getCookie(request, cookieName);
    const headerToken = request.headers.get(headerName);

    if (!cookieToken || !headerToken || !timingSafeEqualStr(cookieToken, headerToken)) {
      return new Response("CSRF token validation failed", {
        status: 403,
        headers: {
          "Content-Type": "text/plain",
        },
      });
    }

    // Token is valid, proceed
    context.csrfToken = cookieToken;
    return next();
  };
}

/**
 * Constant-time comparison of two strings. Returns false for unequal lengths
 * (without an early-exit timing signal on the contents themselves).
 */
function timingSafeEqualStr(a: string, b: string): boolean {
  const aBuf = Buffer.from(a, "utf8");
  const bBuf = Buffer.from(b, "utf8");
  if (aBuf.length !== bBuf.length) {
    return false;
  }
  return timingSafeEqual(aBuf, bBuf);
}

/**
 * Same-origin check for state-changing requests. When an Origin (or Referer)
 * header is present its ORIGIN — scheme + host + port — must match the
 * request's (TS-23): comparing bare `.host` ignored the scheme, so an
 * http page and its https namesake were treated as same-origin. A forged
 * cross-site request carries the attacker's origin and is rejected. Absent
 * both headers we defer to token validation rather than hard-failing
 * legitimate non-browser clients.
 */
function isSameOrigin(request: Request): boolean {
  const source = request.headers.get("Origin") || request.headers.get("Referer");
  if (!source) {
    return true;
  }
  try {
    return new URL(source).origin === new URL(request.url).origin;
  } catch {
    return false;
  }
}

/**
 * Return `response` with `cookie` appended, rewrapping when the response's
 * headers are immutable (native redirects, proxied responses) — see TS-17.
 */
function withSetCookie(response: Response, cookie: string): Response {
  if (response.status >= 200 && response.status < 600) {
    try {
      const headers = new Headers(response.headers);
      headers.append("Set-Cookie", cookie);
      return new Response(response.body, {
        status: response.status,
        statusText: response.statusText,
        headers,
      });
    } catch {
      // Exotic runtimes whose Response constructor rejects a used body.
    }
  }
  response.headers.append("Set-Cookie", cookie);
  return response;
}

/**
 * Augment the context type to include csrfToken
 */
declare module "../core/types.js" {
  interface AppContext {
    /**
     * CSRF token for the current request
     * Available when using csrfMiddleware
     */
    csrfToken?: string;
  }
}
