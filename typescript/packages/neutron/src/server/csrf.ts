/**
 * CSRF (Cross-Site Request Forgery) Protection Middleware
 *
 * Provides token-based CSRF protection for state-changing HTTP methods.
 * Tokens are stored in cookies and must be provided in request headers for validation.
 */

import { randomBytes } from "node:crypto";
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
 * @example
 * ```ts
 * import { csrfMiddleware } from "neutron/server";
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
 * In your forms:
 * ```tsx
 * <form method="POST">
 *   <input type="hidden" name="_csrf" value={context.csrfToken} />
 *   {" "}
 * </form>
 * ```
 */
export function csrfMiddleware(options: CsrfOptions = {}): MiddlewareFn {
  const cookieName = options.cookieName || "_csrf";
  const headerName = options.headerName || "x-csrf-token";
  const ignoredMethods = new Set(
    options.ignoredMethods || ["GET", "HEAD", "OPTIONS"]
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

    // For safe methods: generate token and set cookie
    if (ignoredMethods.has(method)) {
      const token = randomBytes(32).toString("hex");
      context.csrfToken = token;

      const response = await next();

      // Set CSRF token in cookie using serializeCookie for proper validation
      const cookieString = serializeCookie(cookieName, token, {
        path: cookiePath,
        httpOnly: true,
        secure: cookieSecure,
        sameSite: cookieSameSite,
      });

      response.headers.append("Set-Cookie", cookieString);

      return response;
    }

    // For unsafe methods: validate token
    // SECURITY: Use getCookie from cookies.ts for proper parsing (handles URL encoding, quotes, etc.)
    const cookieToken = getCookie(request, cookieName);
    const headerToken = request.headers.get(headerName);

    if (!cookieToken || !headerToken || cookieToken !== headerToken) {
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
 * Augment the context type to include csrfToken
 */
declare module "../core/types.js" {
  interface RouteContext {
    /**
     * CSRF token for the current request
     * Available when using csrfMiddleware
     */
    csrfToken?: string;
  }
}
