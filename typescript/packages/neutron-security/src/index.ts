import { mutableResponse } from "@neutron-build/core";
import { isIP } from "node:net";
import { transportPeer } from "@neutron-build/core";
import { timingSafeEqual } from "node:crypto";
import {
  getCookie,
  serializeCookie,
  type AppContext,
  type CookieSerializeOptions,
  type MiddlewareFn,
} from "@neutron-build/core";

export interface CspNonceMiddlewareOptions {
  contextKey?: string;
  headerName?: string;
  policy?:
    | string
    | ((args: { nonce: string; request: Request; context: AppContext }) => string);
}

export interface CsrfMiddlewareOptions {
  cookieName?: string;
  headerName?: string;
  formFieldName?: string;
  safeMethods?: string[];
  cookie?: CookieSerializeOptions;
  contextKey?: string;
}

export interface TrustedProxyOptions {
  trustProxy?: boolean;
  /** Verify the transport socket address before trusting forwarded identities. */
  trustedProxies?: string[] | ((address: string) => boolean);
  forwardedHeader?: string;
  maxForwardedIps?: number;
  /**
   * Number of trusted proxies between this server and the client. The client
   * IP is read this many hops from the right of the forwarded chain, since the
   * left-most entries are attacker-controlled. Default 1.
   */
  trustedHops?: number;
}

export interface RateLimitMiddlewareOptions extends TrustedProxyOptions {
  capacity: number;
  refillPerSecond?: number;
  tokensPerRequest?: number;
  key?: (request: Request, context: AppContext) => string | Promise<string>;
  denyStatus?: number;
  maxBuckets?: number;
  bucketTtlMs?: number;
  cleanupEvery?: number;
}

export interface SecureCookieDefaultsOptions extends CookieSerializeOptions {
  nodeEnv?: string;
}

const DEFAULT_CSP_CONTEXT_KEY = "cspNonce";
const DEFAULT_CSRF_CONTEXT_KEY = "csrfToken";

export function createCspNonceMiddleware(
  options: CspNonceMiddlewareOptions = {}
): MiddlewareFn {
  const contextKey = options.contextKey || DEFAULT_CSP_CONTEXT_KEY;
  const headerName = options.headerName || "Content-Security-Policy";

  return async (request, context, next) => {
    const nonce = createNonce();
    context[contextKey] = nonce;
    const response = mutableResponse(await next());
    if (!response.headers.has(headerName)) {
      response.headers.set(
        headerName,
        resolvePolicy(options.policy, { nonce, request, context })
      );
    }
    return response;
  };
}

export function getCspNonceFromContext(
  context: AppContext,
  contextKey: string = DEFAULT_CSP_CONTEXT_KEY
): string | null {
  const value = context[contextKey];
  return typeof value === "string" ? value : null;
}

export function createCsrfMiddleware(
  options: CsrfMiddlewareOptions = {}
): MiddlewareFn {
  const cookieName = options.cookieName || "__neutron_csrf";
  const headerName = (options.headerName || "x-csrf-token").toLowerCase();
  const formFieldName = options.formFieldName || "_csrf";
  const safeMethods = new Set(
    (options.safeMethods || ["GET", "HEAD", "OPTIONS"]).map((method) =>
      method.toUpperCase()
    )
  );
  const contextKey = options.contextKey || DEFAULT_CSRF_CONTEXT_KEY;
  // Secure-by-default double-submit: the token is exposed to the page through
  // context (rendered into forms / a meta tag), so the cookie itself stays
  // HttpOnly and SameSite=Strict. This blocks JS/subdomain cookie reads while
  // the page still has the value it needs to echo back in the header/field.
  const cookieOptions = resolveSecureCookieOptions({
    path: "/",
    sameSite: "Strict",
    httpOnly: true,
    ...options.cookie,
  });

  return async (request, context, next) => {
    const method = request.method.toUpperCase();
    const existingToken = getCookie(request, cookieName) || "";
    const csrfToken = existingToken || createNonce();
    context[contextKey] = csrfToken;

    if (!safeMethods.has(method)) {
      if (!isSameOrigin(request)) {
        return new Response("Invalid CSRF origin", { status: 403 });
      }
      // Prefer the header token; only fall back to parsing the (potentially
      // large) form body when no header was supplied.
      const headerToken = request.headers.get(headerName) || "";
      const submittedToken =
        headerToken || (await readFormToken(request, formFieldName));
      if (
        !existingToken ||
        !submittedToken ||
        !timingSafeEqualStr(submittedToken, existingToken)
      ) {
        return new Response("Invalid CSRF token", { status: 403 });
      }
    }

    const response = mutableResponse(await next());
    if (!existingToken) {
      response.headers.append(
        "Set-Cookie",
        serializeCookie(cookieName, csrfToken, cookieOptions)
      );
    }
    return response;
  };
}

export function getCsrfTokenFromContext(
  context: AppContext,
  contextKey: string = DEFAULT_CSRF_CONTEXT_KEY
): string | null {
  const value = context[contextKey];
  return typeof value === "string" ? value : null;
}

export function resolveClientIp(
  request: Request,
  options: TrustedProxyOptions = {}
): string | null {
  const trustProxy = options.trustProxy ?? false;
  if (!trustProxy) {
    return null;
  }

  const peer = transportPeer(request)?.remoteAddress;
  const trusted = options.trustedProxies;
  if (!peer || !trusted || !(typeof trusted === "function" ? trusted(peer) : trusted.includes(peer))) return null;
  const maxForwardedIps = options.maxForwardedIps ?? 5;
  const trustedHops = options.trustedHops ?? 1;
  if (!Number.isSafeInteger(maxForwardedIps) || maxForwardedIps < 1 || !Number.isSafeInteger(trustedHops) || trustedHops < 1) return null;
  const forwardedHeader = (options.forwardedHeader || "x-forwarded-for").toLowerCase();
  const ips = (request.headers.get(forwardedHeader) ?? "").split(",").map(value => value.trim());
  if (ips.length < trustedHops || ips.length > maxForwardedIps || ips.some(ip => !isIP(ip))) return null;
  return ips[ips.length - trustedHops] ?? null;
}

export function createRateLimitMiddleware(
  options: RateLimitMiddlewareOptions
): MiddlewareFn {
  for (const [name, value] of Object.entries(options)) {
    if (typeof value === "number" && !Number.isFinite(value)) throw new RangeError(`${name} must be finite`);
  }
  if (options.capacity <= 0 || (options.refillPerSecond !== undefined && options.refillPerSecond <= 0) || (options.tokensPerRequest !== undefined && options.tokensPerRequest <= 0)) throw new RangeError("Rate limit values must be positive");
  for (const name of ["maxBuckets", "cleanupEvery", "trustedHops", "maxForwardedIps"] as const) {
    const value = options[name];
    if (value !== undefined && (!Number.isSafeInteger(value) || value < 1)) throw new RangeError(`${name} must be a positive integer`);
  }
  if (options.bucketTtlMs !== undefined && options.bucketTtlMs <= 0) throw new RangeError("bucketTtlMs must be positive");
  if (options.denyStatus !== undefined && (!Number.isInteger(options.denyStatus) || options.denyStatus < 400 || options.denyStatus > 599)) throw new RangeError("denyStatus must be an HTTP error status");
  const capacity = Math.max(1, Math.floor(options.capacity));
  const refillPerSecond = Math.max(0.001, options.refillPerSecond ?? capacity);
  const tokensPerRequest = Math.max(1, options.tokensPerRequest ?? 1);
  const denyStatus = options.denyStatus ?? 429;
  const maxBuckets = Math.max(1, Math.floor(options.maxBuckets ?? 50_000));
  const bucketTtlMs = Math.max(
    1_000,
    Math.floor(Math.max(options.bucketTtlMs ?? 0, (capacity / refillPerSecond) * 1_000))
  );
  const cleanupEvery = Math.max(1, Math.floor(options.cleanupEvery ?? 128));
  const buckets = new Map<string, { tokens: number; lastRefillMs: number }>();
  let handledRequests = 0;

  return async (request, context, next) => {
    // Without a key fn, bucket per client IP. The proxy options MUST flow
    // through: calling resolveClientIp(request) bare always returned null
    // (trustProxy defaults false), collapsing every visitor into one global
    // "anonymous" bucket — one abusive client 429'd the whole site. With no
    // trusted IP there is no per-client signal in a standard Request, so the
    // shared bucket is the honest fallback; opt into trustProxy behind a
    // proxy, or supply a key fn.
    const key = options.key
      ? await options.key(request, context)
      : resolveClientIp(request, options) || "anonymous";
    const now = Date.now();
    handledRequests += 1;
    if (handledRequests % cleanupEvery === 0 || buckets.size >= maxBuckets) {
      pruneBuckets(buckets, now, maxBuckets, bucketTtlMs);
    }

    if (!buckets.has(key) && buckets.size >= maxBuckets) {
      return new Response("Rate limit capacity exhausted", { status: 429, headers: { "Retry-After": "1" } });
    }
    const state = buckets.get(key) || { tokens: capacity, lastRefillMs: now };
    const elapsedSeconds = Math.max(0, (now - state.lastRefillMs) / 1000);
    const replenished = Math.min(capacity, state.tokens + elapsedSeconds * refillPerSecond);
    const remainingAfter = replenished - tokensPerRequest;

    if (remainingAfter < 0) {
      const retryAfterSec = Math.ceil((tokensPerRequest - replenished) / refillPerSecond);
      const denied = new Response("Too Many Requests", { status: denyStatus });
      denied.headers.set("Retry-After", String(Math.max(1, retryAfterSec)));
      denied.headers.set("RateLimit-Limit", String(capacity));
      denied.headers.set("RateLimit-Remaining", "0");
      denied.headers.set("RateLimit-Reset", String(Math.max(1, retryAfterSec)));
      buckets.set(key, { tokens: replenished, lastRefillMs: now });
      return denied;
    }

    buckets.set(key, { tokens: remainingAfter, lastRefillMs: now });
    const response = mutableResponse(await next());
    const resetSec = Math.ceil((capacity - remainingAfter) / refillPerSecond);
    response.headers.set("RateLimit-Limit", String(capacity));
    response.headers.set("RateLimit-Remaining", String(Math.floor(remainingAfter)));
    response.headers.set("RateLimit-Reset", String(Math.max(1, resetSec)));
    return response;
  };
}

function pruneBuckets(
  buckets: Map<string, { tokens: number; lastRefillMs: number }>,
  now: number,
  maxBuckets: number,
  bucketTtlMs: number
): void {
  if (buckets.size === 0) {
    return;
  }

  for (const [key, state] of buckets) {
    if (now - state.lastRefillMs > bucketTtlMs) {
      buckets.delete(key);
    }
  }


}

export function resolveSecureCookieOptions(
  options: SecureCookieDefaultsOptions = {}
): CookieSerializeOptions {
  const nodeEnv = options.nodeEnv || process.env.NODE_ENV || "development";
  const isProduction = nodeEnv === "production";
  return {
    path: options.path ?? "/",
    domain: options.domain,
    httpOnly: options.httpOnly ?? true,
    sameSite: options.sameSite ?? "Lax",
    secure: options.secure ?? isProduction,
    expires: options.expires,
    maxAge: options.maxAge,
  };
}

function resolvePolicy(
  policy: CspNonceMiddlewareOptions["policy"],
  args: { nonce: string; request: Request; context: AppContext }
): string {
  if (typeof policy === "function") {
    return policy(args);
  }
  if (typeof policy === "string" && policy.trim().length > 0) {
    return policy.replace(/\{\{\s*nonce\s*\}\}/g, args.nonce);
  }

  return [
    "default-src 'self'",
    `script-src 'self' 'nonce-${args.nonce}'`,
    "style-src 'self' 'unsafe-inline'",
    "object-src 'none'",
    "base-uri 'self'",
    "frame-ancestors 'none'",
  ].join("; ");
}

function timingSafeEqualStr(a: string, b: string): boolean {
  const aBuf = Buffer.from(a, "utf8");
  const bBuf = Buffer.from(b, "utf8");
  if (aBuf.length !== bBuf.length) {
    return false;
  }
  return timingSafeEqual(aBuf, bBuf);
}

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

async function readFormToken(
  request: Request,
  fieldName: string
): Promise<string> {
  const contentType = request.headers.get("content-type") || "";
  if (!contentType.includes("application/x-www-form-urlencoded") &&
      !contentType.includes("multipart/form-data")) {
    return "";
  }

  try {
    const formData = await request.clone().formData();
    const value = formData.get(fieldName);
    return typeof value === "string" ? value : "";
  } catch {
    return "";
  }
}

function createNonce(): string {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  return toBase64Url(bytes);
}

function toBase64Url(bytes: Uint8Array): string {
  if (typeof Buffer !== "undefined") {
    return Buffer.from(bytes)
      .toString("base64")
      .replace(/\+/g, "-")
      .replace(/\//g, "_")
      .replace(/=+$/g, "");
  }

  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary)
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}
