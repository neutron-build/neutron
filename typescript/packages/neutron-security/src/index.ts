import {
  getCookie,
  serializeCookie,
  type AppContext,
  type CookieSerializeOptions,
  type MiddlewareFn,
} from "neutron";

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
  forwardedHeader?: string;
  maxForwardedIps?: number;
}

export interface RateLimitMiddlewareOptions {
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
    const response = await next();
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
  const cookieOptions = resolveSecureCookieOptions({
    path: "/",
    sameSite: "Lax",
    httpOnly: false,
    ...options.cookie,
  });

  return async (request, context, next) => {
    const method = request.method.toUpperCase();
    const existingToken = getCookie(request, cookieName) || "";
    const csrfToken = existingToken || createNonce();
    context[contextKey] = csrfToken;

    if (!safeMethods.has(method)) {
      const headerToken = request.headers.get(headerName) || "";
      const formToken = await readFormToken(request, formFieldName);
      const submittedToken = headerToken || formToken;
      if (!existingToken || !submittedToken || submittedToken !== existingToken) {
        return new Response("Invalid CSRF token", { status: 403 });
      }
    }

    const response = await next();
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

  const maxForwardedIps = Math.max(1, options.maxForwardedIps ?? 5);
  const forwardedHeader = (options.forwardedHeader || "x-forwarded-for").toLowerCase();

  const cfConnectingIp = request.headers.get("cf-connecting-ip");
  if (cfConnectingIp) {
    return cfConnectingIp.trim();
  }

  const xRealIp = request.headers.get("x-real-ip");
  if (xRealIp) {
    return xRealIp.trim();
  }

  const xForwardedFor = request.headers.get(forwardedHeader);
  if (!xForwardedFor || !trustProxy) {
    return null;
  }

  const ips = xForwardedFor
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean)
    .slice(0, maxForwardedIps);

  return ips.length > 0 ? ips[0] : null;
}

export function createRateLimitMiddleware(
  options: RateLimitMiddlewareOptions
): MiddlewareFn {
  const capacity = Math.max(1, Math.floor(options.capacity));
  const refillPerSecond = Math.max(0.001, options.refillPerSecond ?? capacity);
  const tokensPerRequest = Math.max(1, options.tokensPerRequest ?? 1);
  const denyStatus = options.denyStatus ?? 429;
  const maxBuckets = Math.max(1_000, Math.floor(options.maxBuckets ?? 50_000));
  const bucketTtlMs = Math.max(
    1_000,
    Math.floor(options.bucketTtlMs ?? (capacity / refillPerSecond) * 4_000)
  );
  const cleanupEvery = Math.max(1, Math.floor(options.cleanupEvery ?? 128));
  const buckets = new Map<string, { tokens: number; lastRefillMs: number }>();
  let handledRequests = 0;

  return async (request, context, next) => {
    const key = options.key
      ? await options.key(request, context)
      : resolveClientIp(request) || "anonymous";
    const now = Date.now();
    handledRequests += 1;
    if (handledRequests % cleanupEvery === 0 || buckets.size > maxBuckets) {
      pruneBuckets(buckets, now, maxBuckets, bucketTtlMs);
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
    const response = await next();
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

  if (buckets.size <= maxBuckets) {
    return;
  }

  const overflow = buckets.size - maxBuckets;
  const oldest = Array.from(buckets.entries())
    .sort((left, right) => left[1].lastRefillMs - right[1].lastRefillMs)
    .slice(0, overflow);
  for (const [key] of oldest) {
    buckets.delete(key);
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
