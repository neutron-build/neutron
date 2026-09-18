import { randomUUID } from "node:crypto";
import {
  getCookie,
  serializeCookie,
  type CookieSerializeOptions,
} from "../core/cookies.js";
import type { AppContext, MiddlewareFn } from "../core/types.js";
import { transportPeer } from "./peer.js";

export interface SessionData {
  [key: string]: unknown;
}

export interface Session {
  readonly id: string;
  readonly isNew: boolean;
  readonly isDirty: boolean;
  readonly isDestroyed: boolean;
  readonly isRegenerated: boolean;
  get<T = unknown>(key: string): T | undefined;
  set(key: string, value: unknown): void;
  unset(key: string): void;
  destroy(): void;
  /**
   * Regenerate the session ID
   *
   * SECURITY: Call this method when a user's privilege level changes
   * (e.g., after login, logout, or privilege escalation) to prevent
   * session fixation attacks.
   *
   * @example
   * ```ts
   * // After successful login
   * const session = getSession(context);
   * session.regenerate();
   * session.set("userId", user.id);
   * ```
   */
  regenerate(): void;
  toJSON(): SessionData;
}

export interface SessionStorage {
  getSession(sessionId: string): Promise<SessionRecord | null>;
  setSession(sessionId: string, data: SessionData, expiresAt?: number): Promise<void>;
  deleteSession(sessionId: string): Promise<void>;
}

export interface SessionRecord {
  data: SessionData;
  expiresAt?: number;
}

export interface MemorySessionStorageOptions {
  ttlSeconds?: number;
  maxSessions?: number;
}

export interface SessionCookieOptions extends CookieSerializeOptions {
  name?: string;
}

export interface SessionMiddlewareOptions {
  storage: SessionStorage;
  cookie?: SessionCookieOptions;
  ttlSeconds?: number;
  /**
   * List of trusted proxy IP addresses or IPv4 CIDR ranges.
   *
   * SECURITY (TS-32): the nearest hop is identified by the TRANSPORT's
   * socket address (installed by the node adapter), never by forwarding
   * headers — a directly connected client forging `X-Real-IP` cannot
   * impersonate a trusted proxy. When this list is omitted (or the adapter
   * supplied no peer metadata for the request), X-Forwarded-Proto is
   * ignored entirely and Secure falls back to the request's own protocol
   * (plus the production default). IPv6 entries match by exact string only;
   * CIDR ranges are IPv4.
   *
   * @example
   * ```ts
   * trustedProxies: ['127.0.0.1', '::1', '10.0.0.0/8']
   * ```
   */
  trustedProxies?: string[];
}

const SESSION_CONTEXT_KEY = "session";

/**
 * Return `response` with `cookie` appended, without mutating the original
 * (TS-17): responses from `next()` can carry immutable header guards — a
 * native `Response.redirect()`, a network-fetched response — and appending
 * to those throws AFTER session persistence has already happened, turning a
 * login into a 500. Rewrapping with a mutable header copy is lossless for
 * normal HTTP responses (body stream, status, statusText carried over).
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
      // Fall through and attempt the direct append — e.g. an exotic runtime
      // whose Response constructor rejects a used body.
    }
  }
  response.headers.append("Set-Cookie", cookie);
  return response;
}

export function createMemorySessionStorage(
  options: MemorySessionStorageOptions = {}
): SessionStorage {
  const map = new Map<string, SessionRecord>();

  // SECURITY: Limit TTL to prevent integer overflow (max 1 year)
  const MAX_TTL_SECONDS = 365 * 24 * 60 * 60; // 1 year
  const ttlSeconds = options.ttlSeconds || 0;
  const clampedTtl = ttlSeconds > 0 ? Math.min(ttlSeconds, MAX_TTL_SECONDS) : 0;
  const defaultTtlMs = clampedTtl > 0 ? Math.floor(clampedTtl * 1000) : undefined;

  const maxSessions = options.maxSessions && options.maxSessions > 0 ? options.maxSessions : 10000;
  let writeCount = 0;

  function lazySweep(): void {
    writeCount++;
    const shouldSweep = map.size > 1000 && writeCount % 100 === 0;
    const shouldEvict = map.size >= maxSessions;

    if (!shouldSweep && !shouldEvict) return;

    const now = Date.now();
    for (const [key, record] of map) {
      if (record.expiresAt && record.expiresAt <= now) {
        map.delete(key);
      }
    }

    // Evict until at or below capacity (TS-16): the old fixed batch of
    // `floor(maxSessions * 0.1)` removed ZERO entries for any capacity
    // below 10, so small stores grew without bound. Evicting oldest-first
    // (insertion order is LRU order — access promotes recency in
    // getSession) always makes progress. Session replacement above
    // refreshes its own recency first, so an update to an existing session
    // never evicts itself.
    if (map.size > maxSessions) {
      for (const key of map.keys()) {
        if (map.size <= maxSessions) {
          break;
        }
        map.delete(key);
      }
    }
  }

  return {
    async getSession(sessionId) {
      const record = map.get(sessionId);
      if (!record) {
        return null;
      }

      if (record.expiresAt && record.expiresAt <= Date.now()) {
        map.delete(sessionId);
        return null;
      }

      // Promote to most-recently-used: Map preserves insertion order, so
      // re-inserting moves this key to the end and eviction drops genuinely
      // cold sessions rather than recently-active ones.
      map.delete(sessionId);
      map.set(sessionId, record);

      // Ownership boundary (TS-20): a deep clone at egress, so mutating a
      // nested value in the returned data cannot alter the stored record
      // another request reads. Session data must be structured-clone
      // compatible — persistent backends JSON-encode it anyway.
      return {
        data: structuredClone(record.data),
        expiresAt: record.expiresAt,
      };
    },

    async setSession(sessionId, data, expiresAt) {
      const ttlExpiry =
        defaultTtlMs && !expiresAt ? Date.now() + defaultTtlMs : expiresAt;
      // Re-inserting an existing id refreshes its LRU recency rather than
      // leaving it where it was inserted (TS-16).
      map.delete(sessionId);
      map.set(sessionId, {
        // Deep clone at ingress (TS-20): the caller keeps its reference
        // after saving, and a shallow copy left nested objects shared.
        data: structuredClone(data),
        expiresAt: ttlExpiry,
      });
      lazySweep();
    },

    async deleteSession(sessionId) {
      map.delete(sessionId);
    },
  };
}

export function sessionMiddleware(options: SessionMiddlewareOptions): MiddlewareFn {
  const cookieName = options.cookie?.name || "__neutron_session";
  const ttlSeconds =
    Number.isFinite(options.ttlSeconds) && (options.ttlSeconds || 0) > 0
      ? Math.floor(options.ttlSeconds!)
      : undefined;
  const baseCookieOptions = normalizeCookieOptions(options.cookie);
  const trustedProxies = options.trustedProxies;

  return async (request, context, next) => {
    const cookieSessionId = getCookie(request, cookieName);
    const loadedRecord = cookieSessionId
      ? await options.storage.getSession(cookieSessionId)
      : null;
    const cookieOptions = resolveCookieOptionsForRequest(baseCookieOptions, request, trustedProxies);

    const session = createSessionImpl(
      cookieSessionId && loadedRecord ? cookieSessionId : createSessionId(),
      loadedRecord?.data || {},
      !cookieSessionId || !loadedRecord
    );

    context[SESSION_CONTEXT_KEY] = session;

    const response = await next();

    if (session.isDestroyed) {
      if (cookieSessionId) {
        await options.storage.deleteSession(cookieSessionId);
      }
      return withSetCookie(
        response,
        serializeCookie(cookieName, "", {
          ...cookieOptions,
          maxAge: 0,
          expires: new Date(0),
        })
      );
    }

    if (session.isDirty || session.isNew) {
      // SECURITY: Delete old session if regenerated (prevents session fixation)
      if (session.isRegenerated && cookieSessionId) {
        await options.storage.deleteSession(cookieSessionId);
      }
      // Delete orphaned session from storage if the ID changed
      else if (cookieSessionId && cookieSessionId !== session.id) {
        await options.storage.deleteSession(cookieSessionId);
      }
      const expiresAt = ttlSeconds ? Date.now() + ttlSeconds * 1000 : undefined;
      await options.storage.setSession(session.id, session.toJSON(), expiresAt);
      return withSetCookie(
        response,
        serializeCookie(cookieName, session.id, {
          ...cookieOptions,
          ...(ttlSeconds ? { maxAge: ttlSeconds } : {}),
        })
      );
    }

    return response;
  };
}

export function getSessionFromContext(context: AppContext): Session | undefined {
  return context[SESSION_CONTEXT_KEY] as Session | undefined;
}

function createSessionImpl(id: string, initialData: SessionData, isNew: boolean): Session {
  let destroyed = false;
  let dirty = false;
  let regenerated = false;
  let currentId = id;
  const data: SessionData = { ...initialData };

  return {
    get id() {
      return currentId;
    },
    get isNew() {
      return isNew;
    },
    get isDirty() {
      return dirty;
    },
    get isDestroyed() {
      return destroyed;
    },
    get isRegenerated() {
      return regenerated;
    },
    get<T = unknown>(key: string): T | undefined {
      return data[key] as T | undefined;
    },
    set(key, value) {
      if (destroyed) {
        return;
      }
      data[key] = value;
      dirty = true;
    },
    unset(key) {
      if (destroyed) {
        return;
      }
      if (key in data) {
        delete data[key];
        dirty = true;
      }
    },
    destroy() {
      destroyed = true;
      dirty = false;
    },
    regenerate() {
      if (destroyed) {
        return;
      }
      currentId = createSessionId();
      regenerated = true;
      dirty = true;
    },
    toJSON() {
      return { ...data };
    },
  };
}

function normalizeCookieOptions(options?: SessionCookieOptions): CookieSerializeOptions {
  return {
    path: options?.path ?? "/",
    domain: options?.domain,
    httpOnly: options?.httpOnly ?? true,
    secure: options?.secure,
    sameSite: options?.sameSite ?? "Lax",
    expires: options?.expires,
    maxAge: options?.maxAge,
  };
}

function createSessionId(): string {
  return randomUUID();
}

function resolveCookieOptionsForRequest(
  baseOptions: CookieSerializeOptions,
  request: Request,
  trustedProxies?: string[]
): CookieSerializeOptions {
  if (baseOptions.secure !== undefined) {
    return baseOptions;
  }

  // Default Secure when the request is verifiably HTTPS, OR in production. The
  // production fallback matters because a TLS-terminating proxy that forwards
  // plain HTTP — without `trustedProxies` configured — would otherwise yield a
  // non-Secure cookie now that we no longer trust a spoofable X-Forwarded-Proto.
  // Plain-HTTP production deployments must set `cookie.secure: false` explicitly.
  return {
    ...baseOptions,
    secure:
      isSecureRequest(request, trustedProxies) ||
      process.env.NODE_ENV === "production",
  };
}

/**
 * Determines if the request is over HTTPS
 *
 * SECURITY (TS-32): X-Forwarded-Proto is honored only when the request's
 * NEAREST HOP is a configured trusted proxy, and that hop is identified by
 * the TRANSPORT-INSTALLED socket address — never by X-Real-IP or
 * X-Forwarded-For, both of which a directly connected client can forge.
 * Without transport peer metadata (hand-built Requests, adapters that do
 * not install it), no header is trusted at all.
 *
 * @param request - The incoming request
 * @param trustedProxies - List of trusted proxy IPs/CIDR ranges (optional)
 */
function isSecureRequest(request: Request, trustedProxies?: string[]): boolean {
  const forwardedProto = request.headers.get("x-forwarded-proto");
  if (forwardedProto && trustedProxies && trustedProxies.length > 0) {
    const peer = transportPeer(request);
    if (peer && isTrustedProxy(peer.remoteAddress, trustedProxies)) {
      const first = forwardedProto.split(",")[0]?.trim().toLowerCase();
      if (first === "https") {
        return true;
      }
    }
  }

  try {
    const protocol = new URL(request.url).protocol;
    return protocol === "https:";
  } catch {
    return false;
  }
}

/**
 * Checks if an IP address is in the trusted proxies list
 *
 * SECURITY: Uses proper bit-level CIDR matching for IPv4 addresses.
 */
function isTrustedProxy(ip: string, trustedProxies: string[]): boolean {
  for (const trusted of trustedProxies) {
    // Exact match
    if (ip === trusted) {
      return true;
    }

    // CIDR matching with proper bit-level masking
    if (trusted.includes("/")) {
      if (ipMatchesCidr(ip, trusted)) {
        return true;
      }
    }
  }

  return false;
}

function ipMatchesCidr(ip: string, cidr: string): boolean {
  const [prefix, maskStr] = cidr.split('/');
  // Strict mask syntax (TS-32): the old parseInt accepted "08", "8.5",
  // " 8", and "+8" — permissive parsing silently mis-ranged malformed
  // configurations instead of refusing them.
  if (!/^(0|[1-9][0-9]?)$/.test(maskStr ?? "") || maskStr === undefined) {
    return false;
  }
  const maskBits = parseInt(maskStr, 10);
  if (maskBits > 32) return false;

  const ipNum = ipToNumber(ip);
  const prefixNum = ipToNumber(prefix);
  if (ipNum === null || prefixNum === null) return false;

  // Create a bitmask with `maskBits` leading 1s
  const mask = maskBits === 0 ? 0 : (~0 << (32 - maskBits)) >>> 0;
  return (ipNum & mask) === (prefixNum & mask);
}

function ipToNumber(ip: string): number | null {
  const parts = ip.split('.');
  if (parts.length !== 4) return null;
  let num = 0;
  for (const part of parts) {
    // Reject anything that isn't a canonical 1-3 digit octet. Leading zeros
    // (e.g. "010") are ambiguous and would let "127.000.000.001" diverge from
    // the exact-string trusted-proxy comparison.
    if (!/^\d{1,3}$/.test(part) || (part.length > 1 && part[0] === "0")) {
      return null;
    }
    const n = parseInt(part, 10);
    if (n < 0 || n > 255) return null;
    num = (num << 8) | n;
  }
  return num >>> 0; // Convert to unsigned 32-bit
}
