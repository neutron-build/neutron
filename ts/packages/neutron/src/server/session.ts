import { randomUUID } from "node:crypto";
import {
  getCookie,
  serializeCookie,
  type CookieSerializeOptions,
} from "../core/cookies.js";
import type { AppContext, MiddlewareFn } from "../core/types.js";

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
   * List of trusted proxy IP addresses or CIDR ranges.
   * Only requests from these IPs will be trusted for X-Forwarded-Proto header.
   *
   * SECURITY: If not specified, X-Forwarded-Proto will be trusted from any source,
   * which may allow attackers to bypass secure cookie settings. Always configure
   * this in production when behind a proxy.
   *
   * @example
   * ```ts
   * trustedProxies: ['127.0.0.1', '::1', '10.0.0.0/8']
   * ```
   */
  trustedProxies?: string[];
}

const SESSION_CONTEXT_KEY = "session";

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

    // If still over limit, evict oldest entries (LRU approximation)
    if (map.size >= maxSessions) {
      const entries = Array.from(map.entries());
      const toDelete = entries.slice(0, Math.floor(maxSessions * 0.1));
      for (const [key] of toDelete) {
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

      return {
        data: { ...record.data },
        expiresAt: record.expiresAt,
      };
    },

    async setSession(sessionId, data, expiresAt) {
      const ttlExpiry =
        defaultTtlMs && !expiresAt ? Date.now() + defaultTtlMs : expiresAt;
      map.set(sessionId, {
        data: { ...data },
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
      response.headers.append(
        "Set-Cookie",
        serializeCookie(cookieName, "", {
          ...cookieOptions,
          maxAge: 0,
          expires: new Date(0),
        })
      );
      return response;
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
      response.headers.append(
        "Set-Cookie",
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

  return {
    ...baseOptions,
    secure: isSecureRequest(request, trustedProxies),
  };
}

/**
 * Determines if the request is over HTTPS
 *
 * SECURITY: Only trusts X-Forwarded-Proto if request comes from a trusted proxy IP.
 * This prevents attackers from spoofing the header to bypass secure cookie settings.
 *
 * @param request - The incoming request
 * @param trustedProxies - List of trusted proxy IPs/CIDR ranges (optional)
 */
function isSecureRequest(request: Request, trustedProxies?: string[]): boolean {
  const forwardedProto = request.headers.get("x-forwarded-proto");
  if (forwardedProto) {
    // Only trust X-Forwarded-Proto if request is from a trusted proxy
    if (trustedProxies && trustedProxies.length > 0) {
      const clientIp = getClientIp(request);
      if (clientIp && isTrustedProxy(clientIp, trustedProxies)) {
        const first = forwardedProto.split(",")[0]?.trim().toLowerCase();
        if (first === "https") {
          return true;
        }
      }
    } else {
      // SECURITY WARNING: If trustedProxies is not configured, we trust any
      // X-Forwarded-Proto header. This maintains backward compatibility but
      // is less secure. Users should configure trustedProxies in production.
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
 * Extracts client IP from request headers
 */
function getClientIp(request: Request): string | null {
  // Try X-Forwarded-For first (most common)
  const xForwardedFor = request.headers.get("x-forwarded-for");
  if (xForwardedFor) {
    const ips = xForwardedFor.split(",").map((ip) => ip.trim());
    if (ips.length > 0 && ips[0]) {
      return ips[0];
    }
  }

  // Try X-Real-IP
  const xRealIp = request.headers.get("x-real-ip");
  if (xRealIp) {
    return xRealIp.trim();
  }

  return null;
}

/**
 * Checks if an IP address is in the trusted proxies list
 *
 * SECURITY: Simple prefix matching for CIDR ranges and exact matching for IPs.
 * This is a basic implementation - production systems may want more robust CIDR parsing.
 */
function isTrustedProxy(ip: string, trustedProxies: string[]): boolean {
  for (const trusted of trustedProxies) {
    // Exact match
    if (ip === trusted) {
      return true;
    }

    // Simple CIDR prefix matching (e.g., "10.0.0.0/8" matches "10.x.x.x")
    if (trusted.includes("/")) {
      const [prefix, bits] = trusted.split("/");
      if (prefix && bits) {
        const prefixParts = prefix.split(".");
        const ipParts = ip.split(".");
        const maskBits = parseInt(bits, 10);

        // Simple IPv4 CIDR check
        if (prefixParts.length === 4 && ipParts.length === 4 && !isNaN(maskBits)) {
          const octetsToMatch = Math.floor(maskBits / 8);
          let matches = true;
          for (let i = 0; i < octetsToMatch; i++) {
            if (prefixParts[i] !== ipParts[i]) {
              matches = false;
              break;
            }
          }
          if (matches) {
            return true;
          }
        }
      }
    }
  }

  return false;
}
