import type { AppContext, MiddlewareFn } from "@neutron-build/core";

export interface AuthUser {
  id?: string | null;
  email?: string | null;
  name?: string | null;
  image?: string | null;
}

export interface AuthSession<TUser extends AuthUser = AuthUser> {
  user?: TUser | null;
  expiresAt?: string | number | Date;
  [key: string]: unknown;
}

export interface NeutronAuthState<TUser extends AuthUser = AuthUser> {
  adapter: string;
  session: AuthSession<TUser> | null;
  user: TUser | null;
  isAuthenticated: boolean;
}

export interface AuthAdapter<TUser extends AuthUser = AuthUser> {
  name: string;
  getSession(request: Request): Promise<AuthSession<TUser> | null>;
  resolve?(request: Request): Promise<AuthResolution<TUser>>;
}

export interface AuthResolution<TUser extends AuthUser = AuthUser> {
  session: AuthSession<TUser> | null;
  setCookie?: readonly string[];
}

export interface AuthContextMiddlewareOptions<TUser extends AuthUser = AuthUser> {
  adapter: AuthAdapter<TUser>;
  contextKey?: string;
}

export interface ProtectedRouteOptions<TUser extends AuthUser = AuthUser> {
  adapter?: AuthAdapter<TUser>;
  contextKey?: string;
  redirectTo?: string;
  unauthorizedStatus?: number;
}

const DEFAULT_AUTH_CONTEXT_KEY = "auth";

export function createAuthContextMiddleware<TUser extends AuthUser = AuthUser>(
  options: AuthContextMiddlewareOptions<TUser>
): MiddlewareFn {
  const contextKey = options.contextKey || DEFAULT_AUTH_CONTEXT_KEY;
  const adapter = options.adapter;

  return async (request, context, next) => {
    const resolution = await resolveAdapter(adapter, request);
    context[contextKey] = createAuthState(adapter.name, resolution.session);

    return appendSetCookies(await next(), resolution.setCookie);
  };
}

export function createProtectedRouteMiddleware<TUser extends AuthUser = AuthUser>(
  options: ProtectedRouteOptions<TUser> = {}
): MiddlewareFn {
  const contextKey = options.contextKey || DEFAULT_AUTH_CONTEXT_KEY;
  const redirectTo = options.redirectTo;
  const unauthorizedStatus = options.unauthorizedStatus ?? 401;

  return async (request, context, next) => {
    const existing = getAuthFromContext<TUser>(context, contextKey);
    let authState = existing;
    let setCookie: readonly string[] | undefined;
    if (!authState && options.adapter) {
      const resolution = await resolveAdapter(options.adapter, request);
      authState = createAuthState(options.adapter.name, resolution.session);
      setCookie = resolution.setCookie;
      context[contextKey] = authState;
    }

    if (!authState?.isAuthenticated) {
      if (redirectTo) {
        return appendSetCookies(new Response(null, {
          status: 302,
          headers: { Location: safeRedirectLocation(redirectTo, request) },
        }), setCookie);
      }
      return appendSetCookies(new Response("Unauthorized", { status: unauthorizedStatus }), setCookie);
    }

    return appendSetCookies(await next(), setCookie);
  };
}

export function getAuthFromContext<TUser extends AuthUser = AuthUser>(
  context: AppContext,
  contextKey: string = DEFAULT_AUTH_CONTEXT_KEY
): NeutronAuthState<TUser> | null {
  const value = context[contextKey];
  if (!value || typeof value !== "object") {
    return null;
  }
  const candidate = value as Partial<NeutronAuthState<TUser>>;
  if (typeof candidate.adapter !== "string") {
    return null;
  }
  if (candidate.session !== null && (!candidate.session || typeof candidate.session !== "object")) {
    return null;
  }
  return createAuthState(candidate.adapter, candidate.session ?? null);
}

export function requireAuth<TUser extends AuthUser = AuthUser>(
  context: AppContext,
  contextKey: string = DEFAULT_AUTH_CONTEXT_KEY
): NeutronAuthState<TUser> {
  const auth = getAuthFromContext<TUser>(context, contextKey);
  if (!auth?.isAuthenticated) {
    throw new Response("Unauthorized", { status: 401 });
  }
  return auth;
}

export interface BetterAuthAdapterOptions<TUser extends AuthUser = AuthUser> {
  auth: BetterAuthLike;
  name?: string;
  basePath?: string;
  disableCookieCache?: boolean;
  disableRefresh?: boolean;
  includeSessionToken?: boolean;
  mapSession?: (value: unknown) => AuthSession<TUser> | null;
}

interface BetterAuthLike {
  api?: {
    getSession?: (args: {
      headers: Headers;
      query?: { disableCookieCache?: boolean; disableRefresh?: boolean };
      returnHeaders: true;
    }) => Promise<unknown>;
  };
  handler?: (request: Request) => Promise<Response>;
  /** @deprecated Better Auth exposes api.getSession; retained for 0.1 compatibility. */
  getSession?: (request: Request) => Promise<unknown>;
}

export function createBetterAuthAdapter<TUser extends AuthUser = AuthUser>(
  options: BetterAuthAdapterOptions<TUser>
): AuthAdapter<TUser> {
  async function resolve(request: Request): Promise<AuthResolution<TUser>> {
    if (options.auth.api?.getSession) {
      let raw = await options.auth.api.getSession({
        headers: request.headers,
        query: {
          ...(options.disableCookieCache === undefined ? {} : { disableCookieCache: options.disableCookieCache }),
          ...(options.disableRefresh === undefined ? {} : { disableRefresh: options.disableRefresh }),
        },
        returnHeaders: true,
      });
      if (needsDeferredRefresh(raw) && !options.disableRefresh) {
        raw = await refreshBetterAuthSession(options.auth, request, options.basePath || "/api/auth");
      }
      return normalizeBetterAuthResolution(raw, options);
    }
    if (options.auth.getSession) {
      return { session: normalizeLegacySession<TUser>(await options.auth.getSession(request)) };
    }
    return { session: null };
  }

  return {
    name: options.name || "better-auth",
    resolve,
    async getSession(request) {
      return (await resolve(request)).session;
    },
  };
}

export interface AuthJsResolution {
  session: unknown;
  setCookie?: readonly string[];
}

type AuthJsLegacyLike =
  | ((request: Request) => Promise<unknown>)
  | {
      auth?: (request: Request) => Promise<unknown>;
      getSession?: (request: Request) => Promise<unknown>;
    };

export type AuthJsAdapterOptions<TUser extends AuthUser = AuthUser> = {
  name?: string;
} & (
  | { resolveSession: (request: Request) => Promise<AuthJsResolution>; getSession?: never; auth?: never }
  | { getSession: (request: Request) => Promise<unknown>; resolveSession?: never; auth?: never }
  | { auth: AuthJsLegacyLike; resolveSession?: never; getSession?: never }
);

/**
 * Adapts an application-supplied Auth.js session resolver. Auth.js does not
 * expose a framework-neutral `auth(request)` session API, so pass a resolver
 * that returns its direct `{ user, expires }` session shape.
 */
export function createAuthJsAdapter<TUser extends AuthUser = AuthUser>(
  options: AuthJsAdapterOptions<TUser>
): AuthAdapter<TUser> {
  async function resolve(request: Request): Promise<AuthResolution<TUser>> {
    if ("resolveSession" in options && options.resolveSession) {
      const result = await options.resolveSession(request);
      return {
        session: normalizeAuthJsSession<TUser>(result.session),
        setCookie: result.setCookie,
      };
    }
    if ("getSession" in options && options.getSession) {
      return { session: normalizeAuthJsSession<TUser>(await options.getSession(request)) };
    }
    const legacy = options.auth;
    let value: unknown = null;
    if (typeof legacy === "function") value = await legacy(request);
    else if (legacy.auth) value = await legacy.auth(request);
    else if (legacy.getSession) value = await legacy.getSession(request);
    return { session: normalizeAuthJsSession<TUser>(value) };
  }

  return {
    name: options.name || "authjs",
    resolve,
    async getSession(request) {
      return (await resolve(request)).session;
    },
  };
}

export async function authJsResolutionFromResponse(response: Response): Promise<AuthJsResolution> {
  let session: unknown = null;
  if (response.ok) {
    try {
      session = await response.json();
    } catch {
      session = null;
    }
  }
  return { session, setCookie: getSetCookies(response.headers) };
}

function normalizeBetterAuthResolution<TUser extends AuthUser>(
  value: unknown,
  options: BetterAuthAdapterOptions<TUser>
): AuthResolution<TUser> {
  if (!value || typeof value !== "object" || value instanceof Response) {
    return { session: null };
  }
  const envelope = value as Record<string, unknown>;
  const setCookie = envelope.headers instanceof Headers
    ? getSetCookies(envelope.headers)
    : undefined;
  const response = envelope.response;

  if (options.mapSession) {
    return { session: validateSession(options.mapSession(response)), setCookie };
  }
  if (!response || typeof response !== "object" || response instanceof Response) {
    return { session: null, setCookie };
  }

  const payload = response as Record<string, unknown>;
  if (!isRecord(payload.session) || !isRecord(payload.user)) {
    return { session: null, setCookie };
  }

  const session = { ...payload.session } as AuthSession<TUser>;
  if (!options.includeSessionToken) {
    delete session.token;
  }
  session.user = payload.user as TUser;
  return { session: validateSession(session), setCookie };
}

function normalizeAuthJsSession<TUser extends AuthUser>(value: unknown): AuthSession<TUser> | null {
  if (!isRecord(value) || value instanceof Response || !isRecord(value.user)) {
    return null;
  }

  const session = { ...value, user: value.user as TUser } as AuthSession<TUser>;
  if (session.expiresAt === undefined && typeof value.expires === "string") {
    session.expiresAt = value.expires;
  }
  return validateSession(session);
}

function normalizeLegacySession<TUser extends AuthUser>(value: unknown): AuthSession<TUser> | null {
  if (!isRecord(value) || value instanceof Response) return null;
  const nested = isRecord(value.session) ? value.session : value;
  return validateSession(nested as AuthSession<TUser>);
}

function needsDeferredRefresh(value: unknown): boolean {
  if (!isRecord(value) || !isRecord(value.response)) return false;
  return value.response.needsRefresh === true;
}

async function refreshBetterAuthSession(
  auth: BetterAuthLike,
  request: Request,
  basePath: string
): Promise<unknown> {
  if (!auth.handler) {
    throw new Error("Better Auth deferred session refresh requires auth.handler");
  }
  const url = new URL(request.url);
  url.pathname = `${basePath.replace(/\/$/, "")}/get-session`;
  url.search = "";
  const headers = new Headers(request.headers);
  if (!headers.has("Origin")) headers.set("Origin", url.origin);
  const response = await auth.handler(new Request(url, {
    method: "POST",
    headers,
  }));
  let body: unknown = null;
  if (response.ok) {
    try {
      body = await response.json();
    } catch {
      body = null;
    }
  }
  return { headers: response.headers, response: body };
}

function validateSession<TUser extends AuthUser>(session: AuthSession<TUser> | null): AuthSession<TUser> | null {
  if (!session || !isRecord(session.user) || isSessionExpired(session)) {
    return null;
  }
  return session;
}

function resolveUserFromSession<TUser extends AuthUser = AuthUser>(
  session: AuthSession<TUser> | null
): TUser | null {
  if (!session || typeof session !== "object") {
    return null;
  }
  // Enforce expiry: an expired session is not authenticated even if an adapter
  // still returns a user object attached to it.
  if (isSessionExpired(session)) {
    return null;
  }
  const user = session.user;
  if (!user || typeof user !== "object") {
    return null;
  }
  return user as TUser;
}

function isSessionExpired(session: AuthSession): boolean {
  const exp = session.expiresAt;
  if (exp === undefined || exp === null) {
    return false;
  }
  let ms: number;
  if (exp instanceof Date) {
    ms = exp.getTime();
  } else if (typeof exp === "number") {
    if (!Number.isFinite(exp)) return true;
    // Accept both seconds- and milliseconds-epoch values.
    ms = exp < 1e12 ? exp * 1000 : exp;
  } else {
    ms = Date.parse(String(exp));
  }
  if (Number.isNaN(ms)) {
    return true;
  }
  return ms <= Date.now();
}

function createAuthState<TUser extends AuthUser>(
  adapter: string,
  session: AuthSession<TUser> | null
): NeutronAuthState<TUser> {
  const validSession = validateSession(session);
  const user = resolveUserFromSession(validSession);
  return {
    adapter,
    session: validSession,
    user,
    isAuthenticated: Boolean(user),
  };
}

async function resolveAdapter<TUser extends AuthUser>(
  adapter: AuthAdapter<TUser>,
  request: Request
): Promise<AuthResolution<TUser>> {
  if (adapter.resolve) {
    const resolution = await adapter.resolve(request);
    return {
      session: validateSession(resolution.session),
      setCookie: resolution.setCookie?.filter((cookie): cookie is string => typeof cookie === "string"),
    };
  }
  return { session: validateSession(await adapter.getSession(request)) };
}

function appendSetCookies(response: Response, cookies?: readonly string[]): Response {
  if (!cookies?.length) return response;
  const headers = new Headers(response.headers);
  for (const cookie of cookies) headers.append("Set-Cookie", cookie);
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

function getSetCookies(headers: Headers): string[] | undefined {
  const cookieHeaders = headers as Headers & { getSetCookie?: () => string[] };
  const cookies = cookieHeaders.getSetCookie?.() ?? [];
  if (cookies.length) return cookies;
  const combined = headers.get("set-cookie");
  return combined ? splitSetCookieHeader(combined) : undefined;
}

function splitSetCookieHeader(value: string): string[] {
  const cookies: string[] = [];
  let start = 0;
  let position = 0;

  while (position < value.length) {
    const comma = value.indexOf(",", position);
    if (comma === -1) break;
    const nextEquals = value.indexOf("=", comma + 1);
    const nextSemicolon = value.indexOf(";", comma + 1);
    const nextComma = value.indexOf(",", comma + 1);
    if (nextEquals !== -1 && (nextSemicolon === -1 || nextEquals < nextSemicolon) && (nextComma === -1 || nextEquals < nextComma)) {
      cookies.push(value.slice(start, comma).trim());
      start = comma + 1;
    }
    position = comma + 1;
  }

  const last = value.slice(start).trim();
  if (last) cookies.push(last);
  return cookies;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

/**
 * Resolve a post-auth redirect target to a safe, same-origin Location. Relative
 * paths are allowed as-is; absolute URLs are permitted only when same-origin;
 * protocol-relative (`//host`), backslash, and cross-origin targets fall back
 * to "/" — preventing an open redirect if `redirectTo` is ever derived from
 * request input.
 */
function safeRedirectLocation(target: string, request: Request): string {
  // Browsers strip TAB/LF/CR from URLs, so "/\t/evil" would resolve to
  // "//evil" client-side — remove them before the same-origin checks.
  const cleaned = target.replace(/[\t\n\r]/g, "");
  if (cleaned.startsWith("/") && !cleaned.startsWith("//") && !cleaned.startsWith("/\\")) {
    return cleaned;
  }
  try {
    const url = new URL(cleaned, request.url);
    if (url.origin === new URL(request.url).origin) {
      return url.pathname + url.search + url.hash;
    }
  } catch {
    // fall through to safe default
  }
  return "/";
}
