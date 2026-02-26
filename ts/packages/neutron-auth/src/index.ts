import type { AppContext, MiddlewareFn } from "neutron";

export interface AuthUser {
  id?: string;
  email?: string;
  name?: string;
  [key: string]: unknown;
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
    const session = await adapter.getSession(request);
    const user = resolveUserFromSession(session);
    context[contextKey] = {
      adapter: adapter.name,
      session,
      user,
      isAuthenticated: Boolean(user),
    } satisfies NeutronAuthState<TUser>;

    return await next();
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
    if (!authState && options.adapter) {
      const session = await options.adapter.getSession(request);
      const user = resolveUserFromSession(session);
      authState = {
        adapter: options.adapter.name,
        session,
        user,
        isAuthenticated: Boolean(user),
      };
      context[contextKey] = authState;
    }

    if (!authState?.isAuthenticated) {
      if (redirectTo) {
        return Response.redirect(redirectTo, 302);
      }
      return new Response("Unauthorized", { status: unauthorizedStatus });
    }

    return await next();
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
  return value as NeutronAuthState<TUser>;
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
}

interface BetterAuthLike {
  api?: {
    getSession?: (args: { headers: Headers; request: Request }) => Promise<unknown>;
  };
  getSession?: (request: Request) => Promise<unknown>;
}

export function createBetterAuthAdapter<TUser extends AuthUser = AuthUser>(
  options: BetterAuthAdapterOptions<TUser>
): AuthAdapter<TUser> {
  return {
    name: options.name || "better-auth",
    async getSession(request) {
      let raw: unknown = null;
      if (options.auth.api?.getSession) {
        raw = await options.auth.api.getSession({ headers: request.headers, request });
      } else if (options.auth.getSession) {
        raw = await options.auth.getSession(request);
      }
      return normalizeSession<TUser>(raw);
    },
  };
}

export interface AuthJsAdapterOptions<TUser extends AuthUser = AuthUser> {
  auth: AuthJsLike;
  name?: string;
}

type AuthJsLike =
  | ((request: Request) => Promise<unknown>)
  | {
      auth?: (request: Request) => Promise<unknown>;
      getSession?: (request: Request) => Promise<unknown>;
    };

export function createAuthJsAdapter<TUser extends AuthUser = AuthUser>(
  options: AuthJsAdapterOptions<TUser>
): AuthAdapter<TUser> {
  return {
    name: options.name || "authjs",
    async getSession(request) {
      let raw: unknown = null;
      if (typeof options.auth === "function") {
        raw = await options.auth(request);
      } else if (options.auth.auth) {
        raw = await options.auth.auth(request);
      } else if (options.auth.getSession) {
        raw = await options.auth.getSession(request);
      }
      return normalizeSession<TUser>(raw);
    },
  };
}

function normalizeSession<TUser extends AuthUser = AuthUser>(
  value: unknown
): AuthSession<TUser> | null {
  if (!value || typeof value !== "object") {
    return null;
  }

  const candidate = value as Record<string, unknown>;
  if ("session" in candidate && candidate.session && typeof candidate.session === "object") {
    return candidate.session as AuthSession<TUser>;
  }

  // Auth.js commonly returns the session object directly.
  return candidate as AuthSession<TUser>;
}

function resolveUserFromSession<TUser extends AuthUser = AuthUser>(
  session: AuthSession<TUser> | null
): TUser | null {
  if (!session || typeof session !== "object") {
    return null;
  }
  const user = session.user;
  if (!user || typeof user !== "object") {
    return null;
  }
  return user as TUser;
}
