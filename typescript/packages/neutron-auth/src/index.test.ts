import assert from "node:assert/strict";
import { describe, it, mock } from "node:test";
import { betterAuth } from "better-auth";
import { memoryAdapter } from "better-auth/adapters/memory";
import {
  authJsResolutionFromResponse,
  createAuthContextMiddleware,
  createAuthJsAdapter,
  createBetterAuthAdapter,
  createProtectedRouteMiddleware,
  getAuthFromContext,
  requireAuth,
  type AuthAdapter,
  type AuthSession,
  type NeutronAuthState,
} from "./index.js";

function fakeAdapter(session: AuthSession | null, name = "test-adapter"): AuthAdapter {
  return {
    name,
    async getSession() {
      return session;
    },
  };
}

const ok = () => new Response("ok", { status: 200 });

type BetterAuthGetSessionArgs = {
  headers: Headers;
  query?: { disableCookieCache?: boolean; disableRefresh?: boolean };
  returnHeaders: true;
};

// Compile-time contract check against the installed Better Auth server type.
function acceptBetterAuth(auth: ReturnType<typeof betterAuth>) {
  return createBetterAuthAdapter({ auth });
}
void acceptBetterAuth;

describe("auth context", () => {
  it("populates authenticated state from a legacy custom adapter", async () => {
    const session: AuthSession = { user: { id: "u1", email: "a@b.com" } };
    const context: Record<string, unknown> = {};
    const middleware = createAuthContextMiddleware({ adapter: fakeAdapter(session) });
    const response = await middleware(new Request("https://example.com"), context, async () => ok());

    assert.equal(response.status, 200);
    const auth = context.auth as NeutronAuthState;
    assert.equal(auth.adapter, "test-adapter");
    assert.equal(auth.isAuthenticated, true);
    assert.deepEqual(auth.user, session.user);
    assert.equal(auth.session, session);
  });

  it("supports a custom context key", async () => {
    const context: Record<string, unknown> = {};
    const middleware = createAuthContextMiddleware({
      adapter: fakeAdapter({ user: { id: "u1" } }),
      contextKey: "identity",
    });
    await middleware(new Request("https://example.com"), context, async () => ok());
    assert.equal(context.auth, undefined);
    assert.equal((context.identity as NeutronAuthState).isAuthenticated, true);
  });

  it("rejects expired and malformed expiry values", async () => {
    for (const expiresAt of ["2000-01-01T00:00:00Z", "not-a-date", Number.NaN, Infinity]) {
      const context: Record<string, unknown> = {};
      const middleware = createAuthContextMiddleware({
        adapter: fakeAdapter({ user: { id: "u1" }, expiresAt }),
      });
      await middleware(new Request("https://example.com"), context, async () => ok());
      assert.equal((context.auth as NeutronAuthState).isAuthenticated, false);
    }
  });

  it("accepts future second, millisecond, string, and Date expiries", async () => {
    const future = Date.now() + 60_000;
    for (const expiresAt of [future, future / 1000, new Date(future).toISOString(), new Date(future)]) {
      const context: Record<string, unknown> = {};
      const middleware = createAuthContextMiddleware({
        adapter: fakeAdapter({ user: { id: "u1" }, expiresAt }),
      });
      await middleware(new Request("https://example.com"), context, async () => ok());
      assert.equal((context.auth as NeutronAuthState).isAuthenticated, true);
    }
  });
});

describe("protected routes", () => {
  it("returns 401 without authenticated context", async () => {
    const middleware = createProtectedRouteMiddleware();
    const response = await middleware(
      new Request("https://example.com/private"),
      {},
      async () => ok()
    );
    assert.equal(response.status, 401);
    assert.equal(await response.text(), "Unauthorized");
  });

  it("resolves an adapter and allows an authenticated request", async () => {
    const context: Record<string, unknown> = {};
    const middleware = createProtectedRouteMiddleware({
      adapter: fakeAdapter({ user: { id: "u1" } }),
    });
    const response = await middleware(
      new Request("https://example.com/private"),
      context,
      async () => new Response("allowed", { status: 201 })
    );
    assert.equal(response.status, 201);
    assert.equal((context.auth as NeutronAuthState).isAuthenticated, true);
  });

  it("uses a configured denial status", async () => {
    const middleware = createProtectedRouteMiddleware({ unauthorizedStatus: 403 });
    const response = await middleware(new Request("https://example.com/private"), {}, async () => ok());
    assert.equal(response.status, 403);
  });

  it("allows relative and same-origin redirects", async () => {
    for (const redirectTo of ["/login", "https://example.com/login?next=%2Fprivate"]) {
      const middleware = createProtectedRouteMiddleware({ adapter: fakeAdapter(null), redirectTo });
      const response = await middleware(
        new Request("https://example.com/private"),
        {},
        async () => ok()
      );
      assert.equal(response.status, 302);
      assert.match(response.headers.get("location") || "", /^\/login/);
    }
  });

  it("rejects cross-origin, protocol-relative, and backslash redirects", async () => {
    for (const redirectTo of ["https://evil.example/phish", "//evil.example/phish", "/\\evil.example"]) {
      const middleware = createProtectedRouteMiddleware({ adapter: fakeAdapter(null), redirectTo });
      const response = await middleware(
        new Request("https://example.com/private"),
        {},
        async () => ok()
      );
      assert.equal(response.headers.get("location"), "/");
    }
  });
});

describe("context access", () => {
  it("returns null for absent, non-object, and structurally invalid state", () => {
    assert.equal(getAuthFromContext({}), null);
    assert.equal(getAuthFromContext({ auth: "invalid" }), null);
    assert.equal(getAuthFromContext({ auth: { isAuthenticated: true } }), null);
  });

  it("derives state instead of trusting stored user or authentication flags", () => {
    const state = getAuthFromContext({
      auth: {
        adapter: "forged",
        session: null,
        user: { id: "forged" },
        isAuthenticated: true,
      },
    });
    assert.equal(state?.isAuthenticated, false);
    assert.equal(state?.user, null);
  });

  it("uses custom keys and returns valid state", () => {
    const session = { user: { id: "u1" } };
    const state = getAuthFromContext({ identity: { adapter: "test", session } }, "identity");
    assert.equal(state?.isAuthenticated, true);
    assert.deepEqual(state?.user, { id: "u1" });
  });

  it("requireAuth rejects missing state and returns valid state", () => {
    assert.throws(() => requireAuth({}), (error) => error instanceof Response && error.status === 401);
    const state = requireAuth({ auth: { adapter: "test", session: { user: { id: "u1" } } } });
    assert.equal(state.user?.id, "u1");
  });
});

describe("createBetterAuthAdapter", () => {
  it("normalizes Better Auth's documented sibling shape and strips its token", async () => {
    const expiresAt = new Date(Date.now() + 60_000);
    const getSession = mock.fn(async (_args: BetterAuthGetSessionArgs) => ({
      headers: new Headers(),
      response: {
        session: { id: "s1", userId: "ba1", token: "secret", expiresAt },
        user: { id: "ba1", email: "better@example.com" },
      },
    }));
    const adapter = createBetterAuthAdapter({ auth: { api: { getSession } } });
    const session = await adapter.getSession(new Request("https://example.com"));

    assert.deepEqual(session, {
      id: "s1",
      userId: "ba1",
      expiresAt,
      user: { id: "ba1", email: "better@example.com" },
    });
    assert.equal(getSession.mock.calls[0]!.arguments[0].returnHeaders, true);
  });

  it("passes cache options and can retain the token explicitly", async () => {
    const getSession = mock.fn(async (_args: BetterAuthGetSessionArgs) => ({
      headers: new Headers(),
      response: {
        session: { token: "secret", expiresAt: new Date(Date.now() + 60_000) },
        user: { id: "ba2" },
      },
    }));
    const adapter = createBetterAuthAdapter({
      auth: { api: { getSession } },
      disableCookieCache: true,
      disableRefresh: true,
      includeSessionToken: true,
    });
    const session = await adapter.getSession(new Request("https://example.com"));
    assert.equal(session?.token, "secret");
    assert.deepEqual(getSession.mock.calls[0]!.arguments[0].query, {
      disableCookieCache: true,
      disableRefresh: true,
    });
  });

  it("returns null for null and malformed provider payloads", async () => {
    for (const response of [null, {}, { session: {} }, { user: { id: "u1" } }]) {
      const adapter = createBetterAuthAdapter({
        auth: { api: { getSession: async () => ({ headers: new Headers(), response }) } },
      });
      assert.equal(await adapter.getSession(new Request("https://example.com")), null);
    }
  });

  it("supports custom-session output through an explicit mapper", async () => {
    const adapter = createBetterAuthAdapter({
      auth: {
        api: {
          getSession: async () => ({
            headers: new Headers(),
            response: { identity: { id: "custom" }, validUntil: "2099-01-01" },
          }),
        },
      },
      mapSession(value) {
        const custom = value as { identity: { id: string }; validUntil: string };
        return { user: custom.identity, expiresAt: custom.validUntil };
      },
    });
    assert.equal((await adapter.getSession(new Request("https://example.com")))?.user?.id, "custom");
  });

  it("performs Better Auth's deferred POST refresh when requested", async () => {
    const handler = mock.fn(async (_request: Request) => Response.json({
      session: { expiresAt: new Date(Date.now() + 120_000).toISOString() },
      user: { id: "refreshed" },
    }, {
      headers: { "Set-Cookie": "session=new; Path=/; HttpOnly" },
    }));
    const adapter = createBetterAuthAdapter({
      auth: {
        api: {
          getSession: async () => ({
            headers: new Headers(),
            response: {
              session: { expiresAt: new Date(Date.now() + 10_000) },
              user: { id: "old" },
              needsRefresh: true,
            },
          }),
        },
        handler,
      },
      basePath: "/custom-auth",
    });
    const middleware = createAuthContextMiddleware({ adapter });
    const context: Record<string, unknown> = {};
    const response = await middleware(
      new Request("https://example.com/private", { headers: { Cookie: "session=old" } }),
      context,
      async () => ok()
    );
    const refreshRequest = handler.mock.calls[0]!.arguments[0];
    assert.equal(refreshRequest.method, "POST");
    assert.equal(new URL(refreshRequest.url).pathname, "/custom-auth/get-session");
    assert.equal(refreshRequest.headers.get("origin"), "https://example.com");
    assert.equal((context.auth as NeutronAuthState).user?.id, "refreshed");
    assert.match(response.headers.get("set-cookie") || "", /session=new/);
  });

  it("retains the deprecated top-level resolver without claiming it is Better Auth", async () => {
    const adapter = createBetterAuthAdapter({
      auth: { getSession: async () => ({ session: { user: { id: "legacy" } } }) },
    });
    assert.equal((await adapter.getSession(new Request("https://example.com")))?.user?.id, "legacy");
  });

  it("forwards refresh cookies on success", async () => {
    const headers = new Headers();
    headers.append("Set-Cookie", "session=refreshed; Path=/; HttpOnly");
    const adapter = createBetterAuthAdapter({
      auth: {
        api: {
          getSession: async () => ({
            headers,
            response: {
              session: { expiresAt: new Date(Date.now() + 60_000) },
              user: { id: "ba3" },
            },
          }),
        },
      },
    });
    const middleware = createAuthContextMiddleware({ adapter });
    const response = await middleware(new Request("https://example.com"), {}, async () => ok());
    assert.match(response.headers.get("set-cookie") || "", /session=refreshed/);
  });

  it("forwards cleanup cookies on denial", async () => {
    const headers = new Headers();
    headers.append("Set-Cookie", "session=; Path=/; Max-Age=0");
    const adapter = createBetterAuthAdapter({
      auth: { api: { getSession: async () => ({ headers, response: null }) } },
    });
    const middleware = createProtectedRouteMiddleware({ adapter, redirectTo: "/login" });
    const response = await middleware(
      new Request("https://example.com/private"),
      {},
      async () => ok()
    );
    assert.equal(response.status, 302);
    assert.match(response.headers.get("set-cookie") || "", /Max-Age=0/);
  });

  it("authenticates through a real Better Auth instance", async () => {
    const database = { user: [], session: [], account: [], verification: [] };
    const auth = betterAuth({
      database: memoryAdapter(database),
      baseURL: "https://example.com",
      secret: "test-secret-that-is-at-least-thirty-two-characters",
      emailAndPassword: { enabled: true },
    });
    const signUp = await auth.handler(new Request("https://example.com/api/auth/sign-up/email", {
      method: "POST",
      headers: { "Content-Type": "application/json", Origin: "https://example.com" },
      body: JSON.stringify({
        name: "Real User",
        email: "real@example.com",
        password: "long-enough-test-password",
      }),
    }));
    assert.equal(signUp.status, 200);
    const setCookies = signUp.headers.getSetCookie();
    const cookie = setCookies.map((value) => value.split(";", 1)[0]).join("; ");
    const adapter = createBetterAuthAdapter({ auth });
    const session = await adapter.getSession(new Request("https://example.com/studio", {
      headers: { Cookie: cookie },
    }));
    assert.equal(session?.user?.email, "real@example.com");
    assert.equal(session?.token, undefined);
  });

  it("refreshes a real deferred Better Auth session from a navigation request", async () => {
    const database = { user: [], session: [], account: [], verification: [] };
    const auth = betterAuth({
      database: memoryAdapter(database),
      baseURL: "https://example.com",
      secret: "test-secret-that-is-at-least-thirty-two-characters",
      emailAndPassword: { enabled: true },
      session: { expiresIn: 3600, updateAge: 0, deferSessionRefresh: true },
    });
    const signUp = await auth.handler(new Request("https://example.com/api/auth/sign-up/email", {
      method: "POST",
      headers: { "Content-Type": "application/json", Origin: "https://example.com" },
      body: JSON.stringify({
        name: "Deferred User",
        email: "deferred@example.com",
        password: "long-enough-test-password",
      }),
    }));
    const cookie = signUp.headers.getSetCookie()
      .map((value) => value.split(";", 1)[0])
      .join("; ");
    const adapter = createBetterAuthAdapter({ auth });
    const context: Record<string, unknown> = {};
    const middleware = createAuthContextMiddleware({ adapter });
    const response = await middleware(new Request("https://example.com/studio", {
      headers: { Cookie: cookie },
    }), context, async () => ok());

    assert.equal((context.auth as NeutronAuthState).user?.email, "deferred@example.com");
    assert.ok(response.headers.getSetCookie().length > 0);
  });
});

describe("createAuthJsAdapter", () => {
  it("normalizes an application-supplied direct Auth.js session", async () => {
    const getSession = mock.fn(async () => ({
      user: { id: "aj1", email: "aj@example.com" },
      expires: "2099-01-01T00:00:00.000Z",
    }));
    const adapter = createAuthJsAdapter({ getSession });
    const session = await adapter.getSession(new Request("https://example.com"));
    assert.deepEqual(session, {
      user: { id: "aj1", email: "aj@example.com" },
      expires: "2099-01-01T00:00:00.000Z",
      expiresAt: "2099-01-01T00:00:00.000Z",
    });
    assert.equal(getSession.mock.calls.length, 1);
  });

  it("rejects null, malformed data, and Response objects", async () => {
    for (const value of [null, "invalid", {}, new Response(null)]) {
      const adapter = createAuthJsAdapter({ getSession: async () => value });
      assert.equal(await adapter.getSession(new Request("https://example.com")), null);
    }
  });

  it("does not unwrap an Auth.js custom field named session", async () => {
    const adapter = createAuthJsAdapter({
      getSession: async () => ({
        user: { id: "aj2" },
        expires: "2099-01-01T00:00:00.000Z",
        session: { custom: true },
      }),
    });
    const session = await adapter.getSession(new Request("https://example.com"));
    assert.deepEqual(session?.session, { custom: true });
    assert.equal(session?.user?.id, "aj2");
  });

  it("forwards cookies from a response-aware Auth.js resolver", async () => {
    const adapter = createAuthJsAdapter({
      resolveSession: async () => ({
        session: { user: { id: "aj3" }, expires: "2099-01-01T00:00:00.000Z" },
        setCookie: ["authjs=rotated; Path=/; HttpOnly"],
      }),
    });
    const middleware = createAuthContextMiddleware({ adapter });
    const response = await middleware(new Request("https://example.com"), {}, async () => ok());
    assert.match(response.headers.get("set-cookie") || "", /authjs=rotated/);
  });

  it("extracts multiple cookies from an Auth.js core response", async () => {
    const response = Response.json({
      user: { id: "aj4", email: null, name: null },
      expires: "2099-01-01T00:00:00.000Z",
    });
    response.headers.set(
      "Set-Cookie",
      "one=1; Expires=Wed, 21 Oct 2099 07:28:00 GMT; Path=/, two=2; Path=/; HttpOnly"
    );
    Object.defineProperty(response.headers, "getSetCookie", { value: undefined });
    const resolution = await authJsResolutionFromResponse(response);
    assert.equal(resolution.setCookie?.length, 2);
    assert.match(resolution.setCookie?.[0] || "", /^one=1/);
    assert.match(resolution.setCookie?.[1] || "", /^two=2/);
  });

  it("retains the deprecated Auth.js factory input shape", async () => {
    const adapter = createAuthJsAdapter({
      auth: { getSession: async () => ({ user: { id: "legacy-authjs" } }) },
    });
    assert.equal((await adapter.getSession(new Request("https://example.com")))?.user?.id, "legacy-authjs");
  });
});
