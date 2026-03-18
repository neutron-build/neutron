import assert from "node:assert/strict";
import { describe, it, mock } from "node:test";
import {
  createAuthContextMiddleware,
  createProtectedRouteMiddleware,
  getAuthFromContext,
  requireAuth,
  createBetterAuthAdapter,
  createAuthJsAdapter,
  type AuthAdapter,
  type AuthSession,
  type NeutronAuthState,
} from "./index.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function fakeAdapter(
  session: AuthSession | null,
  name = "test-adapter"
): AuthAdapter {
  return {
    name,
    async getSession(_request: Request) {
      return session;
    },
  };
}

const OK_RESPONSE = () => new Response("ok", { status: 200 });

// ---------------------------------------------------------------------------
// createAuthContextMiddleware
// ---------------------------------------------------------------------------

describe("createAuthContextMiddleware", () => {
  it("populates context with authenticated state when session has a user", async () => {
    const session: AuthSession = { user: { id: "u1", email: "a@b.com" } };
    const adapter = fakeAdapter(session);
    const mw = createAuthContextMiddleware({ adapter });

    const ctx: Record<string, unknown> = {};
    const response = await mw(
      new Request("https://example.com"),
      ctx,
      async () => OK_RESPONSE()
    );

    assert.equal(response.status, 200);
    const auth = ctx["auth"] as NeutronAuthState;
    assert.equal(auth.adapter, "test-adapter");
    assert.equal(auth.isAuthenticated, true);
    assert.deepEqual(auth.user, { id: "u1", email: "a@b.com" });
    assert.equal(auth.session, session);
  });

  it("sets isAuthenticated to false when session has no user", async () => {
    const adapter = fakeAdapter({ expiresAt: "2099-01-01" });
    const mw = createAuthContextMiddleware({ adapter });

    const ctx: Record<string, unknown> = {};
    await mw(new Request("https://example.com"), ctx, async () => OK_RESPONSE());

    const auth = ctx["auth"] as NeutronAuthState;
    assert.equal(auth.isAuthenticated, false);
    assert.equal(auth.user, null);
  });

  it("sets isAuthenticated to false when session is null", async () => {
    const adapter = fakeAdapter(null);
    const mw = createAuthContextMiddleware({ adapter });

    const ctx: Record<string, unknown> = {};
    await mw(new Request("https://example.com"), ctx, async () => OK_RESPONSE());

    const auth = ctx["auth"] as NeutronAuthState;
    assert.equal(auth.isAuthenticated, false);
    assert.equal(auth.user, null);
    assert.equal(auth.session, null);
  });

  it("uses a custom contextKey when provided", async () => {
    const session: AuthSession = { user: { id: "u1" } };
    const adapter = fakeAdapter(session);
    const mw = createAuthContextMiddleware({ adapter, contextKey: "myAuth" });

    const ctx: Record<string, unknown> = {};
    await mw(new Request("https://example.com"), ctx, async () => OK_RESPONSE());

    assert.equal(ctx["auth"], undefined);
    const auth = ctx["myAuth"] as NeutronAuthState;
    assert.equal(auth.isAuthenticated, true);
  });

  it("calls next() and returns its response", async () => {
    const adapter = fakeAdapter(null);
    const mw = createAuthContextMiddleware({ adapter });
    const ctx: Record<string, unknown> = {};

    const response = await mw(
      new Request("https://example.com"),
      ctx,
      async () => new Response("custom", { status: 201 })
    );

    assert.equal(response.status, 201);
    assert.equal(await response.text(), "custom");
  });
});

// ---------------------------------------------------------------------------
// createProtectedRouteMiddleware
// ---------------------------------------------------------------------------

describe("createProtectedRouteMiddleware", () => {
  it("returns 401 when no auth state exists and no adapter is provided", async () => {
    const mw = createProtectedRouteMiddleware();
    const response = await mw(
      new Request("https://example.com/secret"),
      {},
      async () => OK_RESPONSE()
    );

    assert.equal(response.status, 401);
    assert.equal(await response.text(), "Unauthorized");
  });

  it("returns 401 when auth context shows unauthenticated", async () => {
    const mw = createProtectedRouteMiddleware();
    const ctx: Record<string, unknown> = {
      auth: {
        adapter: "test",
        session: null,
        user: null,
        isAuthenticated: false,
      },
    };

    const response = await mw(
      new Request("https://example.com/secret"),
      ctx,
      async () => OK_RESPONSE()
    );

    assert.equal(response.status, 401);
  });

  it("allows access when auth context shows authenticated", async () => {
    const mw = createProtectedRouteMiddleware();
    const ctx: Record<string, unknown> = {
      auth: {
        adapter: "test",
        session: { user: { id: "u1" } },
        user: { id: "u1" },
        isAuthenticated: true,
      },
    };

    const response = await mw(
      new Request("https://example.com/secret"),
      ctx,
      async () => OK_RESPONSE()
    );

    assert.equal(response.status, 200);
  });

  it("fetches session via adapter when no existing auth context", async () => {
    const session: AuthSession = { user: { id: "u1" } };
    const adapter = fakeAdapter(session);
    const mw = createProtectedRouteMiddleware({ adapter });
    const ctx: Record<string, unknown> = {};

    const response = await mw(
      new Request("https://example.com/secret"),
      ctx,
      async () => OK_RESPONSE()
    );

    assert.equal(response.status, 200);
    const auth = ctx["auth"] as NeutronAuthState;
    assert.equal(auth.isAuthenticated, true);
  });

  it("redirects when redirectTo is provided and user is unauthenticated", async () => {
    const mw = createProtectedRouteMiddleware({
      redirectTo: "https://example.com/login",
    });
    const response = await mw(
      new Request("https://example.com/secret"),
      {},
      async () => OK_RESPONSE()
    );

    assert.equal(response.status, 302);
    assert.ok(response.headers.get("Location")?.includes("/login"));
  });

  it("supports custom unauthorizedStatus", async () => {
    const mw = createProtectedRouteMiddleware({ unauthorizedStatus: 403 });
    const response = await mw(
      new Request("https://example.com/secret"),
      {},
      async () => OK_RESPONSE()
    );

    assert.equal(response.status, 403);
  });
});

// ---------------------------------------------------------------------------
// getAuthFromContext / requireAuth
// ---------------------------------------------------------------------------

describe("getAuthFromContext", () => {
  it("returns null when context key is missing", () => {
    assert.equal(getAuthFromContext({}), null);
  });

  it("returns null when context value is not an object", () => {
    assert.equal(getAuthFromContext({ auth: "not-an-object" }), null);
  });

  it("returns auth state when present", () => {
    const authState: NeutronAuthState = {
      adapter: "test",
      session: null,
      user: null,
      isAuthenticated: false,
    };
    const result = getAuthFromContext({ auth: authState });
    assert.deepEqual(result, authState);
  });

  it("uses custom context key", () => {
    const authState: NeutronAuthState = {
      adapter: "test",
      session: null,
      user: null,
      isAuthenticated: false,
    };
    assert.equal(getAuthFromContext({ auth: authState }, "other"), null);
    assert.deepEqual(getAuthFromContext({ other: authState }, "other"), authState);
  });
});

describe("requireAuth", () => {
  it("throws a 401 Response when not authenticated", () => {
    try {
      requireAuth({});
      assert.fail("Expected a thrown Response");
    } catch (err) {
      assert.ok(err instanceof Response);
      assert.equal((err as Response).status, 401);
    }
  });

  it("throws a 401 Response when auth state has isAuthenticated=false", () => {
    const ctx = {
      auth: {
        adapter: "test",
        session: null,
        user: null,
        isAuthenticated: false,
      },
    };
    try {
      requireAuth(ctx);
      assert.fail("Expected a thrown Response");
    } catch (err) {
      assert.ok(err instanceof Response);
    }
  });

  it("returns auth state when authenticated", () => {
    const authState: NeutronAuthState = {
      adapter: "test",
      session: { user: { id: "u1" } },
      user: { id: "u1" },
      isAuthenticated: true,
    };
    const result = requireAuth({ auth: authState });
    assert.equal(result.isAuthenticated, true);
    assert.deepEqual(result.user, { id: "u1" });
  });
});

// ---------------------------------------------------------------------------
// createBetterAuthAdapter
// ---------------------------------------------------------------------------

describe("createBetterAuthAdapter", () => {
  it("calls api.getSession when available", async () => {
    const mockSession = { session: { user: { id: "ba1" } } };
    const auth = {
      api: {
        getSession: mock.fn(async () => mockSession),
      },
    };
    const adapter = createBetterAuthAdapter({ auth });

    assert.equal(adapter.name, "better-auth");
    const session = await adapter.getSession(new Request("https://example.com"));

    assert.deepEqual(session, { user: { id: "ba1" } });
    assert.equal(auth.api.getSession.mock.calls.length, 1);
  });

  it("falls back to auth.getSession when api.getSession is not available", async () => {
    const mockSession = { session: { user: { id: "ba2" } } };
    const auth = {
      getSession: mock.fn(async () => mockSession),
    };
    const adapter = createBetterAuthAdapter({ auth });

    const session = await adapter.getSession(new Request("https://example.com"));
    assert.deepEqual(session, { user: { id: "ba2" } });
  });

  it("returns null when session data is null", async () => {
    const auth = {
      api: { getSession: async () => null },
    };
    const adapter = createBetterAuthAdapter({ auth });
    const session = await adapter.getSession(new Request("https://example.com"));
    assert.equal(session, null);
  });

  it("uses custom name when provided", () => {
    const auth = { api: { getSession: async () => null } };
    const adapter = createBetterAuthAdapter({ auth, name: "my-better-auth" });
    assert.equal(adapter.name, "my-better-auth");
  });
});

// ---------------------------------------------------------------------------
// createAuthJsAdapter
// ---------------------------------------------------------------------------

describe("createAuthJsAdapter", () => {
  it("calls function-form auth directly", async () => {
    const authFn = mock.fn(async () => ({ user: { id: "aj1", email: "aj@b.com" } }));
    const adapter = createAuthJsAdapter({ auth: authFn });

    assert.equal(adapter.name, "authjs");
    const session = await adapter.getSession(new Request("https://example.com"));
    assert.deepEqual(session, { user: { id: "aj1", email: "aj@b.com" } });
    assert.equal(authFn.mock.calls.length, 1);
  });

  it("calls auth.auth when available", async () => {
    const authObj = {
      auth: mock.fn(async () => ({ session: { user: { id: "aj2" } } })),
    };
    const adapter = createAuthJsAdapter({ auth: authObj });

    const session = await adapter.getSession(new Request("https://example.com"));
    // normalizeSession extracts the nested .session
    assert.deepEqual(session, { user: { id: "aj2" } });
  });

  it("calls auth.getSession as fallback", async () => {
    const authObj = {
      getSession: mock.fn(async () => ({ user: { id: "aj3" } })),
    };
    const adapter = createAuthJsAdapter({ auth: authObj });

    const session = await adapter.getSession(new Request("https://example.com"));
    assert.deepEqual(session, { user: { id: "aj3" } });
  });

  it("returns null when no auth data is returned", async () => {
    const authFn = async () => null;
    const adapter = createAuthJsAdapter({ auth: authFn });

    const session = await adapter.getSession(new Request("https://example.com"));
    assert.equal(session, null);
  });

  it("returns null for non-object raw responses", async () => {
    const authFn = async () => "not-an-object" as unknown;
    const adapter = createAuthJsAdapter({ auth: authFn });

    const session = await adapter.getSession(new Request("https://example.com"));
    assert.equal(session, null);
  });

  it("uses custom name when provided", () => {
    const authFn = async () => null;
    const adapter = createAuthJsAdapter({ auth: authFn, name: "my-authjs" });
    assert.equal(adapter.name, "my-authjs");
  });
});
