# @neutron-build/auth

Authentication context and protected-route middleware for Neutron.

The package resolves sessions supplied by an authentication provider. It does
not mount login, callback, logout, or account-management endpoints; mount those
through Neutron resource routes using your provider's request handler.

## Better Auth

```ts
import {
  createAuthContextMiddleware,
  createBetterAuthAdapter,
} from "@neutron-build/auth";
import { auth } from "./lib/auth.server";

const adapter = createBetterAuthAdapter({ auth });

export const middleware = [
  createAuthContextMiddleware({ adapter }),
];
```

The adapter uses Better Auth's documented `{ session, user }` response, removes
the bearer session token from Neutron context by default, and forwards refreshed
or cleared session cookies to the outgoing response. Set
`includeSessionToken: true` only if server code explicitly needs the token.

Better Auth's `customSession` plugin can change the response shape. Normalize it
explicitly rather than relying on structural guessing:

```ts
const adapter = createBetterAuthAdapter({
  auth,
  mapSession(value) {
    if (!value || typeof value !== "object") return null;
    const result = value as { identity?: { id: string }; expiresAt?: Date };
    return result.identity
      ? { user: result.identity, expiresAt: result.expiresAt }
      : null;
  },
});
```

Better Auth's deferred session refresh mode is supported through `auth.handler`.
Set `basePath` when the auth handler is mounted anywhere other than
`/api/auth`; the adapter performs the required POST refresh and forwards its
cookies when `needsRefresh` is returned.

Do not combine Better Auth's `customSession` and `deferSessionRefresh` plugins.
In Better Auth 1.6.x, `customSession` replaces `/get-session` with a GET-only
endpoint, while deferred renewal requires POST to that same path. This is an
upstream endpoint conflict rather than a shape the adapter can normalize.

## Protected Routes

Use the context middleware globally, then protect selected app routes:

```ts
import {
  createProtectedRouteMiddleware,
  requireAuth,
} from "@neutron-build/auth";
import type { LoaderArgs } from "@neutron-build/core";

export const config = { mode: "app" };
export const middleware = createProtectedRouteMiddleware({
  redirectTo: "/login",
});

export function loader({ context }: LoaderArgs) {
  const auth = requireAuth(context);
  return { user: auth.user };
}
```

Alternatively, pass an adapter directly to `createProtectedRouteMiddleware`
when global auth context is not installed.

## Auth.js

Auth.js does not expose a framework-neutral `auth(request)` function that
returns a session. Its Next.js `auth(request)` form returns a `Response`, while
its server session form depends on Next.js request context. Supply an
application-owned resolver that returns both the direct Auth.js session and any
rotation or cleanup cookies:

```ts
import {
  authJsResolutionFromResponse,
  createAuthJsAdapter,
} from "@neutron-build/auth";
import { Auth } from "@auth/core";

const adapter = createAuthJsAdapter({
  async resolveSession(request) {
    const url = new URL("/auth/session", request.url);
    const response = await Auth(new Request(url, {
      method: "GET",
      headers: request.headers,
    }), authConfig);
    return authJsResolutionFromResponse(response);
  },
});
```

Do not pass the NextAuth v5 `auth` export or the v4 client `getSession` helper.
The simpler `getSession(request)` option remains available when a resolver does
not rotate cookies. The original `{ auth }` factory input remains accepted for
source compatibility, but should not receive NextAuth's v5 `auth` export or its
v4 client helper.

## Custom Adapters

Existing adapters that implement `getSession(request)` remain supported:

```ts
const adapter = {
  name: "custom",
  async getSession(request: Request) {
    return { user: { id: "user-id" }, expiresAt: new Date(Date.now() + 60_000) };
  },
};
```

Adapters that refresh provider cookies can additionally implement `resolve()`
and return `{ session, setCookie }`. Middleware appends every cookie to success,
redirect, and unauthorized responses.
