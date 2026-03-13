# Enterprise Add-Ons

> **Terminology note:** This page documents **Neutron TypeScript**. In broader ecosystem docs, **Neutron** refers to the umbrella framework/platform across implementations.


Neutron TypeScript core stays lean. Enterprise capabilities ship as optional packages.

## Distributed Cache (`@neutron/cache-redis`)

Use Redis/Dragonfly for cross-instance app + loader cache consistency.

```ts
import { createServer } from "neutron";
import { createRedisNeutronCacheStores } from "@neutron/cache-redis";

const cache = await createRedisNeutronCacheStores({
  url: process.env.DRAGONFLY_URL,
  keyPrefix: "myapp:",
});

await createServer({
  cache,
});
```

## OpenTelemetry (`@neutron/otel`)

Attach hook-based tracing without changing your route code.

```ts
import { createServer } from "neutron";
import { createOpenTelemetryHooks } from "@neutron/otel";

const otelHooks = await createOpenTelemetryHooks({
  serviceName: "my-neutron-app",
  serviceVersion: "1.0.0",
});

await createServer({
  hooks: otelHooks,
});
```

## Auth (`@neutron/auth`)

Create auth context middleware and protected-route guards.

```ts
import { createAuthContextMiddleware, createProtectedRouteMiddleware } from "@neutron/auth";

export const middleware = [
  createAuthContextMiddleware({ adapter: myAuthAdapter }),
  createProtectedRouteMiddleware({ redirectTo: "/login" }),
];
```

## Security (`@neutron/security`)

Apply CSP nonce, CSRF protection, and request-level rate limiting.

```ts
import {
  createCspNonceMiddleware,
  createCsrfMiddleware,
  createRateLimitMiddleware,
} from "@neutron/security";

export const middleware = [
  createCspNonceMiddleware(),
  createCsrfMiddleware(),
  createRateLimitMiddleware({ capacity: 100, refillPerSecond: 50 }),
];
```

## Ops (`@neutron/ops`)

Health/readiness endpoints and structured production logging.

```ts
import { createServer } from "neutron";
import {
  createHealthcheckMiddleware,
  createJsonLoggingHooks,
  createRequestContextMiddleware,
  mergeNeutronHooks,
} from "@neutron/ops";

const hooks = createJsonLoggingHooks();

await createServer({
  hooks: mergeNeutronHooks(undefined, hooks),
});

export const middleware = [
  createRequestContextMiddleware(),
  createHealthcheckMiddleware(),
];
```
