# Migration Guide

> **Terminology note:** This page documents **Neutron TypeScript**. In broader ecosystem docs, **Neutron** refers to the umbrella framework/platform across implementations.


## From Next.js

1. Move page data functions (`getServerSideProps`, route handlers) into route `loader()` and `action()`.
2. Replace framework-specific request/response helpers with standard `Request`, `Response`, `FormData`.
3. Map filesystem routes into `src/routes`.
4. Add route mode explicitly:
   - `export const config = { mode: "static" }`
   - `export const config = { mode: "app" }`
5. Replace navigation/form primitives with Neutron equivalents:
   - `useRouter` -> `useNavigate`
   - form mutations -> `<Form method="post">` + `action()`

## From Remix 2 / Remix 3

1. Keep route module shape (`loader`, `action`, `ErrorBoundary`) and web-standard APIs.
2. Move files into Neutron route conventions under `src/routes`.
3. Add `config.mode` (`static` or `app`) to each route.
4. Replace Remix imports with Neutron exports:
   - hooks from `neutron`
   - components (`Form`, `Link`, `NavLink`) from `neutron`

## From Astro

1. Keep content in `src/content` and define schemas in `src/content/config.*`.
2. Use static routes for zero-JS pages (`mode: "static"`).
3. Use app routes only where interactive SSR + loader/action flow is needed (`mode: "app"`).
4. Keep island behavior by rendering `Island` components or `neutron-island` output in static/app HTML where needed.

## Runtime Choice

Set runtime in `neutron.config.*`:

```ts
import { defineConfig } from "neutron";

export default defineConfig({
  runtime: "preact", // or "react-compat"
});
```

## Static vs App Route Choice

Use `mode: "static"` when:

- Content is known at build time.
- You want zero-JS by default (unless adding islands).
- CDN/static host cache hit ratio is priority.

Use `mode: "app"` when:

- You need per-request data or auth/session context.
- You use `action()` mutations and form handling.
- You need dynamic cache invalidation behavior.

Recommended pattern:

- Marketing/content pages: static.
- Authenticated product surface: app.

## React-Compat Limits

`react-compat` mode is `preact/compat`.

- Works for most React component/hook libraries.
- Not guaranteed for packages relying on private React internals.
- React Server Components model is not supported.

For maximum raw runtime performance, prefer `runtime: "preact"`.

## Enterprise Hardening Add-Ons

Use optional packages only when needed:

- `@neutron/cache-redis` for distributed app/loader cache in multi-instance deployments.
- `@neutron/otel` for OpenTelemetry tracing via server hooks.
- `@neutron/auth` for auth context + protected-route middleware.
- `@neutron/security` for CSP nonce, CSRF, trusted proxy, and rate limiting.
- `@neutron/ops` for health/readiness endpoints and structured logging hooks.

## Deployment Guide (Quick)

- Static-only site: `neutron build --preset static`
- Node app route deployment: `neutron build --preset vercel` (or cloudflare/docker as needed)
- Validate build output: `neutron release-check --preset <target>`

## Upgrade Discipline

- Treat minor versions as additive changes.
- Document breaking behavior in `CHANGELOG.md`.
- Run:
  - `pnpm --filter neutron test`
  - `pnpm -r build`
  - `pnpm run ci:runtime-compat`
  - `pnpm run ci:deploy-presets`
  - `pnpm --dir benchmarks run compare:gate:smoke`
