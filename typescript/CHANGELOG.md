# Changelog

All notable changes to this project are documented in this file.

## [0.1.0] - 2026-05-28

> First coordinated multi-package publish to npm. `@neutron-build/core` and
> `@neutron-build/cli` move from 0.0.1 to 0.1.0; the other eight packages
> publish for the first time, all at 0.1.0. Canonical npm scope is
> `@neutron-build/*` (plus the unscoped `create-neutron`) — see Reality note
> in `docs/rfcs/naming.md`.

### Added

- Core cache-store abstraction for server runtime (`cache.app` + `cache.loader`) with memory defaults and exported cache store types/factories.
- New package `@neutron-build/cache-redis` for distributed Redis/Dragonfly-backed app + loader cache stores.
- New package `@neutron-build/otel` for Neutron hook → OpenTelemetry span/error integration.
- New package `@neutron-build/auth` for auth context middleware, protected-route middleware, and Better Auth/Auth.js style adapters.
- New package `@neutron-build/security` for CSP nonce middleware, CSRF middleware, trusted-proxy IP resolution, rate limiting, and secure cookie defaults.
- New package `@neutron-build/ops` for request-id/trace context middleware, health/readiness middleware, and structured JSON logging hooks.
- New package `@neutron-build/data` (database, cache, sessions, queues, storage, rate limiting).
- New package `@neutron-build/nucleus` — typed Nucleus client (14 data models).
- New package `create-neutron` — project scaffold (`npm create neutron@latest`).
- Enterprise documentation (`docs/enterprise.md`) plus security/support policies (`SECURITY.md`, `SUPPORT.md`).
- Semgrep + `pnpm audit` CI workflow (`.github/workflows/typescript_security.yml`).

### Changed

- `createServer` now supports pluggable cache stores through `NeutronServerOptions.cache`.
- Release docs include explicit support and deprecation policy commitments.
- Naming RFC amended to bless the `@neutron-build/*` org scope (bare `neutron`/`nucleus` and the `@neutron`/`@nucleus` scopes are unavailable on npm).
- Tag format standardized as `ts/vX.Y.Z` (matches the existing `typescript-publish.yml` trigger).

### Security (regression-tested)

- Cross-user app-response cache poisoning closed via credentials gate; CORS-reflected responses no longer cached.
- Rate-limit X-Forwarded-For spoof bypass: default no longer trusts XFF; `trustProxy` opt-in with right-most hop; per-client `context.clientAddress`; key cap.
- CSRF: `crypto.timingSafeEqual` compare; same-origin (Origin/Referer) check; cookie defaults to `HttpOnly` + `SameSite=Strict`; token reuse to stop churn; lazy form-body read.
- Fail-open `X-Forwarded-Proto` removed; new `trustedHosts` server option; `__Host-`/`__Secure-` cookie-prefix enforcement; production-default Secure cookie.
- Bypassable regex HTML sanitizers replaced with trusted-by-default content + optional `sanitize-html` peer dependency; head fragment and island sinks corrected.
- RSS `content:encoded` CDATA-terminator escape; SEO `on*` attribute names blocked; hardened `</script>` head-script guard.
- Request-smuggling rejection (Content-Length + Transfer-Encoding); byte-accurate header limits; opt-in `rejectUnknownLength`.
- Open-redirect closed in route rules (protocol-relative, backslash, tab/newline collapse) and in auth `redirectTo` (same-origin only).
- Default `nosniff` / `X-Frame-Options` / `Referrer-Policy` on static, docker, and generated serverless adapters; docker static path traversal containment + null-byte reject.
- CSP nonce stamped onto framework inline scripts (data, client module, JSON-LD, `headScripts`).
- Dep CVEs: `devalue` ≥5.6.4, `hono` 4.12.x, `@hono/node-server` 1.19.x, `turbo` 2.9.x, `fast-xml-parser` ≥5.7.0 (override).

### Fixed

- Dead Nucleus client specifier in `@neutron-build/data`/drizzle.
- Syntax highlighting restored (Marked v15 async `walkTokens` integration with Shiki).
- `defineCollection` no longer silently drops `live`/`loader`/`cacheTtl`.
- Audit-log IDs use `randomUUID()` (was `Math.random()`).
- Deduplicated `escapeHtml`/`escapeXml` across the codebase into `core/escape.ts`.
- Site install docs corrected to reflect actual published package names and CLI-preset adapter model.

## [0.1.0-dev] - 2026-02-13

### Added

- Server E2E matrix coverage for static/app/islands/forms/errors/streaming routes.
- Content collections recursive file discovery (nested slugs).
- Additional docs for API, benchmarks, migration, release workflow, and examples.
- Deploy presets CI workflow.
- `neutron worker` command with `--entry`, `--mode`, and `--once`.
- `neutron-data` Redis/Dragonfly session driver factory (`createRedisSessionStore`).
- `apps/playground` neutron-data integration profile (`memory` vs `production`) with worker entry and DB migration/seed scripts.
- Server observability hooks (`onRequestStart/End`, loader/action lifecycle, `onError`) for external telemetry adapters.
- Data-profile smoke script + CI lane (`ci:data-profiles`) for `apps/playground` memory profile, with optional production-profile checks when env/services are provided.
- Dedicated example packages: `examples/marketing-reference` and `examples/saas-reference`.
- `create-neutron` templates expanded to `basic`, `marketing`, `app`, and `full`.
- New `neutron release-check` command (build + deploy artifact validation).
- New one-command monorepo release gate: `pnpm run ci:release`.
- New SEO utilities: `buildMetaTags`, `renderMetaTags`, `buildSitemapXml`, `buildRobotsTxt`.
- New i18n routing primitives: `resolveLocalePath`, `withLocalePath`, `stripLocalePrefix`, `createI18nMiddleware`.
- New `Image` component with responsive `srcset` generation and pluggable image loader.
- Benchmark canonical publish workflow: `compare:canonical` and `ci:bench:canonical`.
- Static adapter route coverage test for headers/precompression behavior.
- Deployment guide doc (`docs/deployment.md`) and updated CLI/create-neutron docs for static preset + release checks.

### Changed

- Content collection generated types now emit optional object properties as `?:`.
- E2E islands assertion aligned to production server client-entry injection behavior.
- Route cache config now supports `cache.loaderMaxAge` for loader-data caching.
- App route config now supports `hydrate: false` to disable client runtime/data injection for zero-JS SSR pages.
- Node app runtime now supports loader auto-caching with mutation invalidation.
- Generated adapter runtime bundles now mirror loader auto-cache + invalidation behavior.
- Client navigation protocol now supports `X-Neutron-Data` + optional `X-Neutron-Routes` partial loader requests, with stale-request protection and stronger navigation state handling.
- Content collections now emit clearer contextual errors for parse/schema/MDX failures.
- Route discovery now recognizes `_layout` files across all supported route extensions (`.ts`, `.tsx`, `.js`, `.jsx`, `.mdx`).
- Content config loading now supports `src/content/config.ts` via runtime transpile fallback when Node cannot import TypeScript directly.
- Benchmark harness `neutron-react` lane now runs the same benchmark app as `neutron` (`apps/playground`) with runtime switched by `NEUTRON_RUNTIME` for true renderer parity.
- Benchmark harness now supports load profiles (`BENCH_PROFILE=baseline|stress|saturation`) and payload parity auditing (`BENCH_PAYLOAD_AUDIT`, `BENCH_PAYLOAD_WARN_RATIO`).
- Static adapter output now emits richer static policy metadata, route-level HTML cache header rules, and precompressed artifacts.
- Static benchmark host (`benchmarks/serve-static.mjs`) now uses pre-indexed route resolution, `_headers` parsing, precompressed variant selection, and optional in-memory small-asset serving for more realistic static-host benchmarking.
- Islands hydration path now uses a single runtime path (removed duplicate inline island runtime injection), with stronger island component ID stability and client registration hardening.
- Route discovery now supports route groups (`(group)` directories) without leaking group names into URL paths.
- Vite client route manifest generation now uses lazy route module loading for route-level client code-splitting.
- `neutron build --preset` and `neutron deploy-check --preset` now include `static`.
- `neutron release-check` and docs now define one-command release-grade project validation flow.

### Fixed

- SSR middleware Vite HMR port collision in parallel test runs by using a free port.
- `examples/saas-reference` client route imports now use `neutron/client` for `<Form>` so production client builds do not pull server-only runtime modules.
- Static benchmark fallback cache-control behavior for extensionless HTML routes now defaults correctly to `must-revalidate`.

### Performance

- Completed a fresh full benchmark matrix run and repinned benchmark baselines (`baseline-full.json` and `baseline.json`) from `benchmarks/results/latest.json` for release gating consistency.
- Improved Neutron optimal-static benchmark throughput substantially via benchmark static-host server optimizations (pre-indexed route map + in-memory small-asset serving).
