# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

### Added

- New core cache-store abstraction for server runtime (`cache.app` + `cache.loader`) with memory defaults and exported cache store types/factories.
- New package `@neutron/cache-redis` for distributed Redis/Dragonfly-backed app + loader cache stores.
- New package `@neutron/otel` for Neutron hook -> OpenTelemetry span/error integration.
- New package `@neutron/auth` for auth context middleware, protected-route middleware, and Better Auth/Auth.js style adapters.
- New package `@neutron/security` for CSP nonce middleware, CSRF middleware, trusted proxy IP resolution, rate limiting, and secure cookie defaults.
- New package `@neutron/ops` for request-id/trace context middleware, health/readiness middleware, and structured JSON logging hooks.
- New enterprise documentation (`docs/enterprise.md`) plus security/support policies (`SECURITY.md`, `SUPPORT.md`).

### Changed

- `createServer` now supports pluggable cache stores through `NeutronServerOptions.cache`.
- Release docs now include explicit support and deprecation policy commitments.

## [0.1.0] - 2026-02-13

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
