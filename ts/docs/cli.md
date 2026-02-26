# Neutron CLI

> **Terminology note:** This page documents **Neutron TypeScript**. In broader ecosystem docs, **Neutron** refers to the umbrella framework/platform across implementations.


`neutron` commands:

## `neutron dev`

Starts the development server with Vite + Neutron middleware.

Notes:

- Auto-prepares content collections types before server start.
- Uses `neutron.config.*` runtime (`preact` or `react-compat`).

## `neutron build`

Builds client/server output and static pages.

Options:

- `--preset vercel|cloudflare|docker|static`
- `--cloudflare-mode pages|workers` (when `--preset cloudflare`)

Notes:

- Auto-prepares content collections manifest + types.
- Discovers app/static routes and renders static routes at build time.

## `neutron start`

Starts the production server from current project.

Options:

- `--port <number>`
- `--host <host>`

Both options also support `--port=<number>` and `--host=<host>`.

## `neutron preview`

Previews production output:

- static-only app: static server
- app routes present: Neutron production server

Default preview port is `4173` (or `server.port` from config).

## `neutron deploy-check`

Validates adapter artifacts after build.

Options:

- `--preset vercel|cloudflare|docker|static`
- `--dist <dir>` (default `dist`)

`--preset` is optional; when omitted, Neutron detects built adapter metadata in `dist/`.

## `neutron release-check`

Runs:

1. `neutron build ...`
2. `neutron deploy-check ...`

Options:

- `--preset vercel|cloudflare|docker|static`
- `--dist <dir>` (default `dist`)

## Notes

- Runtime mode is controlled by `neutron.config.*` (`runtime: "preact" | "react-compat"`).
- Content collections are prepared automatically during `dev` and `build`.

## `neutron worker`

Runs a background worker module (via Vite SSR module loading).

Options:

- `--entry <path>` (optional)
- `--mode development|production` (default `development`)
- `--once` (run worker once, then exit)

Worker module contract:

- Export `run(context)` or default function.
- `context` includes: `mode`, `args`, `signal`, `log`.
- Optional return value: teardown function called on shutdown.

Entry resolution order:

1. `--entry`
2. `worker.entry` in `neutron.config.*`
3. `src/worker.ts` (and related fallback candidates)
