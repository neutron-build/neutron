# Deployment Guide

> **Terminology note:** This page documents **Neutron TypeScript**. In broader ecosystem docs, **Neutron** refers to the umbrella framework/platform across implementations.


## Choose the Right Preset

- `static`: static-host output with `_headers`, precompressed assets (`.br`/`.gz`), static policy metadata.
- `vercel`: Node runtime bundle + Vercel config.
- `netlify`: Netlify Functions v2 handler + `_redirects`/`_headers`/`netlify.toml`.
- `cloudflare`: Workers/Pages runtime bundle + Wrangler config.
- `docker`: self-hosted Node runtime bundle + Dockerfile.

## Static Marketing Deploy

```bash
neutron build --preset static
neutron deploy-check --preset static
```

Notes:

- Static adapter emits cache headers and compression artifacts.
- Static routes should use `export const config = { mode: "static" }`.
- Add islands only where interactivity is required.

## App/SaaS Deploy

```bash
neutron build --preset vercel
neutron deploy-check --preset vercel
```

Equivalent for other targets:

- `--preset netlify`
- `--preset cloudflare`
- `--preset docker`

What the generated server runtime (vercel, netlify, cloudflare, docker) serves,
and how it differs from `neutron-ts start`:

- **Security headers**: the same set and order as `start` — `routes.headers`
  rules first (a value overrides a default on its paths, `null` keeps it
  off), then the five defaults (`X-Content-Type-Options`, `X-Frame-Options`,
  `Referrer-Policy`, `Cross-Origin-Opener-Policy`,
  `Cross-Origin-Resource-Policy`) plus `server.securityHeaders.headers`;
  `server.securityHeaders: false` turns them off. This covers every response
  the runtime answers. Files the platform serves itself (prerendered pages,
  `/assets/*`) carry only what `_headers` / `vercel.json` list.
- **Mounts**: `src/server.entry.ts` mounts are served with `start`'s
  semantics — global middleware ahead of each mount unless it opts out, an
  unmatched path falling through to the routes.
- **Not servable**, so the build fails instead of deploying them: a
  `websocket` in `src/server.entry.ts` (no target here has a WebSocket
  upgrade; use `neutron-ts start`), and server actions or mounts in an app
  without an app route (the runtime is only built for app routes). A
  `--preset static` build fails on any server action or server entry.

## One-Command Validation

Project-level:

```bash
neutron release-check --preset <static|vercel|netlify|cloudflare|docker>
```

Monorepo-level:

```bash
pnpm run ci:release
```
