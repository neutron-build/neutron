# Deployment Guide

> **Terminology note:** This page documents **Neutron TypeScript**. In broader ecosystem docs, **Neutron** refers to the umbrella framework/platform across implementations.


## Choose the Right Preset

- `static`: static-host output with `_headers`, precompressed assets (`.br`/`.gz`), static policy metadata.
- `vercel`: Node runtime bundle + Vercel config.
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

- `--preset cloudflare`
- `--preset docker`

## One-Command Validation

Project-level:

```bash
neutron release-check --preset <static|vercel|cloudflare|docker>
```

Monorepo-level:

```bash
pnpm run ci:release
```
