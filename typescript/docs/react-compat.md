# React Compatibility

> **Terminology note:** This page documents **Neutron TypeScript**. In broader ecosystem docs, **Neutron** refers to the umbrella framework/platform across implementations.


Neutron TypeScript supports two runtime modes:

- `preact` (default): direct Preact runtime.
- `react-compat`: aliases `react` and `react-dom` to `preact/compat`.
- Includes `react-dom/server` compatibility alias to `preact-render-to-string` for SSR/static render paths.

Recommended default:

- Use `preact` for maximum runtime performance and lowest overhead.
- Use `react-compat` when you need React package compatibility during migration.

Set runtime mode in `neutron.config.ts`:

```ts
import { defineConfig } from "@neutron-build/core";

export default defineConfig({
  runtime: "react-compat",
});
```

## Compatibility Matrix

| Tier | Status | Notes |
| --- | --- | --- |
| React component model (JSX, hooks, context, memo, refs) | Supported | Via `preact/compat` aliases. |
| App routing/loaders/actions in Neutron | Supported | Same runtime APIs in both modes. |
| Client navigation/forms/hydration | Supported | Covered by runtime-compat smoke lane. |
| Common React libraries that depend on public React APIs | **Tested per package** | See the [compatibility matrix](./react-compat-matrix.md) — real packages, server-rendered, 12 of 13 currently render. |
| Libraries depending on private React internals | Not guaranteed | May fail with `preact/compat`. |
| React Server Components / Next.js RSC model | Not supported | Out of scope for Neutron runtime. |

## Which libraries actually work

**[React library compatibility matrix](./react-compat-matrix.md)** — generated,
not asserted. Each library is mounted for real under `preact/compat` and
server-rendered; a row says yes only if it produced the markup it should.

This page used to answer the question with "usually works — verify per package
in app context", which hands the risk to whoever is evaluating Neutron, at
evaluation time, on their own codebase — and they are usually holding a
Radix-heavy React app when they ask. The matrix answers it instead.

Regenerate after changing a version:

```sh
pnpm --filter @neutron/compat-matrix matrix:write
```

Adding a library is one entry in `compat-matrix/libraries.mjs`: a mount and an
assertion.

## CI Coverage

Two lanes run on every TypeScript workflow build:

- `pnpm run ci:runtime-compat` — the dual-runtime smoke lane. Runs the same
  `@neutron/playground` app under `NEUTRON_RUNTIME=preact` and
  `NEUTRON_RUNTIME=react-compat`, verifying build, server start, HTML responses
  and JSON data transport. This proves the **framework** works in both modes.
- `pnpm run ci:compat-matrix` — the ecosystem lane behind the matrix above. It
  fails if a library that previously rendered stops rendering; a library that
  has never rendered is reported, not fatal, because the matrix is meant to
  state the truth including where the truth is "no".

Both existed as scripts for some time while no workflow invoked them, and this
page claimed the first as CI coverage regardless. They are wired now.
