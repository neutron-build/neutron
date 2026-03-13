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
import { defineConfig } from "neutron";

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
| Common React libraries that depend on public React APIs | Usually works | Verify per package in app context. |
| Libraries depending on private React internals | Not guaranteed | May fail with `preact/compat`. |
| React Server Components / Next.js RSC model | Not supported | Out of scope for Neutron runtime. |

## CI Coverage

Neutron CI includes a dual-runtime smoke lane:

- `pnpm run ci:runtime-compat`
- Runs the same `@neutron/playground` app in both:
  - `NEUTRON_RUNTIME=preact`
  - `NEUTRON_RUNTIME=react-compat`
- Verifies build, server start, HTML responses, and JSON data transport.
