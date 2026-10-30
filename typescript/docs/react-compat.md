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

## SSR and your dependency graph

Under `react-compat`, every command that builds or serves an SSR graph —
`dev`, `build` (static prerender and the runtime bundle) and `start` —
processes **all** `node_modules` dependencies through Vite
(`ssr.noExternal: true`), not just the `react*` alias targets. This is a
correctness requirement, not a preference: an SSR-*externalized* dependency
resolves its own `react` import through native Node, loading real React
next to `preact-render-to-string` — hooks then crash at render time
(observed with Radix: `Cannot read properties of null (reading 'useRef')`).
Which dependencies import react cannot be predicted from package names, so
the default covers all of them.

The cost is real and accepted knowingly: the SSR runtime bundle inlines the
whole reachable dependency graph (larger output, longer builds), and dev SSR
transforms each dependency once on first request (cached afterwards). If
you need a specific heavy package to stay external — a database driver that
must not be bundled, for example — list it in your `vite.config.ts`:

```ts
export default {
  ssr: {
    // Checked before noExternal, so these stay external even under the
    // blanket default. Do NOT list anything that imports react here.
    external: ["pg"],
  },
};
```

An app cannot narrow the default back to an array — Vite's `mergeConfig`
collapses `ssr.noExternal` to `true` when either side is `true` — so
`ssr.external` is the escape hatch.

`dev` and `start` themselves use that escape hatch for a fixed set of
framework deps: `@mdx-js/mdx`, `gray-matter`, `yaml` and `@babel/parser`
(CommonJS-only subtree tops in `@neutron-build/core`'s import graph — the
runner cannot evaluate them, see the limit below). Keeping exactly those
native lets **`@neutron-build/core` itself run inside the SSR graph**, which
is what puts the framework's own hooks (`useLocation`, `useLoaderData`, …)
and your React hooks on one Preact instance during per-request SSR. Your own
`ssr.external` entries merge with (do not replace) that list.

One known limit, stated plainly: a **CommonJS-only** dependency cannot be
processed by Vite's SSR module runner (`neutron-ts start`, and static
prerendering) — it fails loudly with `require is not defined`. `dev` is
unaffected (the dev optimizer pre-bundles it). A CJS React library should
ship as a client-only island instead of server-rendering, the same advice
the [compatibility matrix](./react-compat-matrix.md) gives for libraries
that fail under `preact/compat` generally.

## TypeScript setup: `@types/react` and `jsxImportSource`

The `react` npm package ships **no bundled types** — that is React's own
choice, not Neutron's aliasing. Under `react-compat` the bundler never loads
real React: `react`, `react-dom` and their subpaths alias onto
`preact/compat` / `preact-render-to-string` (see `resolveRuntimeAliases` in
`@neutron-build/core/config`). Typechecking is a separate story — React
imports and React-authored JSX are checked against whatever types the
compiler can resolve, so a react-compat app needs one devDependency:

```sh
pnpm add -D @types/react
```

`create-neutron --runtime react-compat` scaffolds with this line already in
place. The real `react`/`react-dom` runtime packages are deliberately **not**
added: the aliases make them unnecessary, and an installed-but-unaliased
React next to Preact is exactly the two-copies hazard the aliases exist to
prevent. The react-compat scaffold also sets `jsxImportSource: "react"` in
`tsconfig.json` (see below) and ships a `typecheck` script (`tsc --noEmit`),
so its sources compile out of the box after install.

### Choosing how your JSX is typed

Two coherent setups; mixing them per-file is fine (the pragma is per-module):

1. **Preact-typed JSX (`jsxImportSource: "preact"`).** What a `preact`-runtime
   scaffold ships. JSX in your own modules checks against Preact's types while
   React-ecosystem components you import still carry their React-typed
   props. This is the least-friction default; occasionally a React library's
   React-typed `children` prop disagrees with Preact's JSX types at the call
   site — wrap that usage in a small module of its own (see 2) rather than
   casting everywhere.

2. **React-typed modules (`jsxImportSource: "react"`).** The setup for
   migration-heavy ports (React-authored JSX everywhere, e.g. a Radix-heavy
   app). Set it per-file with a pragma, or project-wide in `tsconfig.json` —
   which is what a `--runtime react-compat` scaffold now ships, using the
   same runtime → import-source rule the Vite transform already applies
   (`resolveRuntimeJsx`), so typechecking and bundling agree:

   ```tsx
   /** @jsxImportSource react */
   import { Form } from "@neutron-build/core/client";
   ```

   The framework's client components are usable from React-typed JSX: their
   element returns are deliberately JSX-agnostic (`<Island>`, `<Form>`,
   `<Link>`, `<NavLink>`, `<ViewTransitions>`, `<ScrollReveal>`, `<Image>`,
   `<Picture>`, `<Await>`), while their props stay fully typed — a mistyped
   prop fails TS2322 against the same `FormProps`/`LinkProps` under either
   import source. Both worlds are pinned by compiled fixtures in
   `@neutron-build/core` (`src/client/type-fixtures/react-nav-form/` and
   `react-island/`, compiled with `jsxImportSource: "react"` against
   @types/react) and by the preact-JSX component tests in the package build.

   One residual, stated plainly: the control-flow components (`Show`, `For`,
   `Switch`, `Match`, `Index`) still return preact `VNode`s and fail TS2786
   from React-typed JSX. Keep modules that use them Preact-typed (setup 1,
   or the per-file pragma `/** @jsxImportSource preact */`) until they
   receive the same treatment.

3. **Children props shared by both worlds.** A component that *forwards*
   `children` in JSX cannot type that prop with either runtime's native
   type if the module must compile under both import sources: Preact's
   `ComponentChildren` includes bare `object`, which React's `ReactNode`
   rejects in the render position (TS2322), and a preact-only install has no
   `ReactNode` to import. `@neutron-build/core/client` exports `JsxChildren`
   for exactly that shape — the structural element type both runtimes
   produce and accept back. The scaffolded root `_layout.tsx` uses it, so
   the same source file typechecks under either scaffold runtime.

## Compatibility Matrix

| Tier | Status | Notes |
| --- | --- | --- |
| React component model (JSX, hooks, context, memo, refs) | Supported | Via `preact/compat` aliases. |
| App routing/loaders/actions in Neutron | Supported | Same runtime APIs in both modes. |
| Client navigation/forms/hydration | Supported | Covered by runtime-compat smoke lane. |
| Common React libraries that depend on public React APIs | **Tested per package** | See the [compatibility matrix](./react-compat-matrix.md) — real packages, server-rendered, 28 of 29 currently render. |
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
