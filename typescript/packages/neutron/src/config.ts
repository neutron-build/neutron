import type { NeutronAdapter } from "./adapters/adapter.js";
import type { NeutronServerOptions } from "./server/index.js";

export type NeutronRuntime = "preact" | "react-compat";

export interface NeutronRedirectRule {
  source: string;
  destination: string;
  permanent?: boolean;
  statusCode?: 301 | 302 | 307 | 308;
}

export interface NeutronRewriteRule {
  source: string;
  destination: string;
}

export interface NeutronHeaderRule {
  /**
   * Path pattern: literal segments, `:param`, and one catch-all — `:name*`
   * (zero or more segments, so `/builder/:path*` also matches `/builder`, as
   * in Next) or `*` (one or more).
   */
  source: string;
  /**
   * Headers for matching responses, on every response the server sends for
   * the path (pages, static and resource routes, mounts). A header the
   * response set itself wins. A value here overrides the `securityHeaders`
   * default for these paths; `null` sends no such header here, suppressing
   * a security default (e.g. `{ "X-Frame-Options": null }` on embeddable
   * routes).
   */
  headers: Record<string, string | null>;
}

export interface NeutronRoutesConfig {
  redirects?: NeutronRedirectRule[];
  rewrites?: NeutronRewriteRule[];
  headers?: NeutronHeaderRule[];
}

export interface NeutronWorkerConfig {
  entry?: string;
}

/**
 * Input validation limits to prevent DoS attacks
 *
 * SECURITY: These limits protect against malicious requests with oversized payloads,
 * excessive headers, or extremely long URLs that could cause memory exhaustion or
 * processing delays.
 */
export interface NeutronInputLimits {
  /**
   * Maximum request body size in bytes
   * @default 10485760 (10MB)
   */
  maxRequestBodySize?: number;

  /**
   * Maximum size of individual header values in bytes
   * @default 16384 (16KB)
   */
  maxHeaderSize?: number;

  /**
   * Maximum number of headers allowed in a request
   * @default 100
   */
  maxHeaderCount?: number;

  /**
   * Maximum URL length in characters
   * @default 2048
   */
  maxUrlLength?: number;
}

export interface NeutronImageConfig {
  /** Default quality for optimized images (1-100) @default 75 */
  quality?: number;
  /** Preferred output formats in priority order @default ["avif", "webp"] */
  formats?: string[];
  /** Responsive image widths to generate @default [320, 640, 960, 1200, 1600] */
  widths?: number[];
  /** Allowed remote image hostnames */
  remotePatterns?: Array<{ hostname: string; protocol?: string }>;
}

export interface NeutronMarkdownConfig {
  /**
   * remark plugins applied to MDX content. Not applied to plain `.md`
   * content (which goes through the `marked` pipeline below).
   */
  remarkPlugins?: unknown[];
  /**
   * rehype plugins applied to MDX content. Not applied to plain `.md`
   * content (which goes through the `marked` pipeline below).
   */
  rehypePlugins?: unknown[];
  /**
   * `marked` extensions applied to plain `.md` content via
   * `Marked.use(...)`. Use this to wire `marked-katex-extension`,
   * `marked-directive`, `marked-extended-tables`, or custom token
   * extensions. Each entry must be a value accepted by `Marked.use`.
   * Applied after the built-in syntax-highlight extension if any.
   */
  markedExtensions?: unknown[];
  syntaxHighlight?: { theme?: string } | false;
  /**
   * Maximum number of rendered content bodies to keep in the in-memory,
   * content-addressed render cache. The cache lets a long-running SSR server
   * serve repeated markdown/MDX content without re-compiling it per request,
   * and bounds memory to a working set rather than the whole collection.
   * Static builds render each page once, so the cache is near-inert there; the
   * bound keeps it from accumulating the entire collection's HTML. Set to `0`
   * to disable caching entirely.
   * @default 256
   */
  renderCacheSize?: number;
}

export interface NeutronConfig {
  adapter?: NeutronAdapter;
  server?: Omit<NeutronServerOptions, "rootDir">;
  routes?: NeutronRoutesConfig;
  runtime?: NeutronRuntime;
  worker?: NeutronWorkerConfig;
  /** Image optimization configuration */
  image?: NeutronImageConfig;
  /**
   * Input validation limits
   * @default { maxRequestBodySize: 10485760, maxHeaderSize: 16384, maxHeaderCount: 100, maxUrlLength: 2048 }
   */
  inputLimits?: NeutronInputLimits;
  markdown?: NeutronMarkdownConfig;
}

export function defineConfig(config: NeutronConfig): NeutronConfig {
  return config;
}

export function resolveRuntime(config?: NeutronConfig): NeutronRuntime {
  return config?.runtime ?? "preact";
}

export function resolveRuntimeAliases(
  runtime: NeutronRuntime
): Record<string, string> | undefined {
  if (runtime !== "react-compat") {
    return undefined;
  }

  return {
    react: "preact/compat",
    "react-dom": "preact/compat",
    "react-dom/client": "preact/compat",
    "react-dom/server": "preact-render-to-string",
    "react-dom/server.browser": "preact-render-to-string",
    "react-dom/test-utils": "preact/test-utils",
    "react/jsx-runtime": "preact/jsx-runtime",
    "react/jsx-dev-runtime": "preact/jsx-dev-runtime",
  };
}

/**
 * SSR externals policy for a runtime.
 *
 * `preact` needs nothing: the SSR graph is already Preact.
 *
 * `react-compat` returns `true` — process EVERY node_modules dependency
 * through Vite's SSR graph. Under react-compat, any dependency that
 * imports `react`/`react-dom` must be transformed by Vite for the
 * react → preact/compat alias to reach it; an externalized dependency
 * resolves `react` natively and loads REAL React next to
 * preact-render-to-string, which crashes at render time (observed with
 * Radix: "Cannot read properties of null (reading 'useRef')"). Which
 * packages import react cannot be predicted from names — Fylun's graph
 * is dozens (Radix families, sonner, cmdk, …) and includes transitive
 * deps the app never lists — so the correct default is all of them.
 * The previous `react*`-only list covered only the direct alias targets.
 *
 * Tradeoff, stated honestly: `noExternal: true` makes the SSR runtime
 * bundle inline the whole reachable dependency graph and makes dev SSR
 * transform each dependency once (cached after that). That is the price
 * of a correct React-ecosystem graph. Opt specific dependencies back
 * out with `ssr.external: [...]` in vite.config.ts — Vite checks
 * `ssr.external` before `noExternal`, so named externals stay external
 * even under a blanket `noExternal: true` (mergeConfig collapses
 * `noExternal` to `true` when either side is `true`, so `ssr.external`
 * — not a narrower `noExternal` array — is the escape hatch).
 */
export function resolveRuntimeNoExternal(runtime: NeutronRuntime): string[] | true {
  if (runtime !== "react-compat") {
    return [];
  }

  return true;
}

/**
 * Deps that stay EXTERNAL under a runtime's blanket `ssr.noExternal: true`
 * because Vite's SSR module-runner cannot evaluate them: CommonJS-only (or
 * CommonJS at their resolved entry), each dies with `require is not
 * defined` / `exports is not defined` when the runner inlines it. They are
 * the tops of the CJS subtrees in @neutron-build/core's import graph:
 *
 * - `@mdx-js/mdx` — content/mdx; covers the hast/micromark/acorn-jsx and
 *   style-to-js chains beneath it
 * - `gray-matter` — content frontmatter (js-yaml/kind-of ride along)
 * - `yaml` — config/route-rules parsing
 * - `@babel/parser` — route type generation
 *
 * Keeping exactly these native lets CORE itself run in the SSR graph
 * (dev and start compose this the same way), which is what unifies core's
 * hooks (useLocation, useLoaderData, …) with the app's react-compat hooks
 * on one Preact instance — see RouteRenderer in core/render-app-route.ts.
 * A new CJS-only dependency fails loudly; the runner error names the
 * module, and it gets added here.
 */
export const RUNTIME_RUNNER_CJS_EXTERNALS = [
  "@mdx-js/mdx",
  "gray-matter",
  "yaml",
  "@babel/parser",
] as const;

/**
 * esbuild JSX settings for a runtime.
 *
 * The runtime is declared once, in neutron.config.ts. Everything derivable
 * from it should be derived here rather than restated by each project — the
 * automatic JSX runtime needs only an import source, so a project needs no
 * Vite plugin to compile JSX. Projects previously carried
 * `@preact/preset-vite` for this; that plugin remains useful in dev for HMR
 * and devtools, but it is no longer required for a correct build.
 *
 * react-compat keeps `react` as the import source because its aliases
 * (see resolveRuntimeAliases) already point react/jsx-runtime at Preact's.
 */
export function resolveRuntimeJsx(runtime: NeutronRuntime): {
  jsx: "automatic";
  jsxImportSource: string;
} {
  return {
    jsx: "automatic",
    jsxImportSource: runtime === "react-compat" ? "react" : "preact",
  };
}
