import * as path from "node:path";
import * as fs from "node:fs";
import { createHash } from "node:crypto";
import { build as viteBuild, loadConfigFromFile, mergeConfig, createServer } from "vite";
import { neutronPlugin, CLIENT_ROUTE_QUERY } from "@neutron-build/core/vite";
import { runtimeOxc, warnOnDuplicateCorePlugin } from "../lib/vite-shared.js";
import {
  discoverRoutes,
  adapterCloudflare,
  adapterDocker,
  adapterNetlify,
  adapterStatic,
  adapterVercel,
  prepareContentCollections,
  prepareRouteTypes,
  resolveRuntime,
  resolveRuntimeAliases,
  resolveRuntimeNoExternal,
  RUNTIME_RUNNER_CJS_EXTERNALS,
  resolvePreactSsr,
  vitePreactAliases,
  mergeSeoMetaInput,
  renderDocumentHead,
  buildHtmlOpenTag,
  buildBodyOpenTag,
  setActiveMarkdownConfig,
  assertRenderedFragment,
  renderSpeculationRules,
  isResponse,
  isResourceRoute,
  withRouterProviders,
  type CreateElement,
} from "@neutron-build/core";
import { findGatedStaticRoutes, gatedStaticRoutesError } from "./gated-static.js";
import type {
  NeutronConfig,
  NeutronAdapter,
  Route,
  RouteModule,
  AppContext,
  LoaderArgs,
  HeadersArgs,
  HeadArgs,
  SeoMetaInput,
  GetStaticPathsResult,
} from "@neutron-build/core";
import { renderToString } from "preact-render-to-string";
import { h } from "preact";
import { createRequire } from "node:module";
import { extractClientEntryScriptSrc } from "../client-entry.js";
import { loadNeutronConfig } from "../lib/config.js";
import { isUnsafeResolvedPath, resolvePath } from "./static-paths.js";
import { writePublicFilesMetadata } from "./public-files.js";
import {
  SERVER_BUNDLE_MANIFEST,
  SERVER_BUNDLE_FORMAT,
  findGlobalMiddlewareFile,
  generateServerBundleEntry,
} from "@neutron-build/core/server";

export async function build(): Promise<void> {
  const cwd = process.cwd();
  const routesDir = path.resolve(cwd, "src/routes");
  const outputDir = path.resolve(cwd, "dist");
  const neutronConfig = await loadNeutronConfig(cwd, { command: "build" });
  const runtime = resolveRuntime(neutronConfig);
  // JSX comes from the declared runtime, so a project needs no Vite plugin of
  // its own to compile it: the fact is stated once, in neutron.config.ts.
  const oxcJsx = runtimeOxc(runtime);
  const runtimeAliases = resolveRuntimeAliases(runtime);
  const runtimeNoExternal = resolveRuntimeNoExternal(runtime);
  // Absolute preact / RTS paths so Vite SSR can resolve the renderer even when
  // the app only declares `preact` (pnpm keeps RTS under core/cli). Without
  // these aliases, ssrLoadModule("preact-render-to-string") fails and a native
  // fallback binds a second Preact options object → hooks crash with __H.
  const preactSsr = resolvePreactSsr(cwd, {
    from: [createRequire(import.meta.url).resolve("../../package.json")],
  });
  // Ordered array: subpaths before bare `preact` so jsx-dev-runtime is not
  // joined onto the package root (Vite string aliases are prefix-matched).
  const preactAliases = vitePreactAliases(preactSsr, runtimeAliases);
  const buildArgs = parseBuildArgs(process.argv.slice(3));
  const selectedAdapter = resolveAdapterForBuild(neutronConfig, buildArgs);

  // Wire user-supplied markdown config (marked extensions, remark/rehype
  // plugins) so it's picked up by content rendering throughout the build.
  setActiveMarkdownConfig(neutronConfig.markdown);

  await prepareContentCollections({
    rootDir: cwd,
    // Manifest is only a runtime fallback for sites lacking a content.config.ts.
    // Skipping the write avoids a JSON.stringify overflow on very large content
    // sets (Node's max string length is ~512 MiB and KaTeX-rendered HTML can
    // push the combined manifest past that).
    writeManifest: false,
    writeTypes: true,
    markdownConfig: neutronConfig.markdown,
  } as any);
  await prepareRouteTypes({
    rootDir: cwd,
    routesDir: "src/routes",
    writeTypes: true,
  });

  if (!fs.existsSync(routesDir)) {
    console.error(`Routes directory not found: ${routesDir}`);
    process.exit(1);
  }

  console.log("Building Neutron app...\n");

  const routes = discoverRoutes({ routesDir });
  const pageRoutes = routes.filter(
    (r) => !r.file.includes("_layout")
  );
  const staticRouteCount = pageRoutes.filter((route) => route.config.mode === "static").length;
  const appRouteCount = pageRoutes.filter((route) => route.config.mode === "app").length;

  // Static-only sites can still use <Island> for interactive hydration; if any
  // source file references Island from the client entrypoint, we need the full
  // client bundle (not just CSS) so the island-runtime + component code ship
  // to the browser.
  function fileUsesIsland(p: string): boolean {
    try {
      const src = fs.readFileSync(p, "utf-8");
      return /\bIsland\b/.test(src) && /from\s+["'][^"']*@neutron-build\/core\/client["']/.test(src);
    } catch {
      return false;
    }
  }
  function walkForIslands(dir: string): boolean {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      if (entry.name === "node_modules" || entry.name.startsWith(".")) continue;
      const p = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        if (walkForIslands(p)) return true;
      } else if (/\.(tsx|ts|jsx|js)$/.test(entry.name) && fileUsesIsland(p)) {
        return true;
      }
    }
    return false;
  }
  const srcDir = path.join(cwd, "src");
  const hasIslands =
    routes.some((r) => fileUsesIsland(r.file)) ||
    (fs.existsSync(srcDir) && walkForIslands(srcDir));
  void staticRouteCount;

  // List only page routes — `_layout` files are layouts, not navigable routes,
  // and listing them produced confusing duplicates (e.g. `/` shown twice).
  console.log(`Found ${pageRoutes.length} routes:\n`);
  for (const route of pageRoutes) {
    const isStatic = route.config.mode === "static";
    const type = isStatic ? "static" : "app";
    const hasParams = route.params.length > 0;
    const paramNote = hasParams ? " (has params)" : "";
    console.log(`  ${route.path} (${type})${paramNote}`);
  }
  if (hasIslands && appRouteCount === 0) {
    console.log("\n  (static routes with islands — client bundle will be emitted for hydration)");
  }
  console.log("");

  const loadedConfig = await loadConfigFromFile(
    { command: "build", mode: "production" },
    undefined,
    cwd
  );

  const userConfig = loadedConfig?.config || {};
  // A project that also registers neutronPlugin() in its own vite.config gets
  // two instances, and Vite's mergeConfig concatenates rather than replaces.
  // Worse, the project's copy is imported from ITS @neutron-build/core, which
  // is often an older version than this CLI's, so which one answers for the
  // route manifest decides real behaviour. dev.ts drops the duplicate; build
  // cannot follow until versions are aligned, because dropping it silently
  // switches a skewed project onto different route semantics. Say so instead.
  warnOnDuplicateCorePlugin(userConfig.plugins);

  // CSS Modules: pin scoped class names to a deterministic function of (local
  // name, file path). The static/app pages are pre-rendered by a Vite *dev* SSR
  // instance (createServer below) while their CSS is emitted by a separate Vite
  // *build* instance; Vite's default scoped-name hashing differs between the two,
  // so module classes in the HTML would not match any rule in the emitted CSS.
  // Forcing one deterministic name in every Vite instance keeps them in sync.
  const cwdFwd = cwd.replace(/\\/g, "/");
  const cssConfig = {
    modules: {
      generateScopedName(name: string, filename: string): string {
        const clean = filename.replace(/\?.*$/, "").replace(/\\/g, "/");
        const rel = clean.startsWith(cwdFwd + "/") ? clean.slice(cwdFwd.length + 1) : clean;
        const hash = createHash("sha256").update(`${rel}#${name}`).digest("hex").slice(0, 8);
        const safe = name.replace(/[^a-zA-Z0-9_-]/g, "_");
        return `_${safe}_${hash}`;
      },
    },
  };

  // Build client assets. For static-only sites (no app routes), create a
  // temporary CSS-only entry so Vite still extracts stylesheets without
  // requiring an index.html entry point. Static sites WITH islands take the
  // same branch: neutronPlugin only supplies a Rolldown entry for app routes,
  // so the app-bundle build below would fall back to Vite's default
  // `index.html` entry and die with UNRESOLVED_ENTRY. Their island code is
  // built by the dedicated islands pass further down, and going through the
  // CSS-extraction build is what lets route- and layout-level stylesheets
  // reach a prerendered islands page at all.
  if (appRouteCount > 0) {
    console.log("Building client bundle...");
    await viteBuild(
      mergeConfig(userConfig, {
        oxc: oxcJsx,
        configFile: false,
        root: cwd,
        plugins: [
          {
            name: "neutron:resolve-render-to-string-stream-css",
            enforce: "pre" as const,
            resolveId(id: string) {
              if (id === "preact-render-to-string/stream") {
                return createRequire(path.join(cwd, "package.json")).resolve(id);
              }
              return null;
            },
            load(id: string) {
              if (id.endsWith("/preact-render-to-string/stream")) {
                const real = createRequire(path.join(cwd, "package.json")).resolve(
                  "preact-render-to-string/stream"
                );
                return fs.readFileSync(real, "utf-8");
              }
              return null;
            },
          },
          neutronPlugin({ routesDir, rootDir: cwd, routeRules: neutronConfig.routes }),
        ],
        css: cssConfig,
        resolve: {
          alias: preactAliases,
          dedupe: ["preact", "preact/hooks", "preact/compat", "preact/jsx-runtime"],
        },
        build: {
          outDir: outputDir,
          emptyOutDir: true,
          // The manifest is how the client entry is identified. Without it the
          // only signal is the emitted filename, and a route named `index`
          // produces `assets/index-<hash>.js` — the exact shape of the real
          // entry. See extractClientEntryScriptSrc.
          manifest: true,
        },
      })
    );
  } else {
    console.log("Building CSS bundle (static site)...");
    const cssEntryDir = path.join(cwd, ".neutron");
    fs.mkdirSync(cssEntryDir, { recursive: true });
    const cssEntryPath = path.join(cssEntryDir, "_css-entry.js");
    // Import every route + layout MODULE through the client-route pipeline so Vite
    // walks the real import graph and extracts ALL reachable CSS — side-effect
    // imports, component-level CSS, AND CSS Modules — not just side-effect `*.css`
    // imports textually present in the route file. The ?CLIENT_ROUTE_QUERY suffix
    // makes neutronPlugin strip server-only code (loader/action) while keeping CSS
    // imports in build mode, exactly as the client bundle does. Root-relative paths
    // (`/src/...`) are used so Vite resolves them from the project root.
    // Namespace-import + export each module so its component graph is NOT
    // tree-shaken. Side-effect-only imports would keep `import "x.css"` lines but
    // drop value-imported CSS Modules (`import s from "x.module.css"`) once the
    // unused component that references them is shaken away.
    // Resource routes are skipped: they render no page and carry no CSS, and
    // their handlers are server code through and through.
    const importLines: string[] = [];
    const keepRefs: string[] = [];
    routes.filter((route) => !isResourceRoute(route)).forEach((route, i) => {
      const abs = route.file.replace(/\\/g, "/");
      const rel = abs.startsWith(cwdFwd + "/") ? abs.slice(cwdFwd.length) : "/@fs" + abs;
      importLines.push(`import * as __r${i} from ${JSON.stringify(rel + "?" + CLIENT_ROUTE_QUERY)};`);
      keepRefs.push(`__r${i}`);
    });
    fs.writeFileSync(
      cssEntryPath,
      importLines.join("\n") + `\nexport const __keep = [${keepRefs.join(", ")}];\n`
    );
    await viteBuild(
      mergeConfig(userConfig, {
        oxc: oxcJsx,
        configFile: false,
        root: cwd,
        plugins: [
          {
            name: "neutron:resolve-render-to-string-stream-css",
            enforce: "pre" as const,
            resolveId(id: string) {
              if (id === "preact-render-to-string/stream") {
                return createRequire(path.join(cwd, "package.json")).resolve(id);
              }
              return null;
            },
            load(id: string) {
              if (id.endsWith("/preact-render-to-string/stream")) {
                const real = createRequire(path.join(cwd, "package.json")).resolve(
                  "preact-render-to-string/stream"
                );
                return fs.readFileSync(real, "utf-8");
              }
              return null;
            },
          },
          {
            // This pass bundles each page's import graph as BROWSER code only to
            // find its CSS, and a static page may import server-only packages
            // (a native addon cannot be bundled at all — F-P1-2). Dependencies
            // are therefore external here, except modules that resolve to a
            // stylesheet: `import "pkg/style.css"` and packages whose entry is
            // CSS (`@fontsource/*`) are still collected. Nothing this pass
            // emits as JS ships.
            name: "neutron:css-pass-external-deps",
            enforce: "pre" as const,
            async resolveId(source: string, importer: string | undefined, options: Record<string, unknown>) {
              if (!importer || source.startsWith("\0")) return null;
              const resolved = await this.resolve(source, importer, { ...options, skipSelf: true });
              if (!resolved || resolved.external) return resolved;
              const file = resolved.id.replace(/\?.*$/, "").replace(/\\/g, "/");
              if (!file.includes("/node_modules/") || CSS_LANG_RE.test(file)) return resolved;
              return { id: source, external: true };
            },
          },
          neutronPlugin({ routesDir, rootDir: cwd, routeRules: neutronConfig.routes }),
        ],
        css: cssConfig,
        resolve: {
          alias: preactAliases,
          dedupe: ["preact", "preact/hooks", "preact/compat", "preact/jsx-runtime"],
        },
        build: {
          outDir: outputDir,
          emptyOutDir: true,
          // Not a lib build: lib mode inlines every url() asset as a data: URI
          // regardless of assetsInlineLimit (F-P1-3: every font face base64'd
          // into the one stylesheet). As a plain build, fonts and images are
          // emitted under assets/ with hashed names, by the same inline limit
          // as the app client build. The strict entry signature keeps each
          // route's exports, so CSS Modules referenced only by components are
          // not shaken away. JS lands at the top level for the cleanup below.
          rolldownOptions: {
            input: { "neutron-css": cssEntryPath },
            preserveEntrySignatures: "strict" as const,
            output: {
              entryFileNames: "[name].js",
              chunkFileNames: "[name]-[hash].js",
              assetFileNames: "assets/[name]-[hash][extname]",
            },
          },
          cssCodeSplit: false,
          // This build imports every route to walk their CSS, so it emits a
          // route chunk per route — including `assets/index-<hash>.js`. Without
          // the manifest naming them, the client-entry resolver can pick one up
          // and inject a stray route stub into pages that need no JS at all.
          manifest: true,
        },
      })
    );
    try { fs.unlinkSync(cssEntryPath); } catch {}
    // Remove the JS lib output from the CSS-extraction build — we only need
    // the extracted CSS. Preserve any JS files that were copied here from
    // publicDir (e.g. sw.js, service workers, third-party widgets) so user
    // assets aren't silently dropped by the CSS-extraction cleanup.
    const publicDir = userConfig.publicDir
      ? String(userConfig.publicDir)
      : path.join(cwd, "public");
    const publicJsFiles = new Set<string>();
    if (fs.existsSync(publicDir)) {
      for (const f of fs.readdirSync(publicDir)) {
        if (f.endsWith(".js") || f.endsWith(".mjs")) publicJsFiles.add(f);
      }
    }
    for (const f of fs.readdirSync(outputDir)) {
      if ((f.endsWith(".mjs") || f.endsWith(".js")) && !publicJsFiles.has(f)) {
        try { fs.unlinkSync(path.join(outputDir, f)); } catch {}
      }
    }
  }

  // Only the app client build produces a client entry. The static CSS pass's
  // manifest also names a JS entry, but that file is deleted right after it is
  // built, so recording it pointed pages at a 404.
  const clientEntryScriptSrc = appRouteCount > 0 ? extractClientEntryScriptSrc(outputDir) : null;
  if (clientEntryScriptSrc) {
    writeClientEntryMetadata(outputDir, clientEntryScriptSrc);
  }
  // The SSR server answers root URLs from dist only for files listed here.
  writePublicFilesMetadata(
    outputDir,
    userConfig.publicDir === false
      ? null
      : userConfig.publicDir
        ? path.resolve(cwd, String(userConfig.publicDir))
        : path.join(cwd, "public")
  );

  // Standalone islands entry: a SEPARATE Rolldown build (so the SPA/app client
  // bundle stays byte-identical). It imports only the island runtime + the
  // virtual islands manifest — never the SPA runtime — so Rolldown code-splits
  // each island into its own chunk. Prerendered static pages with islands
  // reference THIS entry instead of the 34KB index-*.js.
  if (hasIslands) {
    await viteBuild(
      mergeConfig(userConfig, {
        oxc: oxcJsx,
        configFile: false,
        root: cwd,
        plugins: [neutronPlugin({ routesDir, rootDir: cwd, routeRules: neutronConfig.routes })],
        css: cssConfig,
        resolve: {
          alias: preactAliases,
          dedupe: ["preact", "preact/hooks", "preact/compat", "preact/jsx-runtime"],
        },
        build: {
          outDir: outputDir,
          // Do NOT empty the output — the SPA/app client bundle is already there.
          emptyOutDir: false,
          rolldownOptions: {
            input: { "neutron-islands": "@neutron-build/core/client/islands-entry" },
            output: {
              entryFileNames: "assets/[name]-[hash].js",
              chunkFileNames: "assets/[name]-[hash].js",
              assetFileNames: "assets/[name]-[hash][extname]",
            },
          },
        },
      })
    );
  }

  // Standalone islands entry chunk (tiny runtime + per-island code-split chunks).
  // Prerendered static pages with islands reference this instead of the SPA runtime.
  const islandsEntryScriptSrc = hasIslands ? extractIslandsEntryScriptSrc(outputDir) : null;

  // Collect CSS files produced by the client build for injection into static HTML
  const clientCssFiles = extractClientCssFiles(outputDir);

  const ensureRuntimeBundle = createRuntimeBundleBuilder({
    cwd,
    outputDir,
    routesDir,
    routeRules: neutronConfig.routes,
    routes,
    pageRoutes,
    clientEntryScriptSrc,
    clientCssFiles,
    userConfig,
    runtimeAliases: preactAliases,
    // `true` for react-compat (blanket processing — see resolveRuntimeNoExternal).
    runtimeNoExternal:
      runtimeNoExternal === true ? true : [...preactSsr.noExternal, ...runtimeNoExternal],
  });

  // Create a Vite SSR server for rendering
  const server = await createServer(
    mergeConfig(userConfig, {
      oxc: oxcJsx,
      configFile: false,
      root: cwd,
      plugins: [neutronPlugin({ routesDir, rootDir: cwd, routeRules: neutronConfig.routes })],
      css: cssConfig,
      resolve: {
        // Absolute aliases force one preact + RTS into the SSR graph even when
        // the app does not declare preact-render-to-string (pnpm isolation).
        alias: preactAliases,
        dedupe: ["preact", "preact/hooks", "preact/jsx-runtime", "preact/compat"],
      },
      ssr: {
        // Process these through Vite's SSR graph (aliased + noExternal) instead
        // of native node resolution. @neutron-build/core MUST share this graph
        // too: its inline components (e.g. Link) use hooks. react-compat goes
        // further and processes EVERYTHING (`true`): externalized deps resolve
        // their own react imports natively — real React next to preact (see
        // resolveRuntimeNoExternal). Core's CJS-only deps stay native, as in
        // dev and start: the runner cannot evaluate CommonJS, and a static
        // page that reaches core's root entry otherwise dies with "require is
        // not defined" (RUNTIME_RUNNER_CJS_EXTERNALS).
        noExternal:
          runtimeNoExternal === true ? true : [...preactSsr.noExternal, ...runtimeNoExternal],
        ...(runtimeNoExternal === true
          ? { external: [...RUNTIME_RUNNER_CJS_EXTERNALS] }
          : {}),
      },
      server: {
        middlewareMode: true,
        hmr: false,
        ws: false,
        // Allow loading the framework-provided renderer from outside the app root.
        fs: { strict: false },
      },
      optimizeDeps: {
        noDiscovery: true,
      },
      appType: "custom",
    })
  );

  // Resolve the renderer through the SAME Vite SSR server that loads the route
  // modules. No native fallback: a second Preact options object is exactly the
  // dual-instance __H crash. Aliases above make these resolvable.
  const appPreact = (await server.ssrLoadModule("preact")) as { h?: typeof h };
  const appRts = (await server.ssrLoadModule("preact-render-to-string")) as {
    renderToString?: typeof renderToString;
    default?: { renderToString?: typeof renderToString };
  };
  const appH = appPreact?.h;
  const appRender = appRts?.renderToString ?? appRts?.default?.renderToString;
  if (!appH || !appRender) {
    throw new Error(
      "[Neutron] Failed to load preact / preact-render-to-string through the Vite SSR graph. " +
        "Install preact in the app and ensure @neutron-build/core is installed."
    );
  }

  // Router providers from the SAME graph as the route modules, as the start
  // server does (loadRunnerRenderer): a Provider and a consumer from two module
  // instances silently read the context default — `useLocation()` reporting
  // "/" on every prerendered page. Falls back to the CLI's own copy.
  let appWithRouterProviders = withRouterProviders;
  try {
    const appCore = (await server.ssrLoadModule("@neutron-build/core")) as {
      withRouterProviders?: typeof withRouterProviders;
    };
    if (appCore.withRouterProviders) appWithRouterProviders = appCore.withRouterProviders;
  } catch {
    // keep the fallback
  }
  // Same provider values the request-serving renderer mounts.
  const mountRouterProviders = (
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    element: any,
    route: Route,
    pathname: string,
    params: Record<string, string>,
    loaderData: Record<string, unknown>
  ) =>
    appWithRouterProviders(appH as CreateElement, element, {
      routeId: route.id,
      pathname,
      search: "",
      params,
      loaderData,
      actionData: undefined,
    });

  // Get layouts map
  const layouts = new Map<string, Route>();
  for (const route of routes) {
    if (route.file.includes("_layout")) {
      layouts.set(route.id, route);
    }
  }
  const moduleCache = new Map<string, Promise<RouteModule>>();
  const staticHeadersByRoute: Record<string, Record<string, string>> = {};

  function getLayoutChain(route: Route): Route[] {
    const chain: Route[] = [];
    let currentId: string | null = route.parentId;

    while (currentId) {
      const parent = layouts.get(currentId);
      if (parent) {
        chain.push(parent);
        currentId = parent.parentId;
      } else {
        break;
      }
    }

    return chain;
  }

  async function loadRouteModule(route: Route): Promise<RouteModule> {
    let pending = moduleCache.get(route.file);
    if (!pending) {
      pending = server.ssrLoadModule(route.file).then((loaded) => loaded as RouteModule);
      moduleCache.set(route.file, pending);
    }
    return pending;
  }

  async function resolveRouteHeaders(
    route: Route,
    layoutChain: Route[],
    request: Request,
    context: AppContext,
    params: Record<string, string>,
    loaderData: unknown
  ): Promise<Record<string, string>> {
    const allRoutes = [...layoutChain].reverse();
    allRoutes.push(route);

    const loaderDataMap: Record<string, unknown> = {};
    if (loaderData !== undefined) {
      loaderDataMap[route.id] = loaderData;
    }

    const merged = new Headers();
    for (const currentRoute of allRoutes) {
      const currentModule = await loadRouteModule(currentRoute);
      if (!currentModule.headers) {
        continue;
      }

      const args: HeadersArgs = {
        request,
        params,
        context,
        loaderData: loaderDataMap,
      };
      const resolved = normalizeHeaders(await currentModule.headers(args));
      for (const [name, value] of Object.entries(resolved)) {
        merged.set(name, value);
      }
    }

    return headersToRecord(merged);
  }

  async function resolveRouteHeadHtml(
    route: Route,
    layoutChain: Route[],
    request: Request,
    context: AppContext,
    params: Record<string, string>,
    loaderData: unknown,
    pathname: string
  ): Promise<{ headHtml: string; seo: SeoMetaInput | null }> {
    const allRoutes = [...layoutChain].reverse();
    allRoutes.push(route);

    const loaderDataMap: Record<string, unknown> = {};
    if (loaderData !== undefined) {
      loaderDataMap[route.id] = loaderData;
    }

    let mergedSeo: SeoMetaInput | null = null;
    const headFragments: string[] = [];

    for (const currentRoute of allRoutes) {
      const currentModule = await loadRouteModule(currentRoute);
      if (!currentModule.head) {
        continue;
      }

      const args: HeadArgs = {
        request,
        params,
        context,
        loaderData: loaderDataMap,
        pathname,
      };
      // Routes use `head({ data })` — provide `data` as an alias for
      // the current route's loader data so destructuring works.
      const headArgsWithData = {
        ...args,
        data: loaderDataMap[currentRoute.id] ?? loaderData,
      };
      const resolved = await currentModule.head(headArgsWithData);
      if (!resolved) {
        continue;
      }

      if (typeof resolved === "string") {
        headFragments.push(resolved);
        continue;
      }

      mergedSeo = mergeSeoMetaInput(mergedSeo, resolved);
    }

    return {
      headHtml: renderDocumentHead(pathname, mergedSeo, headFragments),
      seo: mergedSeo,
    };
  }

  // Render static routes
  console.log("\nRendering static routes...");
  
  const staticRoutes = pageRoutes.filter((r) => r.config.mode === "static");
  let renderedCount = 0;
  let skippedCount = 0;
  // Routes that threw while rendering. Distinct from skips (no component, no
  // getStaticPaths): a skip is a choice, a render error is a broken page, and
  // a build that shipped one must not report success.
  let renderErrorCount = 0;

  // A-020 (build side): a prerendered file answers before middleware can
  // run, so `mode: "static"` + middleware would publish a gated page. Core's
  // renderStatic refuses the combination for its own renderer; the production
  // build refuses it here, before prerendering any page. Resource routes are
  // exempt — never prerendered, their gates run per request.
  const gatedStatic = findGatedStaticRoutes(routes);
  if (gatedStatic.length > 0) {
    console.error(`\n${gatedStaticRoutesError(gatedStatic)}`);
    process.exit(1);
  }

  for (const route of staticRoutes) {
    // Resource routes (method handlers, no component) are endpoints served
    // per request — never prerendered, and never baked via a stray `loader`
    // export (a resource route's loader never runs at serve time either, so
    // a file baked from it could never be served).
    if (isResourceRoute(route)) {
      console.log(`  Skipping ${route.path} (resource route — served per request)`);
      skippedCount++;
      continue;
    }
    try {
      const module = await loadRouteModule(route);

      if (!module?.default) {
        // Dynamic resource route: getStaticPaths + a loader returning a raw
        // Response, no component (e.g. per-page .md sources at /docs/x.md).
        // Iterate the paths and bake each response to its resolved literal
        // path. Only matches this specific shape, so it can't affect the
        // no-param resource routes or component routes handled below.
        if (route.params.length > 0 && module?.getStaticPaths && module?.loader) {
          const result = await module.getStaticPaths();
          const pathList = Array.isArray(result)
            ? result
            : (result as GetStaticPathsResult).paths;
          for (const { params } of pathList) {
            const resolvedPath = resolvePath(route.path, params);
            assertSafeResolvedPath(route.path, resolvedPath);
            const request = new Request("http://localhost" + resolvedPath);
            let response: Response | undefined;
            try {
              const r = await module.loader({ request, params, context: {} } as LoaderArgs);
              if (isResponse(r)) response = r;
            } catch (error) {
              if (isResponse(error)) response = error;
              else throw error;
            }
            if (response) {
              const body = Buffer.from(await response.arrayBuffer());
              const outPath = getResourceOutputPath(outputDir, resolvedPath);
              fs.mkdirSync(path.dirname(outPath), { recursive: true });
              fs.writeFileSync(outPath, body);
              console.log(`  ${resolvedPath} → ${path.relative(outputDir, outPath)}`);
              renderedCount++;
            }
          }
          continue;
        }

        // Resource route: no component to render, but a GET loader that
        // returns a raw Response (sitemap.xml, rss feeds, JSON endpoints,
        // ...) can still be baked to a static file at its literal path.
        if (module?.loader) {
          const request = new Request("http://localhost" + route.path);
          let response: Response | undefined;
          try {
            const result = await module.loader({ request, params: {}, context: {} } as LoaderArgs);
            if (isResponse(result)) response = result;
          } catch (error) {
            if (isResponse(error)) response = error;
            else throw error;
          }

          if (response) {
            const body = Buffer.from(await response.arrayBuffer());
            const outPath = getResourceOutputPath(outputDir, route.path);
            fs.mkdirSync(path.dirname(outPath), { recursive: true });
            fs.writeFileSync(outPath, body);
            console.log(`  ${route.path} → ${path.relative(outputDir, outPath)}`);
            renderedCount++;
            continue;
          }
        }

        console.log(`  Skipping ${route.path} (no component)`);
        skippedCount++;
        continue;
      }

      // Handle dynamic routes with getStaticPaths
      if (route.params.length > 0) {
        if (!module.getStaticPaths) {
          console.log(`  Skipping ${route.path} (needs getStaticPaths export)`);
          skippedCount++;
          continue;
        }

        // Get all paths to render — supports both array and { paths: [] } forms
        const result = await module.getStaticPaths();
        const pathList = Array.isArray(result) ? result : (result as GetStaticPathsResult).paths;

        for (const { params, props } of pathList) {
          // Build the actual path by substituting params
          const resolvedPath = resolvePath(route.path, params);
          assertSafeResolvedPath(route.path, resolvedPath);
          const context: AppContext = {};
          const request = new Request("http://localhost" + resolvedPath);

          // Call the route's loader (same as static routes) so content,
          // TOC, pagination, etc. are available. Fall back to props from
          // getStaticPaths if there is no loader.
          let loaderData: unknown = props || {};
          if (module.loader) {
            loaderData = await module.loader({
              request,
              params,
              context,
            } as LoaderArgs);
          }

          const layoutChain = getLayoutChain(route);

          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          let element: any = appH(module.default as any, {
            data: loaderData,
            params,
          });

          const loaderDataByRoute: Record<string, unknown> = { [route.id]: loaderData };
          for (const layoutRoute of layoutChain) {
            const layoutModule = await loadRouteModule(layoutRoute);
            // Call layout loaders so sidebars, nav trees, etc. are populated.
            let layoutData: unknown = {};
            if (layoutModule?.loader) {
              layoutData = await layoutModule.loader({
                request,
                params,
                context,
              } as LoaderArgs);
              loaderDataByRoute[layoutRoute.id] = layoutData;
            }
            if (layoutModule?.default) {
              element = appH(layoutModule.default as any, { data: layoutData }, element);
            }
          }
          element = mountRouterProviders(element, route, resolvedPath, params, loaderDataByRoute);

          const html = appRender(element);
          // Mounted inside the shell's `<div id="app">` (wrapHtml owns the
          // document). A full-document render would nest a second document
          // inside #app — reject it before the page is written.
          assertRenderedFragment(html, layoutChain[0]?.file ?? route.file);
          const { headHtml, seo } = await resolveRouteHeadHtml(
            route,
            layoutChain,
            request,
            context,
            params,
            loaderData,
            resolvedPath
          );
          const fullHtml = wrapHtml(
            html,
            resolvedPath,
            loaderData,
            clientEntryScriptSrc,
            headHtml,
            clientCssFiles,
            islandsEntryScriptSrc,
            seo
          );

          const outPath = getOutputPath(outputDir, resolvedPath);
          fs.mkdirSync(path.dirname(outPath), { recursive: true });
          fs.writeFileSync(outPath, fullHtml);

          const routeHeaders = await resolveRouteHeaders(
            route,
            layoutChain,
            request,
            context,
            params,
            loaderData
          );
          if (Object.keys(routeHeaders).length > 0) {
            staticHeadersByRoute[resolvedPath] = routeHeaders;
          }

          console.log(`  ${resolvedPath} → ${path.relative(outputDir, outPath)}`);
          renderedCount++;
        }
        continue;
      }

      // Static route without params
      const context: AppContext = {};
      const request = new Request("http://localhost" + route.path);
      let loaderData: unknown = undefined;
      if (module.loader) {
        loaderData = await module.loader({
          request,
          params: {},
          context,
        } as LoaderArgs);
      }

      const layoutChain = getLayoutChain(route);

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      let element: any = appH(module.default as any, {
        data: loaderData,
        params: {},
      });

      const loaderDataByRoute: Record<string, unknown> =
        loaderData !== undefined ? { [route.id]: loaderData } : {};
      for (const layoutRoute of layoutChain) {
        const layoutModule = await loadRouteModule(layoutRoute);
        let layoutData: unknown = {};
        if (layoutModule?.loader) {
          layoutData = await layoutModule.loader({
            request,
            params: {},
            context,
          } as LoaderArgs);
          loaderDataByRoute[layoutRoute.id] = layoutData;
        }
        if (layoutModule?.default) {
          element = appH(layoutModule.default as any, { data: layoutData }, element);
        }
      }
      element = mountRouterProviders(element, route, route.path, {}, loaderDataByRoute);

      const html = appRender(element);
      // Mounted inside the shell's `<div id="app">` (wrapHtml owns the
      // document). A full-document render would nest a second document inside
      // #app — reject it before the page is written.
      assertRenderedFragment(html, layoutChain[0]?.file ?? route.file);
      const { headHtml, seo } = await resolveRouteHeadHtml(
        route,
        layoutChain,
        request,
        context,
        {},
        loaderData,
        route.path
      );
      const fullHtml = wrapHtml(
        html,
        route.path,
        loaderData,
        clientEntryScriptSrc,
        headHtml,
        clientCssFiles,
        islandsEntryScriptSrc,
        seo
      );

      const outPath = getOutputPath(outputDir, route.path);
      fs.mkdirSync(path.dirname(outPath), { recursive: true });
      fs.writeFileSync(outPath, fullHtml);

      const routeHeaders = await resolveRouteHeaders(
        route,
        layoutChain,
        request,
        context,
        {},
        loaderData
      );
      if (Object.keys(routeHeaders).length > 0) {
        staticHeadersByRoute[route.path] = routeHeaders;
      }

      console.log(`  ${route.path} → ${path.relative(outputDir, outPath)}`);
      renderedCount++;
    } catch (error) {
      console.error(`  Error rendering ${route.path}:`, error);
      renderErrorCount++;
    }
  }

  if (Object.keys(staticHeadersByRoute).length > 0) {
    writeStaticHeadersMetadata(outputDir, staticHeadersByRoute);
  }

  // Allow pending module processing to settle before closing middleware server.
  await new Promise((resolve) => setTimeout(resolve, 50));
  await server.close();

  await buildStartServerBundle({
    cwd,
    outputDir,
    routesDir,
    routeRules: neutronConfig.routes,
    routes,
    userConfig,
    runtimeAliases: preactAliases,
    runtimeNoExternal:
      runtimeNoExternal === true ? true : [...preactSsr.noExternal, ...runtimeNoExternal],
    runtime,
    oxc: oxcJsx,
  });

  if (selectedAdapter) {
    console.log(`\nRunning adapter: ${selectedAdapter.name}`);
    await selectedAdapter.adapt({
      rootDir: cwd,
      outDir: outputDir,
      routes: {
        total: pageRoutes.length,
        static: staticRouteCount,
        app: appRouteCount,
      },
      clientEntryScriptSrc,
      ensureRuntimeBundle,
      log: (message: string) => {
        console.log(`  [adapter] ${message}`);
      },
    } as any);
  }

  console.log(`\nRendered ${renderedCount} pages, skipped ${skippedCount}.`);
  if (renderErrorCount > 0) {
    console.error(
      `\nBuild failed: ${renderErrorCount} route${renderErrorCount === 1 ? "" : "s"} threw while rendering ` +
        `(see "Error rendering" above). The output in ${outputDir} is incomplete.`
    );
    process.exit(1);
  }
  console.log(`\nBuild complete!`);
  console.log(`Output: ${outputDir}`);
}

/**
 * Resolve a route pattern with params to an actual path — see
 * commands/static-paths.ts (tested from source there).
 */

/** Security gate for every param-derived static write: reject traversal. */
// Vite's stylesheet languages (CSS_LANGS_RE in vite/src/node/constants.ts).
const CSS_LANG_RE = /\.(css|less|sass|scss|styl|stylus|pcss|postcss|sss)$/;

function assertSafeResolvedPath(routePath: string, resolvedPath: string): void {
  if (isUnsafeResolvedPath(resolvedPath)) {
    throw new Error(
      `Route ${routePath}: getStaticPaths params resolved to "${resolvedPath}", ` +
        "which escapes the output directory. Refusing to write outside dist/."
    );
  }
}

function wrapHtml(
  content: string,
  routePath: string,
  _loaderData?: unknown,
  clientEntryScriptSrc: string | null = null,
  headHtml: string = renderDocumentHead(routePath, null),
  cssFiles: string[] = [],
  islandsEntryScriptSrc: string | null = null,
  seo: SeoMetaInput | null = null
): string {
  // Detect islands in content — only load client runtime if interactive islands exist
  const hasIslands = content.includes("<neutron-island");
  // Prefer the standalone islands entry (tiny runtime + per-island code-split
  // chunks) over the full 34KB SPA runtime. Fall back to the SPA runtime only
  // if no islands entry was emitted.
  const islandScriptSrc = islandsEntryScriptSrc || clientEntryScriptSrc;
  const clientScript = hasIslands && islandScriptSrc
    ? `<script type="module" src="${escapeHtml(islandScriptSrc)}"></script>`
    : "";

  const cssLinks = cssFiles
    .map((href) => `<link rel="stylesheet" href="${escapeHtml(href)}">`)
    .join("\n");

  return `<!DOCTYPE html>
${buildHtmlOpenTag(seo?.htmlAttrs)}
<head>
${headHtml}
${cssLinks}
</head>
${buildBodyOpenTag(seo?.bodyAttrs)}
<div id="app">${content}</div>
${clientScript}
${renderSpeculationRules()}
</body>
</html>`;
}

function escapeHtml(str: string): string {
  return str
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function getOutputPath(outputDir: string, routePath: string): string {
  if (routePath === "/") {
    return path.join(outputDir, "index.html");
  }

  const cleanPath = routePath.replace(/\/$/, "");
  return path.join(outputDir, cleanPath, "index.html");
}

// Resource routes serve a specific file (sitemap.xml, rss.xml, ...), not an
// HTML page — write to the literal path, not <path>/index.html.
function getResourceOutputPath(outputDir: string, routePath: string): string {
  return path.join(outputDir, routePath.replace(/^\//, ""));
}

function extractClientCssFiles(outputDir: string): string[] {
  const assetsDir = path.join(outputDir, "assets");
  if (!fs.existsSync(assetsDir)) return [];
  return fs
    .readdirSync(assetsDir)
    .filter((name) => name.endsWith(".css"))
    .map((name) => `/assets/${name}`);
}

/**
 * Find the standalone islands entry chunk emitted by the client build. Named
 * input "neutron-islands" → Rolldown emits `assets/neutron-islands-[hash].js`.
 */
function extractIslandsEntryScriptSrc(outputDir: string): string | null {
  const assetsDir = path.join(outputDir, "assets");
  if (!fs.existsSync(assetsDir)) return null;
  const candidates = fs
    .readdirSync(assetsDir)
    .filter((name) => name.startsWith("neutron-islands-") && name.endsWith(".js"))
    .sort();
  if (candidates.length === 0) return null;
  return `/assets/${candidates[candidates.length - 1]}`;
}


function writeClientEntryMetadata(outputDir: string, src: string): void {
  const metadataPath = path.join(outputDir, ".neutron-client-entry.json");
  fs.writeFileSync(metadataPath, JSON.stringify({ src }, null, 2));
}

function normalizeHeaders(
  value: Headers | Record<string, string> | null | undefined
): Record<string, string> {
  if (!value) {
    return {};
  }

  if (value instanceof Headers) {
    return headersToRecord(value);
  }

  const output: Record<string, string> = {};
  for (const [name, headerValue] of Object.entries(value)) {
    const lower = name.toLowerCase();
    if (lower === "content-length" || lower === "set-cookie") {
      continue;
    }
    output[name] = String(headerValue);
  }
  return output;
}

function headersToRecord(headers: Headers): Record<string, string> {
  const output: Record<string, string> = {};
  headers.forEach((value, name) => {
    const lower = name.toLowerCase();
    if (lower === "content-length" || lower === "set-cookie") {
      return;
    }
    output[name] = value;
  });
  return output;
}

function writeStaticHeadersMetadata(
  outputDir: string,
  headersByRoute: Record<string, Record<string, string>>
): void {
  const metadataPath = path.join(outputDir, ".neutron-static-headers.json");
  fs.writeFileSync(metadataPath, JSON.stringify(headersByRoute, null, 2));
}

interface BuildArgs {
  preset: "vercel" | "cloudflare" | "docker" | "netlify" | "static" | null;
  cloudflareMode: "pages" | "workers";
}

interface RuntimeBundleBuilderOptions {
  cwd: string;
  outputDir: string;
  routesDir: string;
  routeRules?: NeutronConfig["routes"];
  routes: Route[];
  pageRoutes: Route[];
  clientEntryScriptSrc: string | null;
  clientCssFiles: string[];
  userConfig: Record<string, any>;
  runtimeAliases?: Array<{ find: string; replacement: string }> | Record<string, string>;
  /** `true` = process the whole dependency graph (react-compat). */
  runtimeNoExternal?: string[] | true;
}

interface RuntimeBundleArtifact {
  target: "node" | "worker";
  outDir: string;
  entryPath: string;
  entryRelativePath: string;
}

interface RuntimeRouteDef {
  id: string;
  path: string;
  parentId: string | null;
  params: string[];
  mode: "static" | "app";
  cache?: {
    maxAge?: number;
    loaderMaxAge?: number;
  };
  isLayout: boolean;
  file: string;
}

function parseBuildArgs(argv: string[]): BuildArgs {
  let preset: BuildArgs["preset"] = null;
  let cloudflareMode: BuildArgs["cloudflareMode"] = "pages";

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--preset" && argv[i + 1]) {
      const value = argv[++i];
      if (value === "vercel" || value === "cloudflare" || value === "docker" || value === "netlify" || value === "static") {
        preset = value;
      }
      continue;
    }
    if (arg.startsWith("--preset=")) {
      const value = arg.split("=")[1];
      if (value === "vercel" || value === "cloudflare" || value === "docker" || value === "netlify" || value === "static") {
        preset = value;
      }
      continue;
    }
    if (arg === "--cloudflare-mode" && argv[i + 1]) {
      const value = argv[++i];
      if (value === "pages" || value === "workers") {
        cloudflareMode = value;
      }
      continue;
    }
    if (arg.startsWith("--cloudflare-mode=")) {
      const value = arg.split("=")[1];
      if (value === "pages" || value === "workers") {
        cloudflareMode = value;
      }
    }
  }

  return { preset, cloudflareMode };
}

function resolveAdapterForBuild(
  config: NeutronConfig,
  args: BuildArgs
): NeutronAdapter | undefined {
  if (args.preset === "vercel") {
    return adapterVercel();
  }
  if (args.preset === "netlify") {
    return adapterNetlify();
  }
  if (args.preset === "cloudflare") {
    return adapterCloudflare({ mode: args.cloudflareMode });
  }
  if (args.preset === "docker") {
    return adapterDocker();
  }
  if (args.preset === "static") {
    return adapterStatic();
  }
  return config.adapter;
}

function createRuntimeBundleBuilder(
  options: RuntimeBundleBuilderOptions
): (target: RuntimeBundleArtifact["target"]) => Promise<RuntimeBundleArtifact> {
  const pending = new Map<
    RuntimeBundleArtifact["target"],
    Promise<RuntimeBundleArtifact>
  >();

  return async (target: RuntimeBundleArtifact["target"]): Promise<RuntimeBundleArtifact> => {
    let bundle = pending.get(target);
    if (!bundle) {
      bundle = buildRuntimeBundle(options, target);
      pending.set(target, bundle);
    }
    return bundle;
  };
}

async function buildRuntimeBundle(
  options: RuntimeBundleBuilderOptions,
  target: RuntimeBundleArtifact["target"]
): Promise<RuntimeBundleArtifact> {
  const appRoutes = options.pageRoutes.filter((route) => route.config.mode === "app");
  if (appRoutes.length === 0) {
    throw new Error(`No app routes found; cannot build ${target} runtime bundle.`);
  }

  const runtimeRoutes = collectRuntimeRoutes(options.routes, appRoutes);
  const runtimeDir = path.join(options.cwd, ".neutron", "runtime");
  fs.mkdirSync(runtimeDir, { recursive: true });

  const entryPath = path.join(runtimeDir, `entry.${target}.ts`);
  // Optional global middleware: <cwd>/src/middleware.{ts,tsx,js,mjs}.
  const globalMiddlewarePath = [
    "src/middleware.ts",
    "src/middleware.tsx",
    "src/middleware.js",
    "src/middleware.mjs",
  ]
    .map((rel) => path.join(options.cwd, rel))
    .find((abs) => fs.existsSync(abs));
  fs.writeFileSync(
    entryPath,
    generateRuntimeEntrySource(
      runtimeRoutes,
      appRoutes,
      options.clientEntryScriptSrc,
      options.clientCssFiles,
      entryPath,
      options.routeRules,
      globalMiddlewarePath
    ),
    "utf-8"
  );

  const bundleOutDir = path.join(options.outputDir, "server", target);
  await bundleServerEntry(options, {
    entryPath,
    outDir: bundleOutDir,
    target: target === "worker" ? "webworker" : "node",
    chunkFileNames: "chunks/[name]-[hash].js",
  });

  const entryRelativePath = path.relative(options.outputDir, path.join(bundleOutDir, "entry.js"));
  return {
    target,
    outDir: bundleOutDir,
    entryPath: path.join(bundleOutDir, "entry.js"),
    entryRelativePath: entryRelativePath.split(path.sep).join("/"),
  };
}

/**
 * One SSR (Rolldown) build of a generated server entry, in the module graph
 * every server bundle uses: the app's Vite config, the runtime's preact
 * aliases, and preact + preact-render-to-string + @neutron-build/core
 * processed INTO the bundle (react-compat: everything), so the renderer, the
 * router providers and the routes' hooks are one Preact instance whatever
 * the install shape.
 */
async function bundleServerEntry(
  options: Pick<
    RuntimeBundleBuilderOptions,
    "cwd" | "routesDir" | "routeRules" | "userConfig" | "runtimeAliases" | "runtimeNoExternal"
  > & { oxc?: unknown },
  build: {
    entryPath: string;
    outDir: string;
    target: "node" | "webworker";
    chunkFileNames: string;
    sourcemap?: boolean;
    plugins?: unknown[];
    external?: string[];
  }
): Promise<void> {
  const entryPath = build.entryPath;
  const bundleOutDir = build.outDir;
  await viteBuild(
    mergeConfig(options.userConfig, {
      configFile: false,
      root: options.cwd,
      ...(options.oxc ? { oxc: options.oxc } : {}),
      plugins: [
        ...(build.plugins ?? []),
        // Vite's SSR resolver does not honor the `exports` map for the
        // dynamic-import subpath `preact-render-to-string/stream` inside the
        // noExternal @neutron-build/core dep — it falls back to a raw
        // <pkgdir>/stream path (ENOENT), breaking the docker/node runtime
        // bundle (and thus streaming SSR in production). This resolveId hook
        // runs before the default resolver and returns the real file via
        // Node's own exports resolution. Only this one subpath tripped it.
        {
          name: "neutron:resolve-render-to-string-stream",
          enforce: "pre" as const,
          resolveId(id: string) {
            if (id === "preact-render-to-string/stream") {
              // Return the real file so Rolldown bundles it (Vite's SSR
              // noExternal resolver otherwise yields a raw <pkgdir>/stream
              // path that fails to load — docker/node preset breakage).
              return createRequire(path.join(options.cwd, "package.json")).resolve(id);
            }
            return null;
          },
          load(id: string) {
            // Backstop: if some earlier resolver already produced the raw
            // `.../preact-render-to-string/stream` path (no extension), Vite's
            // load-fallback ENOENTs on it. Serve the real module contents.
            if (id.endsWith("/preact-render-to-string/stream")) {
              const real = createRequire(path.join(options.cwd, "package.json")).resolve(
                "preact-render-to-string/stream"
              );
              return fs.readFileSync(real, "utf-8");
            }
            return null;
          },
        },
        neutronPlugin({
          routesDir: options.routesDir,
          rootDir: options.cwd,
          routeRules: options.routeRules,
        }),
      ],
      resolve: {
        ...(options.runtimeAliases ? { alias: options.runtimeAliases } : {}),
        dedupe: ["preact", "preact/hooks", "preact/compat", "preact/jsx-runtime"],
      },
      ssr: {
        target: build.target,
        ...(build.external ? { external: build.external } : {}),
        // Bundle @neutron-build/core with the same preact the renderer uses, so
        // inline hook components (e.g. Link) in app-mode routes don't hit the
        // two-preact-instance "__H" crash at runtime — matching the build/dev paths.
        // react-compat passes `true` here (resolveRuntimeNoExternal): every dep
        // that imports react must be IN the bundle, or the bundle imports real
        // React at runtime and SSR crashes. Escape hatch: ssr.external in the
        // app's vite.config re-externalizes specific packages.
        noExternal:
          options.runtimeNoExternal === true
            ? true
            : [
                "preact",
                "preact/hooks",
                "preact-render-to-string",
                "@neutron-build/core",
                ...(options.runtimeNoExternal || []),
              ],
      },
      build: {
        ssr: entryPath,
        outDir: bundleOutDir,
        emptyOutDir: true,
        ...(build.sourcemap ? { sourcemap: true } : {}),
        rolldownOptions: {
          output: {
            format: "esm",
            entryFileNames: "entry.js",
            chunkFileNames: build.chunkFileNames,
            assetFileNames: "assets/[name]-[hash][extname]",
          },
        },
      },
    })
  );
}

/**
 * The `neutron-ts start` server bundle (F-P2-1). `createServer` otherwise
 * builds a Vite SSR runtime at every boot and transforms each route module,
 * layout and the global middleware on first use. This compiles exactly that
 * set — every discovered route file, since any of them can be rendered at
 * runtime (gated static routes, not-found pages, a static path missing from
 * dist), plus src/middleware.* — into dist/server/start/, and records it in
 * dist/.neutron-server-bundle.json, which createServer loads natively.
 *
 * Built only when the server would start an SSR runtime at all (app routes,
 * resource routes or global middleware); a static-only site has nothing to
 * load. A failure here does not fail the build: start then falls back to the
 * Vite runtime, as it did before bundles existed, and says so at boot.
 */
async function buildStartServerBundle(
  options: Pick<
    RuntimeBundleBuilderOptions,
    "cwd" | "outputDir" | "routesDir" | "routeRules" | "routes" | "userConfig" | "runtimeAliases" | "runtimeNoExternal"
  > & { runtime: string; oxc: unknown }
): Promise<void> {
  const manifestPath = path.join(options.outputDir, SERVER_BUNDLE_MANIFEST);
  fs.rmSync(manifestPath, { force: true });

  const globalMiddlewareFile = findGlobalMiddlewareFile(options.cwd);
  const pages = options.routes.filter((route) => !route.file.includes("_layout"));
  const needsRuntime =
    globalMiddlewareFile !== null ||
    pages.some((route) => route.config.mode === "app" || isResourceRoute(route));
  if (!needsRuntime) {
    return;
  }

  const runtimeDir = path.join(options.cwd, ".neutron", "runtime");
  fs.mkdirSync(runtimeDir, { recursive: true });
  const entryPath = path.join(runtimeDir, "entry.start.ts");
  const files = [
    ...options.routes.map((route) => route.file),
    ...(globalMiddlewareFile ? [globalMiddlewareFile] : []),
  ];
  fs.writeFileSync(
    entryPath,
    generateServerBundleEntry({
      entryPath,
      rootDir: options.cwd,
      files,
      runtime: options.runtime as "preact" | "react-compat",
    }),
    "utf-8"
  );

  const outDir = path.join(options.outputDir, "server", "start");
  const externals = importerResolvedExternals(outDir);
  console.log("\nBuilding server bundle...");
  try {
    await bundleServerEntry(options, {
      entryPath,
      outDir,
      target: "node",
      // Flat: every chunk sits in outDir, so a source file's location
      // relative to the chunk that holds it is a build-time constant.
      chunkFileNames: "[name]-[hash].js",
      sourcemap: true,
      plugins: [pinImportMetaPathsPlugin(outDir), externals.plugin],
      // `vite` is reached only through core's Vite fallback (a dynamic import
      // that never runs under a bundle). Under react-compat, core's CJS-only
      // deps stay native exactly as in start's Vite runtime.
      external: [
        "vite",
        ...(options.runtimeNoExternal === true ? RUNTIME_RUNNER_CJS_EXTERNALS : []),
      ],
    });
  } catch (error) {
    console.warn(
      `\n  [warn] Server bundle failed to build; \`neutron-ts start\` will transform route ` +
        `modules through Vite at boot instead.\n  ${String((error as Error)?.message ?? error)}\n`
    );
    return;
  }
  externals.link();

  fs.writeFileSync(
    manifestPath,
    JSON.stringify(
      {
        format: SERVER_BUNDLE_FORMAT,
        entry: path.relative(options.outputDir, path.join(outDir, "entry.js")).split(path.sep).join("/"),
        runtime: options.runtime,
      },
      null,
      2
    ),
    "utf-8"
  );
  console.log(`  ${files.length} modules → ${path.relative(options.cwd, outDir)}/entry.js`);
}

/**
 * Externals resolve from the module that imports them, as under Vite's SSR
 * runtime and plain Node. A bundled module's bare import is emitted into a
 * chunk under dist/server/start/, where Node resolves it from the APP ROOT —
 * and under pnpm a bundled dependency's own dependencies (core's
 * @mdx-js/mdx, shiki, …) are not visible from there, or resolve to the app's
 * different version. Such an import is rewritten to a private alias
 * (`@neutron-external/<name>-<hash>`) that a symlink in the bundle's own
 * node_modules/ points at the package root the importer resolved. Node then
 * applies that package's exports map with its usual conditions and loads the
 * same file the server's native modules do. Imports the app root resolves
 * identically stay bare.
 */
function importerResolvedExternals(outDir: string) {
  const links = new Map<string, string>();
  const rootOwnPackage = new Map<string, string | null>();

  const packageRootFrom = (dir: string, name: string): string | null => {
    for (let current = dir; ; current = path.dirname(current)) {
      const candidate = path.join(current, "node_modules", name);
      if (fs.existsSync(path.join(candidate, "package.json"))) {
        return fs.realpathSync(candidate);
      }
      if (path.dirname(current) === current) return null;
    }
  };
  const packageRootOf = (packageJsonPath: string, name: string): string | null => {
    for (let dir = path.dirname(packageJsonPath); ; dir = path.dirname(dir)) {
      const pkg = path.join(dir, "package.json");
      if (fs.existsSync(pkg)) {
        try {
          if (JSON.parse(fs.readFileSync(pkg, "utf-8")).name === name) return fs.realpathSync(dir);
        } catch {
          // keep walking
        }
      }
      if (path.dirname(dir) === dir) return null;
    }
  };

  return {
    plugin: {
      name: "neutron:importer-resolved-externals",
      enforce: "pre" as const,
      async resolveId(
        this: {
          resolve: (
            id: string,
            importer?: string,
            options?: { skipSelf?: boolean }
          ) => Promise<{ id: string; external?: boolean | string; packageJsonPath?: string } | null>;
        },
        id: string,
        importer: string | undefined
      ) {
        if (!importer || id.startsWith(".") || id.startsWith("/") || id.startsWith("\0") || id.includes(":")) {
          return null;
        }
        const resolved = await this.resolve(id, importer, { skipSelf: true });
        if (!resolved || resolved.external !== true || !resolved.packageJsonPath) return null;
        const parts = id.split("/");
        const name = id.startsWith("@") ? parts.slice(0, 2).join("/") : parts[0];
        const subpath = id.slice(name.length);
        const target = packageRootOf(resolved.packageJsonPath, name);
        if (!target) return null;
        if (!rootOwnPackage.has(name)) rootOwnPackage.set(name, packageRootFrom(outDir, name));
        if (rootOwnPackage.get(name) === target) return null;
        const hash = createHash("sha256").update(target).digest("hex").slice(0, 8);
        const alias = `@neutron-external/${name.replace(/^@/, "").replace(/\//g, "__")}-${hash}`;
        links.set(alias, target);
        return { id: alias + subpath, external: true };
      },
    },
    /** Create the alias symlinks (relative, so the dist moves with node_modules). */
    link(): void {
      for (const [alias, target] of links) {
        const linkPath = path.join(outDir, "node_modules", alias);
        fs.mkdirSync(path.dirname(linkPath), { recursive: true });
        fs.rmSync(linkPath, { force: true, recursive: true });
        fs.symlinkSync(path.relative(path.dirname(linkPath), target), linkPath, "dir");
      }
    },
  };
}

/**
 * Keep `import.meta.url` / `filename` / `dirname` pointing at each module's
 * SOURCE file, as they do under Vite's SSR runtime and plain Node. Bundled,
 * they would name the chunk instead, and code that resolves files next to
 * itself (fonts, templates, a generated client's engine) reads the wrong
 * directory. Rewritten relative to the chunk's own URL rather than as an
 * absolute path, so the dist keeps working when the app root moves with it.
 */
function pinImportMetaPathsPlugin(outDir: string) {
  const META = /\bimport\.meta\.(url|filename|dirname)\b/g;
  return {
    name: "neutron:pin-import-meta-paths",
    enforce: "pre" as const,
    transform(code: string, id: string) {
      if (id.startsWith("\0") || !code.includes("import.meta.")) return null;
      const file = id.split("?")[0];
      if (!path.isAbsolute(file) || !fs.existsSync(file)) return null;
      META.lastIndex = 0;
      if (!META.test(code)) return null;
      const rel = path.relative(outDir, file).split(path.sep).join("/");
      const url = `new URL(${JSON.stringify(rel)}, import.meta.url)`;
      let usesPath = false;
      const replaced = code.replace(META, (_match, prop: string) => {
        if (prop === "url") return `${url}.href`;
        usesPath = true;
        const filename = `__neutronFileURLToPath(${url})`;
        return prop === "filename" ? filename : `__neutronDirname(${filename})`;
      });
      // Same line: the transform moves no code between lines (map: null).
      const prefix = usesPath
        ? 'import { fileURLToPath as __neutronFileURLToPath } from "node:url"; ' +
          'import { dirname as __neutronDirname } from "node:path"; '
        : "";
      return { code: prefix + replaced, map: null };
    },
  };
}

function collectRuntimeRoutes(routes: Route[], appRoutes: Route[]): RuntimeRouteDef[] {
  const byId = new Map<string, Route>(routes.map((route) => [route.id, route]));
  const includedIds = new Set<string>();

  for (const route of appRoutes) {
    includedIds.add(route.id);
    let parentId = route.parentId;
    while (parentId) {
      includedIds.add(parentId);
      parentId = byId.get(parentId)?.parentId ?? null;
    }
  }

  return routes
    .filter((route) => includedIds.has(route.id))
    .map((route) => ({
      id: route.id,
      path: route.path,
      parentId: route.parentId,
      params: route.params,
      mode: route.config.mode,
      cache: route.config.cache,
      isLayout: route.file.includes("_layout"),
      file: route.file,
    }));
}

function generateRuntimeEntrySource(
  runtimeRoutes: RuntimeRouteDef[],
  appRoutes: Route[],
  clientEntryScriptSrc: string | null,
  clientCssFiles: string[],
  entryPath: string,
  routeRules: NeutronConfig["routes"] | undefined,
  globalMiddlewarePath?: string
): string {
  const imports: string[] = [];
  const moduleEntries: string[] = [];
  // Prepend an optional global middleware (outermost) in the generated entry.
  // Only references __globalMiddlewareModule when its import was emitted.
  const globalMiddlewareDecl = globalMiddlewarePath
    ? "const GLOBAL_MIDDLEWARE = (() => {\n" +
      "  const __gmExport = __globalMiddlewareModule.middleware ?? __globalMiddlewareModule.default;\n" +
      "  return typeof __gmExport === 'function' ? [__gmExport] : (Array.isArray(__gmExport) ? __gmExport.filter((f) => typeof f === 'function') : []);\n" +
      "})();"
    : "const GLOBAL_MIDDLEWARE = [];";
  const routeDefs: string[] = [];
  const appRouteIds = appRoutes.map((route) => route.id);
  const routeRulesJson = JSON.stringify(routeRules || {});

  if (globalMiddlewarePath) {
    const relGm = relativeImportPath(path.dirname(entryPath), globalMiddlewarePath);
    imports.push(`import * as __globalMiddlewareModule from "${relGm}";`);
  }

  runtimeRoutes.forEach((route, index) => {
    const importVar = `routeModule${index}`;
    const relPath = relativeImportPath(path.dirname(entryPath), route.file);
    imports.push(`import * as ${importVar} from "${relPath}";`);
    moduleEntries.push(`  "${escapeJsString(route.id)}": ${importVar},`);
    routeDefs.push(`  {
    id: "${escapeJsString(route.id)}",
    path: "${escapeJsString(route.path)}",
    parentId: ${route.parentId ? `"${escapeJsString(route.parentId)}"` : "null"},
    params: ${JSON.stringify(route.params)},
    mode: "${route.mode}",
    cache: ${JSON.stringify(route.cache || null)},
    isLayout: ${route.isLayout ? "true" : "false"},
  },`);
  });

  return `import { createRouter, compileRouteRules, resolveRouteRuleRedirect, resolveRouteRuleRewrite, resolveRouteRuleHeaders, renderAppRoute, isMutationMethod, createMemoryLoaderCacheStore } from "@neutron-build/core/runtime-edge";
${imports.join("\n")}

const CLIENT_ENTRY_SCRIPT_SRC = ${JSON.stringify(clientEntryScriptSrc)};
const CLIENT_STYLESHEET_HREFS = ${JSON.stringify(clientCssFiles)};
const ROUTE_RULES = compileRouteRules(${routeRulesJson});

const ROUTE_DEFS = [
${routeDefs.join("\n")}
];

const ROUTE_MODULES = {
${moduleEntries.join("\n")}
};

const APP_ROUTE_IDS = new Set(${JSON.stringify(appRouteIds)});
const ROUTE_DEF_BY_ID = new Map(ROUTE_DEFS.map((route) => [route.id, route]));
const ROUTES_BY_ID = new Map(ROUTE_DEFS.map((route) => [route.id, toRuntimeRoute(route)]));
const LOADER_DATA_CACHE = createMemoryLoaderCacheStore();
${globalMiddlewareDecl}

const router = createRouter();
for (const routeDef of ROUTE_DEFS) {
  if (!routeDef.isLayout && APP_ROUTE_IDS.has(routeDef.id)) {
    router.insert(toRuntimeRoute(routeDef));
  }
}

let __requestSeq = 0;

async function handleNeutronRequestInner(request) {
  const requestUrl = new URL(request.url);
  const pathname = normalizePathname(requestUrl.pathname);
  if (!pathname) {
    return new Response("Bad Request", { status: 400 });
  }

  const redirect = resolveRouteRuleRedirect(ROUTE_RULES, pathname, requestUrl.search);
  if (redirect) {
    return new Response(null, {
      status: redirect.status,
      headers: {
        Location: redirect.location,
      },
    });
  }

  const rewrite = resolveRouteRuleRewrite(ROUTE_RULES, pathname);
  const effectivePathname = rewrite?.pathname || pathname;

  const match = router.match(effectivePathname);
  if (!match || !APP_ROUTE_IDS.has(match.route.id)) {
    return new Response("Not Found", { status: 404 });
  }

  const layouts = getLayoutChain(match.route);
  const allRoutes = [...layouts, match.route];
  const routeModules = new Map();
  for (const route of allRoutes) {
    routeModules.set(route.id, ROUTE_MODULES[route.id] || {});
  }

  if (isMutationMethod(request.method)) {
    await LOADER_DATA_CACHE.deleteByPath(effectivePathname);
  }

  const response = await renderAppRoute(
    request,
    { route: match.route, params: match.params, layouts },
    routeModules,
    {
      clientEntryScriptSrc: CLIENT_ENTRY_SCRIPT_SRC,
      stylesheetHrefs: CLIENT_STYLESHEET_HREFS,
      loaderDataCache: LOADER_DATA_CACHE,
      requestTrace: {
        requestId: String(++__requestSeq),
        method: request.method,
        pathname: effectivePathname,
      },
      globalMiddleware: GLOBAL_MIDDLEWARE,
    }
  );

  if (isMutationMethod(request.method)) {
    await applyMutationInvalidationToLoaderDataCache(effectivePathname, response);
  }

  applyRouteRuleHeaders(response, pathname);
  return response;
}

function toRuntimeRoute(routeDef) {
  const config = { mode: routeDef.mode };
  if (routeDef.cache) {
    config.cache = routeDef.cache;
  }

  return {
    id: routeDef.id,
    path: routeDef.path,
    file: routeDef.id,
    pattern: /^$/,
    params: routeDef.params,
    config,
    parentId: routeDef.parentId,
  };
}

function getLayoutChain(route) {
  const layouts = [];
  let parentId = route.parentId;
  while (parentId) {
    const routeDef = ROUTE_DEF_BY_ID.get(parentId);
    if (!routeDef) {
      break;
    }
    if (routeDef.isLayout) {
      const layoutRoute = ROUTES_BY_ID.get(routeDef.id);
      if (layoutRoute) {
        layouts.unshift(layoutRoute);
      }
    }
    parentId = routeDef.parentId;
  }
  return layouts;
}

function normalizePathname(pathname) {
  let decoded;
  try {
    decoded = decodeURIComponent(pathname || "/");
  } catch {
    return null;
  }

  if (!decoded.startsWith("/") || decoded.includes("..")) {
    return null;
  }
  if (decoded.length > 1 && decoded.endsWith("/")) {
    return decoded.slice(0, -1);
  }
  return decoded;
}

function applyRouteRuleHeaders(response, pathname) {
  const matches = resolveRouteRuleHeaders(ROUTE_RULES, pathname);
  for (const match of matches) {
    for (const [name, value] of Object.entries(match.headers || {})) {
      try {
        if (!response.headers.has(name)) {
          response.headers.set(name, String(value));
        }
      } catch {
        // Ignore immutable Response headers (for example, redirect responses).
      }
    }
  }
}

async function applyMutationInvalidationToLoaderDataCache(pathname, response) {
  const directive = response.headers.get("x-neutron-invalidate");
  if (!directive) {
    return;
  }

  const tokens = directive
    .split(",")
    .map((token) => token.trim())
    .filter(Boolean);

  if (tokens.length === 0) {
    return;
  }

  for (const token of tokens) {
    if (token === "*") {
      await LOADER_DATA_CACHE.clear();
      return;
    }
    if (token === "self") {
      await LOADER_DATA_CACHE.deleteByPath(pathname);
      continue;
    }
    const normalized = normalizePathname(token);
    if (normalized) {
      await LOADER_DATA_CACHE.deleteByPath(normalized);
    }
  }
}

// Apply baseline security headers to every response from the production handler
// (the dev server does this already; the generated handler must match).
export async function handleNeutronRequest(request) {
  const response = await handleNeutronRequestInner(request);
  const defaults = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "strict-origin-when-cross-origin",
  };
  for (const [name, value] of Object.entries(defaults)) {
    if (!response.headers.has(name)) {
      response.headers.set(name, value);
    }
  }
  return response;
}
`;
}

function relativeImportPath(fromDir: string, filePath: string): string {
  const rel = path.relative(fromDir, filePath).split(path.sep).join("/");
  return rel.startsWith(".") ? rel : `./${rel}`;
}

function escapeJsString(value: string): string {
  return value.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}
