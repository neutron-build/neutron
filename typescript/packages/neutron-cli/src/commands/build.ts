import * as path from "node:path";
import * as fs from "node:fs";
import { createHash } from "node:crypto";
import { build as viteBuild, loadConfigFromFile, mergeConfig, createServer } from "vite";
import { neutronPlugin, CLIENT_ROUTE_QUERY } from "@neutron-build/core/vite";
import {
  discoverRoutes,
  adapterCloudflare,
  adapterDocker,
  adapterStatic,
  adapterVercel,
  prepareContentCollections,
  prepareRouteTypes,
  resolveRuntime,
  resolveRuntimeAliases,
  resolveRuntimeNoExternal,
  mergeSeoMetaInput,
  renderDocumentHead,
  setActiveMarkdownConfig,
} from "@neutron-build/core";
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

export async function build(): Promise<void> {
  const cwd = process.cwd();
  const routesDir = path.resolve(cwd, "src/routes");
  const outputDir = path.resolve(cwd, "dist");
  const neutronConfig = await loadNeutronConfig(cwd);
  const runtime = resolveRuntime(neutronConfig);
  const runtimeAliases = resolveRuntimeAliases(runtime);
  const runtimeNoExternal = resolveRuntimeNoExternal(runtime);
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
  // requiring an index.html entry point.
  if (appRouteCount > 0 || hasIslands) {
    console.log("Building client bundle...");
    await viteBuild(
      mergeConfig(userConfig, {
        configFile: false,
        root: cwd,
        plugins: [neutronPlugin({ routesDir, rootDir: cwd, routeRules: neutronConfig.routes })],
        css: cssConfig,
        resolve: {
          ...(runtimeAliases ? { alias: runtimeAliases } : {}),
          dedupe: ["preact", "preact/hooks", "preact/compat", "preact/jsx-runtime"],
        },
        build: {
          outDir: outputDir,
          emptyOutDir: true,
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
    const importLines: string[] = [];
    const keepRefs: string[] = [];
    routes.forEach((route, i) => {
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
        configFile: false,
        root: cwd,
        plugins: [neutronPlugin({ routesDir, rootDir: cwd, routeRules: neutronConfig.routes })],
        css: cssConfig,
        resolve: {
          ...(runtimeAliases ? { alias: runtimeAliases } : {}),
          dedupe: ["preact", "preact/hooks", "preact/compat", "preact/jsx-runtime"],
        },
        build: {
          outDir: outputDir,
          emptyOutDir: true,
          lib: { entry: cssEntryPath, formats: ["es"] as const },
          rollupOptions: {
            output: { assetFileNames: "assets/[name]-[hash][extname]" },
          },
          cssCodeSplit: false,
        },
      })
    );
    try { fs.unlinkSync(cssEntryPath); } catch {}
    // Remove the JS lib output — we only need the extracted CSS.
    for (const f of fs.readdirSync(outputDir)) {
      if (f.endsWith(".mjs") || f.endsWith(".js")) {
        try { fs.unlinkSync(path.join(outputDir, f)); } catch {}
      }
    }
  }

  const clientEntryScriptSrc = extractClientEntryScriptSrc(outputDir);
  if (clientEntryScriptSrc) {
    writeClientEntryMetadata(outputDir, clientEntryScriptSrc);
  }

  // Standalone islands entry: a SEPARATE Rollup build (so the SPA/app client
  // bundle stays byte-identical). It imports only the island runtime + the
  // virtual islands manifest — never the SPA runtime — so Rollup code-splits
  // each island into its own chunk. Prerendered static pages with islands
  // reference THIS entry instead of the 34KB index-*.js.
  if (hasIslands) {
    await viteBuild(
      mergeConfig(userConfig, {
        configFile: false,
        root: cwd,
        plugins: [neutronPlugin({ routesDir, rootDir: cwd, routeRules: neutronConfig.routes })],
        css: cssConfig,
        resolve: {
          ...(runtimeAliases ? { alias: runtimeAliases } : {}),
          dedupe: ["preact", "preact/hooks", "preact/compat", "preact/jsx-runtime"],
        },
        build: {
          outDir: outputDir,
          // Do NOT empty the output — the SPA/app client bundle is already there.
          emptyOutDir: false,
          rollupOptions: {
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
    userConfig,
    runtimeAliases,
    runtimeNoExternal,
  });

  // Create a Vite SSR server for rendering
  const server = await createServer(
    mergeConfig(userConfig, {
      configFile: false,
      root: cwd,
      plugins: [neutronPlugin({ routesDir, rootDir: cwd, routeRules: neutronConfig.routes })],
      css: cssConfig,
      resolve: {
        ...(runtimeAliases ? { alias: runtimeAliases } : {}),
        // Force one preact copy so route components and the renderer share the
        // same hooks dispatcher (otherwise hooks crash during pre-render).
        dedupe: ["preact", "preact/hooks", "preact/jsx-runtime", "preact/compat"],
      },
      ssr: {
        // Process these through Vite's SSR graph (deduped) instead of native
        // node resolution, so preact-render-to-string binds to the app's preact.
        // @neutron-build/core MUST share the SSR graph's preact too: its inline
        // components (e.g. Link) use hooks, and if core resolves preact natively
        // while the renderer/route components use the SSR-graph copy, the hooks
        // dispatcher is unset and pre-render crashes ("reading '__H'").
        noExternal: ["preact", "preact/hooks", "preact-render-to-string", "@neutron-build/core", ...runtimeNoExternal],
      },
      server: {
        middlewareMode: true,
        hmr: false,
        ws: false,
        // Allow loading the CLI-provided renderer from outside the app root.
        fs: { strict: false },
      },
      optimizeDeps: {
        noDiscovery: true,
      },
      appType: "custom",
    })
  );

  // Resolve the renderer through the SAME Vite SSR server that loads the route
  // modules, so preact-render-to-string and the components share ONE preact
  // instance. The CLI's own preact-render-to-string can otherwise bind to a
  // different preact copy (e.g. under pnpm, or a linked/monorepo install),
  // leaving the hooks dispatcher unset and crashing any component that uses
  // useState/useEffect during pre-render with "Cannot read properties of
  // undefined (reading '__H')".
  let appH = h;
  let appRender = renderToString;
  try {
    // Load the renderer + preact through the SAME Vite SSR graph as the route
    // components so they share ONE preact instance (otherwise hooks crash during
    // pre-render). This resolves them from the app, where preact and (ideally)
    // preact-render-to-string are installed at a single matched version. If the
    // app doesn't ship preact-render-to-string, we fall back to the CLI copy,
    // which is correct whenever the app and CLI resolve the same preact.
    const appPreact = (await server.ssrLoadModule("preact")) as { h?: typeof h };
    const appRts = (await server.ssrLoadModule("preact-render-to-string")) as {
      renderToString?: typeof renderToString;
      default?: { renderToString?: typeof renderToString };
    };
    if (appPreact?.h) appH = appPreact.h;
    const rts = appRts?.renderToString ?? appRts?.default?.renderToString;
    if (rts) appRender = rts;
  } catch {
    // Fall back to the CLI-bundled renderer (single-preact installs are fine).
  }

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
  ): Promise<string> {
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

    return renderDocumentHead(pathname, mergedSeo, headFragments);
  }

  // Render static routes
  console.log("\nRendering static routes...");
  
  const staticRoutes = pageRoutes.filter((r) => r.config.mode === "static");
  let renderedCount = 0;
  let skippedCount = 0;

  for (const route of staticRoutes) {
    try {
      const module = await loadRouteModule(route);

      if (!module?.default) {
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

          for (const layoutRoute of [...layoutChain].reverse()) {
            const layoutModule = await loadRouteModule(layoutRoute);
            // Call layout loaders so sidebars, nav trees, etc. are populated.
            let layoutData: unknown = {};
            if (layoutModule?.loader) {
              layoutData = await layoutModule.loader({
                request,
                params,
                context,
              } as LoaderArgs);
            }
            if (layoutModule?.default) {
              element = appH(layoutModule.default as any, { data: layoutData }, element);
            }
          }

          const html = appRender(element);
          const headHtml = await resolveRouteHeadHtml(
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
            islandsEntryScriptSrc
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

      for (const layoutRoute of [...layoutChain].reverse()) {
        const layoutModule = await loadRouteModule(layoutRoute);
        let layoutData: unknown = {};
        if (layoutModule?.loader) {
          layoutData = await layoutModule.loader({
            request,
            params: {},
            context,
          } as LoaderArgs);
        }
        if (layoutModule?.default) {
          element = h(layoutModule.default as any, { data: layoutData }, element);
        }
      }

      const html = appRender(element);
      const headHtml = await resolveRouteHeadHtml(
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
        islandsEntryScriptSrc
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
      skippedCount++;
    }
  }

  if (Object.keys(staticHeadersByRoute).length > 0) {
    writeStaticHeadersMetadata(outputDir, staticHeadersByRoute);
  }

  // Allow pending module processing to settle before closing middleware server.
  await new Promise((resolve) => setTimeout(resolve, 50));
  await server.close();

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
  console.log(`\nBuild complete!`);
  console.log(`Output: ${outputDir}`);
}

/**
 * Resolve a route pattern with params to an actual path.
 * Handles both named params and catch-all (splat) params:
 *   "/blog/:slug"  + { slug: "hello" }                → "/blog/hello"
 *   "/docs/*"      + { "*": "getting-started/intro" }  → "/docs/getting-started/intro"
 */
function resolvePath(pattern: string, params: Record<string, string>): string {
  let resolved = pattern;

  for (const [key, value] of Object.entries(params)) {
    // Named param — :slug or [slug]
    const bracketReplaced = resolved.replace(`[${key}]`, value);
    const colonReplaced = resolved.replace(`:${key}`, value);

    if (bracketReplaced !== resolved) {
      resolved = bracketReplaced;
    } else if (colonReplaced !== resolved) {
      resolved = colonReplaced;
    } else {
      // Catch-all — replace *paramName (e.g., *slug) or bare *
      const splatPattern = key === "*" ? "*" : `*${key}`;
      resolved = resolved.replace(splatPattern, value);
    }
  }

  return resolved;
}

function wrapHtml(
  content: string,
  routePath: string,
  _loaderData?: unknown,
  clientEntryScriptSrc: string | null = null,
  headHtml: string = renderDocumentHead(routePath, null),
  cssFiles: string[] = [],
  islandsEntryScriptSrc: string | null = null
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
<html lang="en">
<head>
${headHtml}
${cssLinks}
</head>
<body>
<div id="app">${content}</div>
${clientScript}
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
 * input "neutron-islands" → Rollup emits `assets/neutron-islands-[hash].js`.
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

function extractClientEntryScriptSrc(outputDir: string): string | null {
  const assetsDir = path.join(outputDir, "assets");
  if (fs.existsSync(assetsDir)) {
    const candidates = fs
      .readdirSync(assetsDir)
      .filter((name) => name.startsWith("index-") && name.endsWith(".js"))
      .sort();

    if (candidates.length > 0) {
      return `/assets/${candidates[candidates.length - 1]}`;
    }
  }

  const indexPath = path.join(outputDir, "index.html");
  if (!fs.existsSync(indexPath)) {
    return null;
  }

  const html = fs.readFileSync(indexPath, "utf-8");
  const match = html.match(/<script[^>]*type="module"[^>]*src="([^"]+)"[^>]*><\/script>/i);
  return match?.[1] || null;
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
  preset: "vercel" | "cloudflare" | "docker" | "static" | null;
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
  userConfig: any;
  runtimeAliases?: Record<string, string>;
  runtimeNoExternal?: string[];
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
      if (value === "vercel" || value === "cloudflare" || value === "docker" || value === "static") {
        preset = value;
      }
      continue;
    }
    if (arg.startsWith("--preset=")) {
      const value = arg.split("=")[1];
      if (value === "vercel" || value === "cloudflare" || value === "docker" || value === "static") {
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
  fs.writeFileSync(
    entryPath,
    generateRuntimeEntrySource(
      runtimeRoutes,
      appRoutes,
      options.clientEntryScriptSrc,
      entryPath,
      options.routeRules
    ),
    "utf-8"
  );

  const bundleOutDir = path.join(options.outputDir, "server", target);
  await viteBuild(
    mergeConfig(options.userConfig, {
      configFile: false,
      root: options.cwd,
      plugins: [
        neutronPlugin({
          routesDir: options.routesDir,
          rootDir: options.cwd,
          routeRules: options.routeRules,
        }),
      ],
      ...(options.runtimeAliases ? { resolve: { alias: options.runtimeAliases } } : {}),
      ssr: {
        target: target === "worker" ? "webworker" : "node",
        // Bundle @neutron-build/core with the same preact the renderer uses, so
        // inline hook components (e.g. Link) in app-mode routes don't hit the
        // two-preact-instance "__H" crash at runtime — matching the build/dev paths.
        noExternal: [
          "preact",
          "preact-render-to-string",
          "@neutron-build/core",
          ...(options.runtimeNoExternal || []),
        ],
      },
      build: {
        ssr: entryPath,
        outDir: bundleOutDir,
        emptyOutDir: true,
        rollupOptions: {
          output: {
            format: "esm",
            entryFileNames: "entry.js",
            chunkFileNames: "chunks/[name]-[hash].js",
            assetFileNames: "assets/[name]-[hash][extname]",
          },
        },
      },
    })
  );

  const entryRelativePath = path.relative(options.outputDir, path.join(bundleOutDir, "entry.js"));
  return {
    target,
    outDir: bundleOutDir,
    entryPath: path.join(bundleOutDir, "entry.js"),
    entryRelativePath: entryRelativePath.split(path.sep).join("/"),
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
  entryPath: string,
  routeRules: NeutronConfig["routes"] | undefined
): string {
  const imports: string[] = [];
  const moduleEntries: string[] = [];
  const routeDefs: string[] = [];
  const appRouteIds = appRoutes.map((route) => route.id);
  const routeRulesJson = JSON.stringify(routeRules || {});

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

  return `import { h } from "preact";
import { createRouter, runMiddlewareChain, renderToString, encodeSerializedPayloadAsJson, serializeForInlineScript, mergeSeoMetaInput, renderDocumentHead, compileRouteRules, resolveRouteRuleRedirect, resolveRouteRuleRewrite, resolveRouteRuleHeaders } from "@neutron-build/core/runtime-edge";
${imports.join("\n")}

const CLIENT_ENTRY_SCRIPT_SRC = ${JSON.stringify(clientEntryScriptSrc)};
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
const LOADER_DATA_CACHE = new Map();
const LOADER_CACHE_MAX_ENTRIES = 4000;

const router = createRouter();
for (const routeDef of ROUTE_DEFS) {
  if (!routeDef.isLayout && APP_ROUTE_IDS.has(routeDef.id)) {
    router.insert(toRuntimeRoute(routeDef));
  }
}

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

  const layoutChain = getLayoutChain(match.route);
  const allRoutes = [...layoutChain, match.route];
  const routeModules = new Map();
  for (const route of allRoutes) {
    routeModules.set(route.id, ROUTE_MODULES[route.id] || {});
  }

  const middlewares = [];
  for (const route of allRoutes) {
    const mod = routeModules.get(route.id);
    if (mod?.middleware) {
      middlewares.push(mod.middleware);
    }
  }

  const context = {};
  if (isMutationMethod(request.method)) {
    invalidateLoaderDataCacheForPath(effectivePathname);
  }

  const response = await runMiddlewareChain(middlewares, request, context, async () => {
    let actionData = undefined;
    const pageModule = routeModules.get(match.route.id);

    if (!pageModule?.default) {
      return new Response("Not Found", { status: 404 });
    }

    if (isMutationMethod(request.method) && pageModule.action) {
      try {
        const actionResult = await pageModule.action({
          request,
          params: match.params,
          context,
        });
        if (actionResult instanceof Response) {
          return actionResult;
        }
        actionData = actionResult;
      } catch (error) {
        if (error instanceof Response) {
          return error;
        }
        return renderErrorResponse(allRoutes, routeModules, match.route, toError(error));
      }
    }

    const loaderResults = await Promise.all(
      allRoutes.map(async (route) => {
        const mod = routeModules.get(route.id);
        if (!mod?.loader) {
          return { routeId: route.id, data: null, error: null };
        }
        const routeParams = route.id === match.route.id ? match.params : {};
        const loaderCacheMaxAge = route.config?.cache?.loaderMaxAge || 0;
        const canCacheLoaderData =
          loaderCacheMaxAge > 0 && isLoaderDataCacheableRequest(request);
        const canReadLoaderCache =
          canCacheLoaderData && isLoaderDataCacheReadableMethod(request.method);
        const loaderCacheKey = canCacheLoaderData
          ? buildLoaderDataCacheKey(request, route.id, routeParams)
          : null;
        if (loaderCacheKey && canReadLoaderCache) {
          const cachedData = readCachedLoaderData(loaderCacheKey);
          if (cachedData !== null) {
            return { routeId: route.id, data: cachedData, error: null };
          }
        }
        try {
          const data = await mod.loader({
            request,
            params: routeParams,
            context,
          });
          if (loaderCacheKey) {
            storeLoaderDataCache(loaderCacheKey, data, loaderCacheMaxAge);
          }
          return { routeId: route.id, data, error: null };
        } catch (error) {
          return { routeId: route.id, data: null, error };
        }
      })
    );

    const loaderData = {};
    for (const result of loaderResults) {
      if (result.error) {
        if (result.error instanceof Response) {
          return result.error;
        }
        const errorRoute = allRoutes.find((route) => route.id === result.routeId) || match.route;
        return renderErrorResponse(allRoutes, routeModules, errorRoute, toError(result.error));
      }
      if (result.data !== null) {
        loaderData[result.routeId] = result.data;
      }
    }

    const routeHeaders = await resolveRouteHeaders(allRoutes, routeModules, {
      request,
      params: match.params,
      context,
      loaderData,
      actionData,
    });

    if (isJsonRequest(request)) {
      const payload = { ...loaderData };
      if (actionData !== undefined) {
        payload.__action__ = actionData;
      }
      routeHeaders.set("Content-Type", "application/json");
      return new Response(encodeSerializedPayloadAsJson(payload), { headers: routeHeaders });
    }

    try {
      let element = h(pageModule.default, {
        data: loaderData[match.route.id],
        params: match.params,
        actionData,
      });

      for (let i = layoutChain.length - 1; i >= 0; i--) {
        const layoutRoute = layoutChain[i];
        const layoutModule = routeModules.get(layoutRoute.id);
        if (layoutModule?.default) {
          element = h(layoutModule.default, { data: loaderData[layoutRoute.id] }, element);
        }
      }

      const routeHeadHtml = await resolveRouteHeadHtml(allRoutes, routeModules, {
        request,
        params: match.params,
        context,
        loaderData,
        actionData,
        pathname,
      });
      const html = renderToString(element);
      const fullHtml = wrapHtml(html, pathname, loaderData, actionData, routeHeadHtml);
      return new Response(fullHtml, {
        headers: withDefaultContentType(routeHeaders, "text/html; charset=utf-8"),
      });
    } catch (error) {
      return renderErrorResponse(allRoutes, routeModules, match.route, toError(error));
    }
  });

  if (isMutationMethod(request.method)) {
    applyMutationInvalidationToLoaderDataCache(effectivePathname, response);
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

function isMutationMethod(method) {
  const normalized = String(method || "GET").toUpperCase();
  return normalized === "POST" || normalized === "PUT" || normalized === "PATCH" || normalized === "DELETE";
}

function isJsonRequest(request) {
  const accept = request.headers.get("Accept") || "";
  return accept.includes("application/json");
}

function isLoaderDataCacheableRequest(request) {
  const cacheControl = request.headers.get("Cache-Control") || "";
  if (cacheControl.includes("no-cache") || cacheControl.includes("no-store")) {
    return false;
  }

  if (request.headers.has("Authorization") || request.headers.has("Cookie")) {
    return false;
  }

  return true;
}

function isLoaderDataCacheReadableMethod(method) {
  const normalized = String(method || "GET").toUpperCase();
  return normalized === "GET" || normalized === "HEAD";
}

function buildLoaderDataCacheKey(request, routeId, params) {
  const url = new URL(request.url);
  const encodedParams = stableEncodeParams(params);
  return \`\${url.pathname}::\${url.search}::\${routeId}::\${encodedParams}\`;
}

function stableEncodeParams(params) {
  const sortedEntries = Object.entries(params).sort(([left], [right]) =>
    left.localeCompare(right)
  );
  return JSON.stringify(sortedEntries);
}

function readCachedLoaderData(key) {
  const entry = LOADER_DATA_CACHE.get(key);
  if (!entry) {
    return null;
  }
  if (entry.expiresAt <= Date.now()) {
    LOADER_DATA_CACHE.delete(key);
    return null;
  }
  return entry.data;
}

function storeLoaderDataCache(key, data, maxAgeSec) {
  if (!(maxAgeSec > 0)) {
    return;
  }

  if (!LOADER_DATA_CACHE.has(key) && LOADER_DATA_CACHE.size >= LOADER_CACHE_MAX_ENTRIES) {
    const oldest = LOADER_DATA_CACHE.keys().next().value;
    if (typeof oldest === "string") {
      LOADER_DATA_CACHE.delete(oldest);
    }
  }

  LOADER_DATA_CACHE.set(key, {
    data,
    expiresAt: Date.now() + maxAgeSec * 1000,
  });
}

function invalidateLoaderDataCacheForPath(pathname) {
  const normalized = normalizePathname(pathname);
  if (!normalized) {
    return;
  }
  const prefix = \`\${normalized}::\`;
  for (const key of LOADER_DATA_CACHE.keys()) {
    if (key.startsWith(prefix)) {
      LOADER_DATA_CACHE.delete(key);
    }
  }
}

function applyMutationInvalidationToLoaderDataCache(pathname, response) {
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
      LOADER_DATA_CACHE.clear();
      return;
    }
    if (token === "self") {
      invalidateLoaderDataCacheForPath(pathname);
      continue;
    }
    const normalized = normalizePathname(token);
    if (normalized) {
      invalidateLoaderDataCacheForPath(normalized);
    }
  }
}

async function resolveRouteHeaders(allRoutes, routeModules, args) {
  const headers = new Headers();
  for (const route of allRoutes) {
    const mod = routeModules.get(route.id);
    if (!mod?.headers) {
      continue;
    }
    const resolved = await mod.headers(args);
    const next = toHeaders(resolved);
    next.forEach((value, name) => {
      headers.set(name, value);
    });
  }
  return headers;
}

async function resolveRouteHeadHtml(allRoutes, routeModules, args) {
  let mergedSeo = null;
  const headFragments = [];

  for (const route of allRoutes) {
    const mod = routeModules.get(route.id);
    if (!mod?.head) {
      continue;
    }

    const resolved = await mod.head(args);
    if (!resolved) {
      continue;
    }

    if (typeof resolved === "string") {
      headFragments.push(resolved);
      continue;
    }

    mergedSeo = mergeSeoMetaInput(mergedSeo, resolved);
  }

  return renderDocumentHead(args.pathname, mergedSeo, headFragments);
}

function toHeaders(value) {
  if (!value) {
    return new Headers();
  }
  if (value instanceof Headers) {
    return new Headers(value);
  }
  const headers = new Headers();
  for (const [name, headerValue] of Object.entries(value)) {
    headers.set(name, String(headerValue));
  }
  return headers;
}

function withDefaultContentType(headers, fallback) {
  if (!headers.has("Content-Type")) {
    headers.set("Content-Type", fallback);
  }
  return headers;
}

function renderErrorResponse(allRoutes, modules, route, error) {
  const boundary = findNearestErrorBoundary(allRoutes, modules, route);
  if (!boundary) {
    return new Response(renderDefaultError(error), {
      status: 500,
      headers: { "Content-Type": "text/html; charset=utf-8" },
    });
  }

  const boundaryElement = h(boundary, { error });
  const boundaryHtml = renderToString(boundaryElement);
  return new Response(wrapHtml(boundaryHtml, route.path, {}), {
    status: 500,
    headers: { "Content-Type": "text/html; charset=utf-8" },
  });
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

function findNearestErrorBoundary(allRoutes, modules, route) {
  const pageModule = modules.get(route.id);
  if (pageModule?.ErrorBoundary) {
    return pageModule.ErrorBoundary;
  }

  for (let i = allRoutes.length - 2; i >= 0; i--) {
    const layoutModule = modules.get(allRoutes[i].id);
    if (layoutModule?.ErrorBoundary) {
      return layoutModule.ErrorBoundary;
    }
  }

  return undefined;
}

function renderDefaultError(error) {
  return \`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Error - Neutron</title>
</head>
<body>
  <h1>Application Error</h1>
  <pre>\${escapeHtml(error.message || "Unknown error")}</pre>
</body>
</html>\`;
}

function wrapHtml(content, pathname, loaderData, actionData, headHtml = "") {
  const allData = { ...loaderData };
  if (actionData !== undefined) {
    allData.__action__ = actionData;
  }
  const dataScript = Object.keys(allData).length > 0
    ? \`<script>window.__NEUTRON_DATA_SERIALIZED__=\${serializeForInlineScript(allData)};</script>\`
    : "";
  const clientScript = CLIENT_ENTRY_SCRIPT_SRC
    ? \`<script type="module" src="\${escapeHtml(CLIENT_ENTRY_SCRIPT_SRC)}"></script>\`
    : "";

  return \`<!DOCTYPE html>
<html lang="en">
<head>
\${headHtml || renderDocumentHead(pathname, null)}
</head>
<body>
<div id="app">\${content}</div>
\${dataScript}
\${clientScript}
</body>
</html>\`;
}

function escapeHtml(str) {
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function toError(value) {
  if (value instanceof Error) {
    return value;
  }
  if (typeof value === "string") {
    return new Error(value);
  }
  return new Error("Unknown error");
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

async function loadNeutronConfig(cwd: string): Promise<NeutronConfig> {
  const candidates = [
    "neutron.config.ts",
    "neutron.config.js",
    "neutron.config.mjs",
    "neutron.config.cjs",
  ];

  for (const file of candidates) {
    const fullPath = path.resolve(cwd, file);
    if (!fs.existsSync(fullPath)) {
      continue;
    }

    const loaded = await loadConfigFromFile(
      { command: "build", mode: "production" },
      fullPath,
      cwd
    );

    if (loaded?.config) {
      return loaded.config as NeutronConfig;
    }
  }

  return {};
}
