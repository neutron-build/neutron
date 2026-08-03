import * as fs from "node:fs";
import * as path from "node:path";
import MagicString from "magic-string";
import type { Plugin, ViteDevServer } from "vite";
import { escapeHtml } from "../core/escape.js";
import { assertRenderedFragment } from "../core/fragment-guard.js";
import { discoverRoutes } from "../core/manifest.js";
import { prepareRouteTypes } from "../core/route-typegen.js";
import { createRouter } from "../core/router.js";
import { runMiddlewareChain } from "../core/middleware.js";
import {
  compileRouteRules,
  resolveRouteRuleHeaders,
  resolveRouteRuleRedirect,
  resolveRouteRuleRewrite,
} from "../core/route-rules.js";
import {
  encodeSerializedPayloadAsJson,
  serializeForInlineScript,
} from "../core/serialization.js";
import {
  hasServerOnlyImport,
  isContentModuleId,
  isServerOnlySpecifier,
  stripQueryFromId,
  stripServerOnlyRouteModule,
} from "./server-only.js";
import {
  findStaticInteractivity,
  formatStaticInteractivityWarning,
} from "./static-interactivity-check.js";
import { renderDocumentHead, mergeSeoMetaInput, buildHtmlOpenTag, buildBodyOpenTag } from "../core/seo.js";
import type { SeoMetaInput } from "../core/seo.js";
import type { NeutronRoutesConfig } from "../config.js";
import type { Route, RouteModule, AppContext, LoaderArgs, ActionArgs, HeadArgs, MiddlewareFn, ErrorBoundaryProps } from "../core/types.js";
import { handleImageRequest } from "../server/image-optimizer.js";
import { checkAccessibility } from "./a11y-checker.js";
import { parseError } from "./error-parser.js";

export interface NeutronPluginOptions {
  routesDir?: string;
  rootDir?: string;
  writeRouteTypes?: boolean;
  routeRules?: NeutronRoutesConfig;
}

const ROUTES_DIR_DEFAULT = "src/routes";
export const CLIENT_ROUTE_QUERY = "neutron-client-route";
const EMPTY_SERVER_MODULE_ID = "\0neutron:empty-server-module";
// Client-side stub for @neutron-build/core/content. The real module is
// server/build-time only (fs, node:crypto, MDX compiler); these no-ops satisfy
// the named imports a client-reachable component may carry, without pulling the
// server module into the browser bundle. The data functions throw if actually
// called on the client — that means data wasn't loaded in a loader/getStaticPaths
// and passed down, which is the real mistake.
const CONTENT_CLIENT_STUB_ID = "\0neutron:content-client-stub";
const CONTENT_CLIENT_STUB_SOURCE = `
const serverOnly = (name) => () => {
  throw new Error(
    "[neutron] \\"" + name + "\\" from @neutron-build/core/content is server-only and " +
    "cannot run in the browser. Load content in a route loader or getStaticPaths and " +
    "pass the result to your components/islands as props."
  );
};
export const getCollection = serverOnly("getCollection");
export const getEntry = serverOnly("getEntry");
export const prepareContentCollections = serverOnly("prepareContentCollections");
export const sanitizeHtml = serverOnly("sanitizeHtml");
export const defineCollection = (config) => config;
export const setActiveMarkdownConfig = () => {};
export const getActiveMarkdownConfig = () => undefined;
export const z = undefined;
export const __renderCacheStatsForTest = () => ({ renders: 0, cacheHits: 0 });
export const __resetRenderCacheForTest = () => {};
export default undefined;
`;
const DEV_TOOLBAR_MODULE_ID = "virtual:neutron/dev-toolbar";
const DEV_TOOLBAR_RESOLVED_ID = "\0virtual:neutron/dev-toolbar";

interface RouterState {
  routes: Route[];
  router: ReturnType<typeof createRouter>;
  /**
   * Resolved root-relative module ids (e.g. "/src/components/Counter.tsx") for
   * every component referenced by an `<Island component={X}>` tag anywhere in
   * the app's source. Populated by an eager source scan in refreshRoutes so the
   * `virtual:neutron-islands` manifest is complete at build time (the per-module
   * `transform` runs lazily and cannot, on its own, enumerate every island).
   */
  islandIds: Set<string>;
}

const ISLAND_VIRTUAL_ID = "virtual:neutron-islands";
const ISLAND_VIRTUAL_RESOLVED_ID = "\0virtual:neutron-islands";
const ISLAND_SCRIPT_EXTENSIONS = [".tsx", ".ts", ".jsx", ".js", ".mjs", ".cjs"];

interface LoaderResult {
  routeId: string;
  data: unknown;
  module: RouteModule;
  error?: Error;
}

interface DevTimingContext {
  loaders: Array<{ routeId: string; ms: number }>;
  renderMs: number | undefined;
}

function sanitizeHost(host: string | undefined): string {
  if (!host) return 'localhost';
  const hostname = host.split(':')[0];
  if (!/^[a-zA-Z0-9._-]+$/.test(hostname)) return 'localhost';
  return host;
}

// Detects URL pathnames that look like file requests so the dev middleware can
// bypass the router for static assets without also bypassing legitimate
// dynamic-route params that happen to contain dots (e.g. `/u/03.09.02`).
//
// A real file extension starts with a letter and is short (≤ 8 chars after
// the dot, alphanumeric). This excludes purely numeric "extensions" like
// `.02` or `.123` that arise from version-tag- or dotted-id-style param
// values, while still matching every real extension we care about (`css`,
// `js`, `tsx`, `svg`, `mp4`, `woff2`, `html`, `json`, …).
const STATIC_ASSET_TRAILING_EXT = /\/[^/]+\.[a-zA-Z][a-zA-Z0-9]{0,7}$/;
function looksLikeStaticAsset(pathname: string): boolean {
  return STATIC_ASSET_TRAILING_EXT.test(pathname);
}

// Resolves the client hydration entry path that the dev server injects as a
// `<script type="module">` into rendered HTML. Apps that hydrate client-side
// place this file at `src/main.tsx` (or one of the other listed candidates).
// SSR-only apps with no hydration step omit the file entirely; in that case
// this returns null and the dev server skips the script injection so the
// missing-file pre-transform error doesn't fire.
const CLIENT_ENTRY_CANDIDATES = [
  "src/main.tsx",
  "src/main.ts",
  "src/main.jsx",
  "src/main.js",
  "src/client.tsx",
  "src/client.ts",
  "src/client.jsx",
  "src/client.js",
];
function resolveClientEntryPath(rootDir: string): string | null {
  for (const candidate of CLIENT_ENTRY_CANDIDATES) {
    const abs = path.join(rootDir, candidate);
    if (fs.existsSync(abs)) {
      return "/" + candidate;
    }
  }
  return null;
}

export function neutronPlugin(options: NeutronPluginOptions = {}): Plugin {
  const routesDir = path.resolve(options.routesDir || ROUTES_DIR_DEFAULT);
  const rootDir = path.resolve(options.rootDir || process.cwd());
  const normalizedRoutesDir = normalizePathForComparison(routesDir);
  const writeRouteTypes = options.writeRouteTypes === true;
  const state: RouterState = {
    routes: [],
    router: createRouter(),
    islandIds: new Set<string>(),
  };
  const srcDir = path.resolve(rootDir, "src");
  const compiledRouteRules = compileRouteRules(options.routeRules);
  const clientEntry = resolveClientEntryPath(rootDir);
  let isBuild = false;

  async function refreshRoutes() {
    state.routes = discoverRoutes({ routesDir });
    state.router = createRouter();
    for (const route of state.routes) {
      state.router.insert(route);
    }

    state.islandIds = scanIslandIds(srcDir, rootDir);

    // Static routes are not hydrated, so hooks outside an island silently do
    // nothing at runtime. Surface it here, where the author can act on it.
    const warning = formatStaticInteractivityWarning(
      findStaticInteractivity(state.routes),
      rootDir
    );
    if (warning) {
      console.warn(warning);
    }

    if (writeRouteTypes) {
      await prepareRouteTypes({
        rootDir,
        routesDir,
        writeTypes: true,
      });
    }
  }

  return {
    name: "neutron:core",
    // Run before @preact/preset-vite's babel transform so the island JSX
    // injection operates on raw `<Island component={X}>` source, not the
    // already-lowered `h(Island, { component: X })` form.
    enforce: "pre",

    async configResolved(config) {
      isBuild = config.command === "build";
      await refreshRoutes();
    },

    configureServer(server: ViteDevServer) {
      void refreshRoutes().catch((error) => {
        console.error("Failed to refresh Neutron routes:", error);
      });

      server.watcher.add(routesDir);

      server.watcher.on("all", (event, file) => {
        if (file.startsWith(routesDir)) {
          void refreshRoutes()
            .then(() => {
              server.ws.send({
                type: "custom",
                event: "neutron:routes-updated",
                data: state.routes,
              });
              server.ws.send({
                type: "custom",
                event: "neutron:error-resolved",
                data: {},
              });
            })
            .catch((error) => {
              console.error("Failed to refresh Neutron routes:", error);
            });
        }
      });

      server.middlewares.use(async (req, res, next) => {
        const requestStartMs = performance.now();
        let matchedRouteId: string | undefined;
        try {
          const url = new URL(req.url || "/", `http://${sanitizeHost(req.headers.host)}`);
          const originalPathname = url.pathname;

          // Vite-internal and dev-machinery paths always bypass the router.
          if (
            originalPathname.startsWith("/@") ||
            originalPathname.startsWith("/node_modules") ||
            originalPathname === "/__vite_ping" ||
            originalPathname === "/favicon.ico"
          ) {
            return next();
          }

          // Static-asset bypass: only short-circuit if the LAST path segment
          // looks like a real file extension (a single trailing `.ext` with
          // alphanumeric characters). This preserves bypasses for things like
          // `/styles.css`, `/foo.js`, `/img/logo.svg` while still routing
          // dotted dynamic params such as Codex unit IDs `/u/03.09.02`, which
          // are param values rather than file paths.
          if (looksLikeStaticAsset(originalPathname)) {
            return next();
          }

          if (originalPathname === "/_neutron/image") {
            const imageRequest = new Request(url.toString(), {
              method: req.method || "GET",
              headers: req.headers as HeadersInit,
            });
            const imageResponse = await handleImageRequest(imageRequest, {
              publicDirs: [
                path.join(rootDir, "public"),
                path.join(rootDir, "src"),
                rootDir,
              ],
              cacheDir: path.join(rootDir, ".neutron", "image-cache"),
            });

            res.statusCode = imageResponse.status;
            imageResponse.headers.forEach((value, key) => {
              res.setHeader(key, value);
            });

            if (imageResponse.body) {
              const reader = imageResponse.body.getReader();
              while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                res.write(Buffer.from(value));
              }
            }
            res.end();
            return;
          }

          const redirect = resolveRouteRuleRedirect(
            compiledRouteRules,
            normalizePathname(originalPathname),
            url.search
          );
          if (redirect) {
            res.statusCode = redirect.status;
            res.setHeader("Location", redirect.location);
            res.end();
            return;
          }

          const rewrite = resolveRouteRuleRewrite(
            compiledRouteRules,
            normalizePathname(originalPathname)
          );
          const pathname = rewrite?.pathname || normalizePathname(originalPathname);

          const match = state.router.match(pathname);
          if (!match) {
            return next();
          }

          const { route, params } = match;
          matchedRouteId = route.id;
          const layoutChain = getLayoutChain(route, state.routes);
          const allRoutes = [...layoutChain, route];

          const request = await createRequest(req, url);
          const context: AppContext = {};

          // Dev timing context
          const devTiming: DevTimingContext = {
            loaders: [],
            renderMs: undefined,
          };

          // Collect middleware from layouts + route
          const middlewares: MiddlewareFn[] = [];
          for (const r of allRoutes) {
            const loaded = await server.ssrLoadModule(r.file);
            const module = loaded as RouteModule;
            if (module.middleware) {
              middlewares.push(module.middleware);
            }
          }

          // Run middleware chain + final handler
          const response = await runMiddlewareChain(
            middlewares,
            request,
            context,
            () => handleRequest(server, route, params, layoutChain, allRoutes, request, context, rootDir, clientEntry, devTiming)
          );
          applyRouteRuleHeadersToResponse(
            response,
            resolveRouteRuleHeaders(compiledRouteRules, normalizePathname(originalPathname))
          );

          // Send response
          response.headers.forEach((value, key) => {
            res.setHeader(key, value);
          });
          res.statusCode = response.status;

          if (response.body) {
            const contentType = response.headers.get("content-type") || "";
            if (contentType.includes("text/html")) {
              // Run through Vite's transformIndexHtml to inject HMR client, etc.
              const reader = response.body.getReader();
              const chunks: Uint8Array[] = [];
              while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                chunks.push(value);
              }
              let rawHtml = Buffer.concat(chunks).toString("utf-8");

              // Collect CSS from Vite's module graph (populated by ssrLoadModule)
              // and inject <link> tags so CSS loads before content renders (prevents FOUC)
              const cssUrls = collectCssFromModuleGraph(server, allRoutes.map(r => r.file));
              if (cssUrls.length > 0) {
                // Deduplicate: keep only ?direct URLs (serve actual CSS, not JS modules)
                const seen = new Set<string>();
                const dedupedUrls: string[] = [];
                for (const url of cssUrls) {
                  const base = url.replace(/\?.*$/, "");
                  if (!seen.has(base)) {
                    seen.add(base);
                    dedupedUrls.push(base.endsWith(".css") ? `${base}?direct` : url);
                  }
                }
                const cssLinks = dedupedUrls
                  .map(url => `  <link rel="stylesheet" href="${url.replace(/"/g, '&quot;')}">`)
                  .join("\n");
                rawHtml = rawHtml.replace("</head>", `${cssLinks}\n</head>`);
              }

              let transformedHtml = await server.transformIndexHtml(
                req.url || "/",
                rawHtml
              );
              // Remove non-?direct CSS links that have a ?direct counterpart
              // (Vite's transformIndexHtml may inject JS-module CSS links that
              // serve JavaScript instead of CSS, causing render-blocking stalls)
              const directBases = new Set<string>();
              const linkPattern = /<link rel="stylesheet" href="([^"]+)">/g;
              let m: RegExpExecArray | null;
              while ((m = linkPattern.exec(transformedHtml)) !== null) {
                const href = m[1];
                if (href.includes("?direct")) {
                  directBases.add(href.replace(/\?direct$/, ""));
                }
              }
              if (directBases.size > 0) {
                transformedHtml = transformedHtml.replace(
                  /<link rel="stylesheet" href="([^"]+)">/g,
                  (match, href) => {
                    if (href.includes("?")) return match;
                    if (directBases.has(href)) return "";
                    return match;
                  }
                );
              }
              res.write(transformedHtml);
            } else {
              const reader = response.body.getReader();
              while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                res.write(Buffer.from(value));
              }
            }
          }
          res.end();

          // Send timing data via HMR WebSocket
          const totalMs = performance.now() - requestStartMs;
          server.ws.send({
            type: "custom",
            event: "neutron:dev-toolbar:request",
            data: {
              pathname: originalPathname,
              routeId: route.id,
              totalMs,
              loaders: devTiming.loaders,
              renderMs: devTiming.renderMs,
              timestamp: Date.now(),
            },
          });
        } catch (err) {
          console.error("SSR Error:", err);

          // Send enriched error to dev toolbar + overlay
          const error = err as Error;
          const errorPayload = parseError(
            error,
            'unknown',
            rootDir,
            matchedRouteId,
            req.url
          );
          server.ws.send({
            type: "custom",
            event: "neutron:dev-toolbar:error",
            data: errorPayload,
          });

          next(err as Error);
        }
      });
    },

    resolveId(id, importer, context) {
      if (!context?.ssr && isServerOnlySpecifier(id)) {
        return EMPTY_SERVER_MODULE_ID;
      }
      // Keep the server-only content runtime (and its node: builtins) out of
      // client bundles, whichever way a client-reachable module reaches it.
      if (!context?.ssr && isContentModuleId(id, importer ?? undefined)) {
        return CONTENT_CLIENT_STUB_ID;
      }

      if (id === "virtual:neutron/routes") {
        return "\0virtual:neutron/routes";
      }
      if (id === "virtual:neutron/manifest") {
        return "\0virtual:neutron/manifest";
      }
      if (id === ISLAND_VIRTUAL_ID) {
        return ISLAND_VIRTUAL_RESOLVED_ID;
      }
      if (id === DEV_TOOLBAR_MODULE_ID) {
        return DEV_TOOLBAR_RESOLVED_ID;
      }
      return null;
    },

    async load(id) {
      if (id === EMPTY_SERVER_MODULE_ID) {
        return "export default undefined;";
      }

      if (id === CONTENT_CLIENT_STUB_ID) {
        return CONTENT_CLIENT_STUB_SOURCE;
      }

      if (id.includes(`?${CLIENT_ROUTE_QUERY}`)) {
        const sourcePath = resolveRouteSourcePath(id);
        const source = requireSource(sourcePath);
        let result = stripServerOnlyRouteModule(source);
        // In dev mode, strip CSS imports — already loaded via <link> tags in SSR HTML.
        // Keeping them would cause Vite to inject duplicate <style> tags,
        // triggering redundant style recalculations and microfreezes.
        // In production builds, keep CSS imports so Vite can extract them to files.
        if (!isBuild) {
          result = result.replace(/^import\s+['"][^'"]+\.css['"];?\s*$/gm, "");
        }
        return result;
      }

      if (id === ISLAND_VIRTUAL_RESOLVED_ID) {
        return generateIslandsModule(state.islandIds);
      }
      if (id === "\0virtual:neutron/routes") {
        return generateRoutesModule(state.routes);
      }
      if (id === "\0virtual:neutron/manifest") {
        return generateManifestModule(state.routes);
      }
      if (id === DEV_TOOLBAR_RESOLVED_ID) {
        const { generateDevToolbarModule } = await import("./dev-toolbar.js");
        return generateDevToolbarModule(state.routes);
      }
      return null;
    },

    transformIndexHtml: {
      order: "post" as const,
      handler(_html: string, ctx: { server?: ViteDevServer }) {
        if (!ctx.server) return [];
        return [
          {
            tag: "script",
            attrs: { type: "module", src: `/@id/__x00__${DEV_TOOLBAR_MODULE_ID}` },
            injectTo: "body" as const,
          },
        ];
      },
    },

    transform(code, id, options) {
      if (id.startsWith("\0")) {
        return null;
      }

      const cleanId = normalizePathForComparison(stripQueryFromId(id));
      if (!isScriptModuleId(cleanId)) {
        return null;
      }

      // Inject a stable `__src="<resolved module id>"` prop into every
      // `<Island component={X}>` tag so the marker carries the island module's
      // root-relative path (minification-safe; component *names* get mangled).
      // This runs for BOTH route files and component files (islands can be
      // wrapped anywhere), and for BOTH SSR and client transforms: SSR needs it
      // so the prerendered HTML stamps `data-src` on the marker; the client
      // build needs it so the standalone runtime can dynamic-import the chunk.
      // It must run before the JSX is lowered by preset-vite.
      let result: { code: string; map: ReturnType<MagicString["generateMap"]> } | null = null;
      if (!isPathOutsideSrc(cleanId, srcDir) && code.includes("<Island")) {
        const injected = injectIslandSources(code, cleanId, rootDir);
        if (injected) {
          for (const islandId of injected.ids) state.islandIds.add(islandId);
          result = injected.result;
          code = injected.result.code;
        }
      }

      // The remaining checks (.server import guard) only apply to client
      // transforms; SSR modules legitimately import server-only code.
      if (options?.ssr) {
        return result;
      }

      const isRouteModule =
        cleanId === normalizedRoutesDir || cleanId.startsWith(normalizedRoutesDir + "/");
      if (isRouteModule) {
        return result;
      }

      // Skip files already transformed by prefresh — their HMR preamble makes
      // them unparseable by our Babel config, and client files with HMR wrappers
      // can't meaningfully import .server modules anyway.
      if (code.includes("$RefreshReg$") || code.includes("__PREFRESH__")) {
        return result;
      }

      if (!hasServerOnlyImport(code)) {
        return result;
      }

      this.error(
        `Client module "${id}" imports a .server module. ` +
          `.server modules can only be imported by route server exports (loader/action/etc) or other .server files.`
      );
      return result;
    },
  };
}

/**
 * Collect CSS URLs by walking the SSR module graph from loaded route files.
 * After ssrLoadModule, CSS imports are tracked in the environment module graph.
 * We inject these as <link> tags so CSS loads before content renders (prevents FOUC).
 */
function collectCssFromModuleGraph(server: ViteDevServer, routeFiles: string[]): string[] {
  const cssUrls: string[] = [];
  const visited = new Set<string>();

  // Try the SSR environment module graph (Vite 6+)
  const ssrEnv = (server as unknown as Record<string, unknown>).environments as
    | Record<string, { moduleGraph?: { getModulesByFile?(f: string): Set<{ url: string; file: string | null; importedModules: Set<unknown> }> | undefined } }>
    | undefined;
  const ssrModuleGraph = ssrEnv?.ssr?.moduleGraph;

  function walkSsr(mod: { url: string; file: string | null; importedModules: Set<unknown> }) {
    const key = mod.url;
    if (visited.has(key)) return;
    visited.add(key);

    if (mod.file && /\.css($|\?)/.test(mod.file)) {
      cssUrls.push(mod.url);
    }

    for (const dep of mod.importedModules) {
      walkSsr(dep as typeof mod);
    }
  }

  if (ssrModuleGraph?.getModulesByFile) {
    for (const file of routeFiles) {
      const mods = ssrModuleGraph.getModulesByFile(file);
      if (mods) {
        for (const mod of mods) walkSsr(mod);
      }
    }
  }

  // Fallback: scan the legacy module graph
  if (cssUrls.length === 0) {
    for (const mods of server.moduleGraph.fileToModulesMap.values()) {
      for (const mod of mods) {
        if (mod.file && /\.css($|\?)/.test(mod.file) && !visited.has(mod.url)) {
          visited.add(mod.url);
          cssUrls.push(mod.url);
        }
      }
    }
  }

  // Last resort: find CSS files in the project src/ directory
  if (cssUrls.length === 0) {
    const srcDir = path.resolve("src");
    if (fs.existsSync(srcDir)) {
      for (const entry of fs.readdirSync(srcDir)) {
        if (entry.endsWith(".css")) {
          cssUrls.push(`/src/${entry}`);
        }
      }
    }
  }

  return cssUrls;
}

function normalizePathname(pathname: string): string {
  if (!pathname) {
    return "/";
  }
  if (pathname.length > 1 && pathname.endsWith("/")) {
    return pathname.slice(0, -1);
  }
  return pathname;
}

function applyRouteRuleHeadersToResponse(
  response: Response,
  ruleHeaders: Array<{ headers: Record<string, string> }>
): void {
  for (const rule of ruleHeaders) {
    for (const [name, value] of Object.entries(rule.headers)) {
      try {
        if (!response.headers.has(name)) {
          response.headers.set(name, value);
        }
      } catch {
        // Some Response instances can expose immutable headers (e.g. redirects).
      }
    }
  }
}

async function resolveDevHeadHtml(
  allRoutes: Route[],
  moduleCache: Map<string, RouteModule>,
  args: HeadArgs
): Promise<{ headHtml: string; seo: SeoMetaInput | null }> {
  let mergedSeo: SeoMetaInput | null = null;
  const headFragments: string[] = [];

  for (const route of allRoutes) {
    const mod = moduleCache.get(route.id);
    if (!mod?.head) continue;
    const resolved = await mod.head({ ...args, data: args.loaderData[route.id] });
    if (!resolved) continue;
    if (typeof resolved === "string") {
      headFragments.push(sanitizeHeadHtml(resolved));
      continue;
    }
    mergedSeo = mergeSeoMetaInput(mergedSeo, resolved);
  }

  return {
    headHtml: renderDocumentHead(args.pathname, mergedSeo, headFragments),
    seo: mergedSeo,
  };
}

async function handleRequest(
  server: ViteDevServer,
  route: Route,
  params: Record<string, string>,
  layoutChain: Route[],
  allRoutes: Route[],
  request: Request,
  context: AppContext,
  rootDir: string,
  clientEntry: string | null,
  devTiming?: DevTimingContext
): Promise<Response> {
  // Load the renderer + preact through the dev server's module graph so they
  // share ONE preact instance with the route components (resolved via
  // ssrLoadModule). A native fallback would bind a second options object and
  // crash hooks with "reading '__H'". CLI/dev set absolute preact aliases so
  // preact-render-to-string is resolvable even when the app doesn't declare it.
  const rts = (await server.ssrLoadModule("preact-render-to-string")) as {
    renderToString?: typeof import("preact-render-to-string").renderToString;
    default?: { renderToString?: typeof import("preact-render-to-string").renderToString };
  };
  const resolved = rts.renderToString ?? rts.default?.renderToString;
  const appH = ((await server.ssrLoadModule("preact")) as {
    h?: typeof import("preact").h;
  }).h;
  if (!resolved || !appH) {
    throw new Error(
      "[Neutron] preact / preact-render-to-string not resolvable through the Vite SSR graph. " +
        "Ensure the CLI/dev config applies resolvePreactSsr aliases."
    );
  }
  const renderToString = resolved;
  const h = appH;

  const isMutation = ["POST", "PUT", "PATCH", "DELETE"].includes(request.method);
  const wantsJson =
    request.headers.get("X-Neutron-Data") === "true" ||
    (request.headers.get("Accept") || "").includes("application/json");
  let actionData: unknown = undefined;
  const requestedRouteIds = resolveRequestedRouteIds(
    request,
    allRoutes,
    isMutation
  );

  // Load all route modules first
  const moduleCache = new Map<string, RouteModule>();
  for (const r of allRoutes) {
    const loaded = await server.ssrLoadModule(r.file);
    moduleCache.set(r.id, loaded as RouteModule);
  }

  // Handle action for mutations
  if (isMutation) {
    const module = moduleCache.get(route.id)!;
    if (module.action) {
      const actionArgs: ActionArgs = { request, params, context };
      try {
        actionData = await module.action(actionArgs);
        if (actionData instanceof Response) {
          return actionData;
        }
      } catch (error) {
        if (error instanceof Response) {
          return error;
        }
        // Action error - send enriched error to overlay
        const actionErrorPayload = parseError(error as Error, 'action', rootDir, route.id, request.url);
        server.ws.send({ type: "custom", event: "neutron:dev-toolbar:error", data: actionErrorPayload });
        return renderError(server, route, layoutChain, error as Error, request, moduleCache, clientEntry);
      }
    }
  }

  // PARALLEL LOADER EXECUTION
  // All loaders run simultaneously with Promise.all
  const loaderPromises = allRoutes.map(async (r) => {
    const module = moduleCache.get(r.id)!;
    if (!module.loader) {
      return { routeId: r.id, data: undefined, module };
    }

    if (requestedRouteIds && !requestedRouteIds.has(r.id)) {
      return { routeId: r.id, data: undefined, module };
    }

    const loaderArgs: LoaderArgs = {
      request,
      params: r.id === route.id ? params : {},
      context,
    };

    const loaderStart = performance.now();
    try {
      const data = await module.loader(loaderArgs);
      if (devTiming) {
        devTiming.loaders.push({ routeId: r.id, ms: performance.now() - loaderStart });
      }
      return { routeId: r.id, data, module };
    } catch (error) {
      if (devTiming) {
        devTiming.loaders.push({ routeId: r.id, ms: performance.now() - loaderStart });
      }
      return { routeId: r.id, data: null, module, error: error as Error };
    }
  });

  const loaderResults = await Promise.all(loaderPromises);

  // Check for loader errors
  const loaderError = loaderResults.find((r) => r.error);
  if (loaderError?.error) {
    if (loaderError.error instanceof Response) return loaderError.error;
    const loaderErrorPayload = parseError(loaderError.error, 'loader', rootDir, loaderError.routeId, request.url);
    server.ws.send({ type: "custom", event: "neutron:dev-toolbar:error", data: loaderErrorPayload });
    return renderError(server, route, layoutChain, loaderError.error, request, moduleCache, clientEntry);
  }

  const pageResult = loaderResults.find((r) => r.routeId === route.id);
  if (!pageResult?.module?.default) {
    return new Response("Not found", { status: 404 });
  }

  const loaderData: Record<string, unknown> = {};
  for (const result of loaderResults) {
    if (result.data !== undefined) {
      loaderData[result.routeId] = result.data;
    }
  }

  // Resolve head() exports (mirrors production server behavior)
  const pathname = new URL(request.url).pathname;
  const { headHtml, seo: mergedSeo } = await resolveDevHeadHtml(allRoutes, moduleCache, {
    request,
    params,
    context,
    loaderData,
    actionData,
    pathname,
  });

  // For JSON requests (client-side navigation), return data + head + CSS
  if (wantsJson) {
    const allData: Record<string, unknown> = { ...loaderData };
    if (actionData !== undefined) {
      allData.__action__ = actionData;
    }
    allData.__head__ = headHtml;
    allData.__css__ = collectCssFromModuleGraph(server, allRoutes.map((r) => r.file));
    return new Response(encodeSerializedPayloadAsJson(allData), {
      headers: { "Content-Type": "application/json" },
    });
  }

  // Render page
  try {
    const pageData = loaderData[route.id];
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let element: any = h(pageResult.module.default as any, {
      data: pageData,
      params,
      actionData,
    });

    for (const layoutRoute of [...layoutChain].reverse()) {
      const layoutResult = loaderResults.find((r) => r.routeId === layoutRoute.id);
      if (layoutResult?.module?.default) {
        const layoutData = loaderData[layoutRoute.id];
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        element = h(layoutResult.module.default as any, { data: layoutData }, element);
      }
    }

    const renderStart = performance.now();
    const html = renderToString(element);
    if (devTiming) {
      devTiming.renderMs = performance.now() - renderStart;
    }

    // Dev-mode accessibility checks
    checkAccessibility(html, route.id);

    // Warn if a component rendered a <head> tag inside the component tree
    if (/<head[\s>]/i.test(html)) {
      console.warn(
        `[neutron] Route "${route.id}" renders a <head> element inside the component tree. ` +
          `This will be placed inside <body> and will not work correctly.\n` +
          `  Use the head() export instead: export function head() { return { title: "..." }; }`
      );
    }

    // The rendered output is mounted inside the shell's `<div id="app">`
    // (wrapHtml owns `<html>`/`<head>`/`<body>`). A full-document render would
    // nest a second document inside #app — reject it with an actionable error.
    assertRenderedFragment(html, layoutChain[0]?.file ?? route.file);

    const fullHtml = wrapHtml(html, route, request, loaderData, clientEntry, actionData, headHtml, mergedSeo);

    return new Response(fullHtml, {
      headers: { "Content-Type": "text/html; charset=utf-8" },
    });
  } catch (error) {
    // Render error - send enriched error to overlay
    const renderErrorPayload = parseError(error as Error, 'render', rootDir, route.id, request.url);
    server.ws.send({ type: "custom", event: "neutron:dev-toolbar:error", data: renderErrorPayload });
    return renderError(server, route, layoutChain, error as Error, request, moduleCache, clientEntry);
  }
}

function resolveRequestedRouteIds(
  request: Request,
  routes: Route[],
  isMutation: boolean
): Set<string> | null {
  if (isMutation || request.headers.get("X-Neutron-Data") !== "true") {
    return null;
  }

  const raw = request.headers.get("X-Neutron-Routes");
  if (!raw) {
    return null;
  }

  const allowed = new Set(routes.map((route) => route.id));
  const filtered = raw
    .split(",")
    .map((token) => token.trim())
    .filter((token) => token.length > 0 && allowed.has(token));

  if (filtered.length === 0) {
    return null;
  }

  return new Set(filtered);
}

async function renderError(
  server: ViteDevServer,
  route: Route,
  layoutChain: Route[],
  error: Error,
  request: Request,
  moduleCache: Map<string, RouteModule>,
  clientEntry: string | null
): Promise<Response> {
  // Same single-instance rule as the main SSR path — no native fallback.
  const rts = (await server.ssrLoadModule("preact-render-to-string")) as {
    renderToString?: typeof import("preact-render-to-string").renderToString;
    default?: { renderToString?: typeof import("preact-render-to-string").renderToString };
  };
  const resolved = rts.renderToString ?? rts.default?.renderToString;
  const appH = ((await server.ssrLoadModule("preact")) as {
    h?: typeof import("preact").h;
  }).h;
  if (!resolved || !appH) {
    throw new Error(
      "[Neutron] preact / preact-render-to-string not resolvable through the Vite SSR graph."
    );
  }
  const renderToString = resolved;
  const h = appH;

  // Find nearest ErrorBoundary (route first, then layouts)
  const module = moduleCache.get(route.id);
  let ErrorBoundary = module?.ErrorBoundary;

  if (!ErrorBoundary) {
    for (const layout of layoutChain) {
      const layoutModule = moduleCache.get(layout.id);
      if (layoutModule?.ErrorBoundary) {
        ErrorBoundary = layoutModule.ErrorBoundary;
        break;
      }
    }
  }

  // No ErrorBoundary - render default error page
  if (!ErrorBoundary) {
    const html = renderDefaultError(error);
    return new Response(html, {
      status: 500,
      headers: { "Content-Type": "text/html; charset=utf-8" },
    });
  }

  // Render ErrorBoundary
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const element = h(ErrorBoundary as any, { error } as ErrorBoundaryProps);
  const html = renderToString(element);
  const fullHtml = wrapHtml(html, route, request, {}, clientEntry);

  return new Response(fullHtml, {
    status: 500,
    headers: { "Content-Type": "text/html; charset=utf-8" },
  });
}

function renderDefaultError(error: Error): string {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Error</title>
  <style>
    body { 
      font-family: system-ui, sans-serif; 
      background: #0A0A0A; 
      color: #EDEDED; 
      padding: 2rem;
      margin: 0;
    }
    .error-container {
      max-width: 800px;
      margin: 0 auto;
    }
    h1 { color: #FF4444; margin-top: 0; }
    pre {
      background: #141414;
      padding: 1rem;
      border-radius: 8px;
      overflow-x: auto;
      border: 1px solid #333;
    }
    .message { font-size: 1.25rem; margin-bottom: 1rem; }
    .stack { font-size: 0.875rem; color: #888; }
  </style>
</head>
<body>
  <div class="error-container">
    <h1>Application Error</h1>
    <p class="message">${escapeHtml(error.message)}</p>
    ${error.stack ? `<pre class="stack">${escapeHtml(error.stack)}</pre>` : ''}
    <p style="margin-top: 2rem; color: #666;">
      Add an <code>ErrorBoundary</code> export to customize this page.
    </p>
  </div>
</body>
</html>`;
}

async function createRequest(req: import("http").IncomingMessage, url: URL): Promise<Request> {
  const MAX_DEV_BODY = 10 * 1024 * 1024; // 10MB
  const chunks: Uint8Array[] = [];
  let totalSize = 0;

  for await (const chunk of req) {
    totalSize += chunk.length;
    if (totalSize > MAX_DEV_BODY) {
      throw new Error('Request body too large');
    }
    chunks.push(chunk);
  }

  const body = chunks.length > 0 ? Buffer.concat(chunks) : undefined;

  return new Request(url.toString(), {
    method: req.method || "GET",
    headers: req.headers as HeadersInit,
    body: body && req.method !== "GET" && req.method !== "HEAD" ? body : undefined,
  });
}

function getLayoutChain(route: Route, allRoutes: Route[]): Route[] {
  const chain: Route[] = [];
  let currentId: string | null = route.parentId;

  while (currentId) {
    const parent = allRoutes.find((r) => r.id === currentId);
    if (parent) {
      chain.push(parent);
      currentId = parent.parentId;
    } else {
      break;
    }
  }

  return chain;
}

function wrapHtml(
  content: string,
  route: Route,
  request: Request,
  loaderData: Record<string, unknown>,
  clientEntry: string | null,
  actionData?: unknown,
  headHtml?: string,
  seo?: SeoMetaInput | null
): string {
  const pathname = new URL(request.url).pathname;
  const resolvedHead = headHtml || renderDocumentHead(pathname, null);

  const allData: Record<string, unknown> = { ...loaderData };
  if (actionData !== undefined) {
    allData.__action__ = actionData;
  }

  const dataScript = `<script>window.__NEUTRON_DATA_SERIALIZED__=${serializeForInlineScript(allData)};</script>`;
  const clientScript = clientEntry
    ? `<script type="module" src="${clientEntry}"></script>`
    : "";

  const htmlTag = buildHtmlOpenTag(seo?.htmlAttrs);
  const bodyTag = buildBodyOpenTag(seo?.bodyAttrs);

  return `<!DOCTYPE html>
${htmlTag}
<head>
${resolvedHead}
</head>
${bodyTag}
<div id="app">${content}</div>
${dataScript}
${clientScript}
</body>
</html>`;
}

export function generateRoutesModule(routes: Route[]): string {
  const cwd = process.cwd().replace(/\\/g, "/");
  const routeMap: string[] = routes.map((route) => {
    const absPath = route.file.replace(/\\/g, "/");
    // Make path relative to project root so Vite can resolve it.
    // Absolute paths would produce "//Users/..." which browsers treat as protocol-relative URLs.
    const relativePath = absPath.startsWith(cwd + "/")
      ? absPath.slice(cwd.length)
      : "/@fs" + absPath;
    return `  ${JSON.stringify(route.id)}: {
    id: ${JSON.stringify(route.id)},
    path: ${JSON.stringify(route.path)},
    parentId: ${route.parentId ? JSON.stringify(route.parentId) : "null"},
    isLayout: ${!!route.isLayout},
    mode: ${JSON.stringify(route.config.mode)},
    hasLoader: ${route.hasLoader !== false},
    load: () => import(${JSON.stringify(relativePath + "?" + CLIENT_ROUTE_QUERY)})
  }`;
  });

  return `export const routes = {
${routeMap.join(",\n")}
};

export const routeIds = ${JSON.stringify(routes.map((r) => r.id))};
`;
}

function generateManifestModule(routes: Route[]): string {
  const manifest = routes.map((route) => ({
    id: route.id,
    path: route.path,
    params: route.params,
    config: route.config,
    parentId: route.parentId,
  }));

  return `export const manifest = ${JSON.stringify(manifest, null, 2)};\n`;
}

export default neutronPlugin;

// ---------------------------------------------------------------------------
// Standalone islands: module-path ids + virtual manifest
// ---------------------------------------------------------------------------

/**
 * Generate the `virtual:neutron-islands` module: maps each island's
 * root-relative module id to a static `import()` thunk. Rollup auto-code-splits
 * each import into its own chunk and rewrites the URL in dev AND prod, so the
 * standalone islands entry can lazy-load exactly the island code a page needs.
 */
function generateIslandsModule(ids: Set<string>): string {
  const entries = [...ids]
    .sort()
    .map((id) => `  ${JSON.stringify(id)}: () => import(${JSON.stringify(id)})`)
    .join(",\n");
  return `export const islands = {\n${entries}\n};\n`;
}

function isPathOutsideSrc(cleanId: string, srcDir: string): boolean {
  const normalizedSrc = normalizePathForComparison(srcDir);
  return !(cleanId === normalizedSrc || cleanId.startsWith(normalizedSrc + "/"));
}

const ISLAND_TAG_RE = /<Island\b([^>]*?)\bcomponent=\{\s*([A-Za-z_$][\w$]*)\s*\}/g;

/**
 * Find every `<Island ... component={X}>` tag, resolve X's import source to a
 * root-relative module id, and inject `__src="<id>"` into the tag. Returns the
 * magic-string result plus the set of resolved island ids (for the manifest).
 */
function injectIslandSources(
  code: string,
  cleanId: string,
  rootDir: string
): { result: { code: string; map: ReturnType<MagicString["generateMap"]> }; ids: string[] } | null {
  ISLAND_TAG_RE.lastIndex = 0;
  const s = new MagicString(code);
  const ids: string[] = [];
  let changed = false;
  let match: RegExpExecArray | null;

  while ((match = ISLAND_TAG_RE.exec(code)) !== null) {
    const componentName = match[2];
    // Skip if this tag already has an __src (idempotent re-transform).
    const tagStart = match.index;
    const tagSlice = code.slice(tagStart, tagStart + match[0].length + 1);
    if (/\b__src=/.test(tagSlice)) continue;

    const id = resolveImportSourceId(code, componentName, cleanId, rootDir);
    if (!id) continue;

    // Insert the prop immediately after `<Island` so it survives whatever
    // attribute layout follows.
    const insertAt = tagStart + "<Island".length;
    s.appendLeft(insertAt, ` __src=${JSON.stringify(id)}`);
    ids.push(id);
    changed = true;
  }

  if (!changed) return null;
  return {
    result: { code: s.toString(), map: s.generateMap({ hires: true }) },
    ids,
  };
}

/**
 * Given a component identifier used in JSX, find its import statement and
 * resolve the specifier to a root-relative module id (e.g.
 * "/src/components/Counter.tsx"). Returns null for bare/package imports — those
 * can't be code-split into a per-island chunk by path id.
 */
function resolveImportSourceId(
  code: string,
  componentName: string,
  importerCleanId: string,
  rootDir: string
): string | null {
  const escaped = componentName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  // default import:  import Counter from "..."
  // named import:    import { Counter } from "..."  /  import { X as Counter } from "..."
  const importRe = new RegExp(
    `import\\s+(?:${escaped}\\b|(?:[\\w$]+\\s*,\\s*)?\\{[^}]*\\b${escaped}\\b[^}]*\\})\\s*from\\s*["']([^"']+)["']`
  );
  const m = code.match(importRe);
  if (!m) return null;
  const specifier = m[1];

  // Only relative specifiers map to a project module id.
  if (!specifier.startsWith(".") && !specifier.startsWith("/")) {
    return null;
  }

  const importerAbs = importerCleanId; // already absolute, forward-slashed
  const importerDir = path.posix.dirname(importerAbs.replace(/\\/g, "/"));
  let resolvedAbs: string;
  if (specifier.startsWith("/")) {
    resolvedAbs = path.posix.join(normalizePathForComparison(rootDir), specifier.slice(1));
  } else {
    resolvedAbs = path.posix.normalize(path.posix.join(importerDir, specifier));
  }

  const onDisk = resolveModuleFileOnDisk(resolvedAbs);
  if (!onDisk) return null;

  const rootFwd = normalizePathForComparison(rootDir);
  if (!onDisk.startsWith(rootFwd + "/")) return null;
  return onDisk.slice(rootFwd.length); // leading slash retained, e.g. /src/...
}

/**
 * Resolve a module specifier (which may carry a `.js` extension that actually
 * refers to a `.tsx` source, or no extension at all) to the real file on disk.
 */
function resolveModuleFileOnDisk(resolvedAbs: string): string | null {
  const tryFiles: string[] = [];
  const extMatch = resolvedAbs.match(/\.(js|jsx|mjs|cjs|ts|tsx)$/);
  if (extMatch) {
    // Exact file, then ".js" → ".tsx"/".ts"/... rewrites.
    tryFiles.push(resolvedAbs);
    const base = resolvedAbs.slice(0, resolvedAbs.length - extMatch[0].length);
    for (const ext of ISLAND_SCRIPT_EXTENSIONS) tryFiles.push(base + ext);
  } else {
    for (const ext of ISLAND_SCRIPT_EXTENSIONS) tryFiles.push(resolvedAbs + ext);
    for (const ext of ISLAND_SCRIPT_EXTENSIONS) {
      tryFiles.push(path.posix.join(resolvedAbs, "index" + ext));
    }
  }
  for (const candidate of tryFiles) {
    if (fs.existsSync(candidate) && fs.statSync(candidate).isFile()) {
      return candidate;
    }
  }
  return null;
}

/**
 * Eager full-source scan: walk every script file under `src/` looking for
 * `<Island component={X}>` tags and resolve each X to a root-relative module
 * id. Used to populate the `virtual:neutron-islands` manifest at build time,
 * since the lazy per-module `transform` only sees modules Vite happens to load.
 */
function scanIslandIds(srcDir: string, rootDir: string): Set<string> {
  const ids = new Set<string>();
  if (!fs.existsSync(srcDir)) return ids;

  const walk = (dir: string) => {
    let entries: fs.Dirent[];
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      if (entry.name === "node_modules" || entry.name.startsWith(".")) continue;
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        walk(full);
      } else if (/\.(tsx|ts|jsx|js|mjs|cjs)$/.test(entry.name)) {
        let src: string;
        try {
          src = fs.readFileSync(full, "utf-8");
        } catch {
          continue;
        }
        if (!src.includes("<Island")) continue;
        const cleanId = normalizePathForComparison(full);
        ISLAND_TAG_RE.lastIndex = 0;
        let match: RegExpExecArray | null;
        while ((match = ISLAND_TAG_RE.exec(src)) !== null) {
          const id = resolveImportSourceId(src, match[2], cleanId, rootDir);
          if (id) ids.add(id);
        }
      }
    }
  };

  walk(srcDir);
  return ids;
}

function normalizePathForComparison(filePath: string): string {
  return filePath.replace(/\\/g, "/").replace(/\/+$/, "");
}

function resolveRouteSourcePath(id: string): string {
  const clean = stripQueryFromId(id);
  const normalized = clean.replace(/\\/g, "/");
  if (/^\/[A-Za-z]:\//.test(normalized)) {
    return normalized.slice(1);
  }
  if (path.isAbsolute(normalized)) {
    return normalized;
  }
  if (normalized.startsWith("/")) {
    return path.resolve(process.cwd(), normalized.slice(1));
  }
  return path.resolve(process.cwd(), normalized);
}

function requireSource(filePath: string): string {
  return fs.readFileSync(filePath, "utf-8");
}

/** Strip <script> tags, event handler attributes, and javascript: URLs from head HTML fragments. */
function sanitizeHeadHtml(html: string): string {
  // Strip script tags and their contents
  html = html.replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, '');
  // Strip event handler attributes
  html = html.replace(/\s+on\w+\s*=\s*(?:"[^"]*"|'[^']*'|[^\s>]*)/gi, '');
  // Strip javascript: URLs
  html = html.replace(/(?:href|src|action)\s*=\s*(?:"javascript:[^"]*"|'javascript:[^']*')/gi, '');
  return html;
}

function isScriptModuleId(id: string): boolean {
  const ext = path.extname(id).toLowerCase();
  return ext === ".js" || ext === ".mjs" || ext === ".cjs" || ext === ".ts" || ext === ".tsx" || ext === ".jsx";
}
