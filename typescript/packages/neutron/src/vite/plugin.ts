import * as fs from "node:fs";
import * as path from "node:path";
import { randomUUID } from "node:crypto";
import MagicString from "magic-string";
import type { Plugin, ViteDevServer } from "vite";
import { escapeHtml } from "../core/escape.js";
import { assertRenderedFragment } from "../core/fragment-guard.js";
import { discoverRoutes } from "../core/manifest.js";
import { prepareRouteTypes } from "../core/route-typegen.js";
import { createRouter } from "../core/router.js";
import { runMiddlewareChain } from "../core/middleware.js";
import { findGlobalMiddlewareFile, loadGlobalMiddleware } from "../core/global-middleware.js";
import { isNotFoundSignal, isResponse, mutableResponse, notFound } from "../core/response.js";
import { dispatchResourceHandler, isResourceRoute } from "../core/resource-routes.js";
import { asNotFoundResponse, declareRepresentationDimensions, resolveClientTier, resolveRouteHeaders } from "../core/render-app-route.js";
import { transformSsrCommonJs } from "./ssr-cjs.js";
import { toDataRedirectResponse } from "../core/data-redirect.js";
import { applySecurityHeaders, resolveSecurityHeadersConfig } from "../server/http-headers.js";
import { Hono } from "hono";
import type { WebSocketServer } from "ws";
import {
  findServerEntryFile,
  resolveServerEntryModule,
  type NeutronServerEntry,
} from "../core/server-entry.js";
import {
  applyRouteRuleHeaders,
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
import { renderDocumentHead, mergeSeoMetaInput, buildHtmlOpenTag, buildBodyOpenTag, type DocumentHeadTag } from "../core/seo.js";
import { extractDocumentHead } from "../core/document-head.js";
import type { SeoMetaInput } from "../core/seo.js";
import type { NeutronClientConfig, NeutronRoutesConfig } from "../config.js";
import type { Route, RouteModule, AppContext, LoaderArgs, ActionArgs, HeadArgs, MiddlewareFn, ErrorBoundaryProps } from "../core/types.js";
import { handleImageRequest } from "../server/image-optimizer.js";
import { appDefinesHealthRoute, DEFAULT_HEALTH_VERSION, healthBody } from "../server/health.js";
import {
  appDefinesSpecRoute,
  serverOpenApiSpec,
  swaggerDocsHtml,
  type NeutronOpenApiOptions,
} from "../server/openapi.js";
import { checkAccessibility } from "./a11y-checker.js";
import {
  analyzeServerActionModule,
  generateServerActionStub,
  mayContainUseServer,
  serverActionModuleKey,
  ServerActionRegistry,
} from "./server-actions.js";
import { handleServerActionRequest, type ServerActionsOptions } from "../core/server-actions.js";
import { SERVER_ACTION_PATH, DEFAULT_SERVER_ACTION_BODY_LIMIT } from "../core/server-action-protocol.js";
import { parseError } from "./error-parser.js";

export interface NeutronPluginOptions {
  routesDir?: string;
  rootDir?: string;
  writeRouteTypes?: boolean;
  routeRules?: NeutronRoutesConfig;
  /** Version the dev server's GET /health reports (`server.version` in neutron.config). */
  version?: string;
  /** `server.openapi` in neutron.config: serve /openapi.json and /docs as `start` does. */
  openapi?: NeutronOpenApiOptions;
  /**
   * `server.securityHeaders` in neutron.config. The dev server sends the same
   * security headers as `start` (defaults on; `false` disables), with
   * `routes.headers` rules overriding them per path.
   */
  securityHeaders?: false | { headers?: Record<string, string> };
  /**
   * Server Actions. `registry` collects the `"use server"` modules the CLIENT
   * graph reaches (`neutron-ts build` shares one across its client passes and
   * writes it to dist); the dev server serves the ones its own registry holds.
   * `bodySizeLimit`/`allowedOrigins` are `server.serverActions` in neutron.config.
   */
  serverActions?: ServerActionsOptions & { registry?: ServerActionRegistry };
  /** neutron.config.ts `client`: client router options, emitted into `virtual:neutron/routes`. */
  client?: NeutronClientConfig;
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
export const renderEntry = serverOnly("renderEntry");
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

// Every virtual module this plugin serves (`virtual:neutron-islands`,
// `virtual:neutron/routes`, `virtual:neutron/manifest`,
// `virtual:neutron/dev-toolbar`) shares the `virtual:neutron` namespace.
const VIRTUAL_NAMESPACE_FILTER = /^virtual:neutron/;

// The bundler behind Vite's dep optimizer — esbuild up to Vite 7, Rolldown
// from Vite 8 — scans and pre-bundles without Vite plugins, so it cannot load
// this plugin's virtual modules; an unresolvable id aborts the optimization
// and takes the dev server down with it. Marking the whole namespace external
// makes the optimizer leave those imports in place, and Vite's import
// analysis resolves them through this plugin when the module is served.
// Structurally typed: vite passes these straight through to the optimizer's
// bundler in both passes. One plugin per bundler, same rule.
const virtualNamespaceExternalPlugin = {
  name: "neutron:virtual-namespace-external",
  setup(build: {
    onResolve: (
      options: { filter: RegExp },
      callback: (args: { path: string }) => { path: string; external: true }
    ) => void;
  }): void {
    build.onResolve({ filter: VIRTUAL_NAMESPACE_FILTER }, (args) => ({
      path: args.path,
      external: true,
    }));
  },
};

const virtualNamespaceExternalRolldownPlugin = {
  name: "neutron:virtual-namespace-external",
  resolveId(id: string): { id: string; external: true } | null {
    return VIRTUAL_NAMESPACE_FILTER.test(id) ? { id, external: true } : null;
  },
};

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

// Marks the dev server's "let Vite answer" response for unmatched paths.
const DEV_PASS_THROUGH_HEADER = "x-neutron-dev-pass-through";
const devPassThroughToken = randomUUID();

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
/**
 * Writes a built-in contract endpoint (/health, /openapi.json, /docs) the way
 * the production server's request-id middleware does: an inbound
 * `x-request-id` is echoed, otherwise one is generated. HEAD gets no body.
 */
function sendBuiltIn(
  req: import("http").IncomingMessage,
  res: import("http").ServerResponse,
  contentType: string,
  body: string
): void {
  const inboundRequestId = req.headers["x-request-id"];
  res.statusCode = 200;
  res.setHeader("Content-Type", contentType);
  res.setHeader("Content-Length", Buffer.byteLength(body));
  res.setHeader(
    "x-request-id",
    typeof inboundRequestId === "string" && inboundRequestId.length > 0
      ? inboundRequestId
      : randomUUID()
  );
  res.end(req.method === "HEAD" ? undefined : body);
}

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
  const securityHeadersConfig = resolveSecurityHeadersConfig(options.securityHeaders);
  // Route-rule headers, then the security defaults they do not override or
  // null out — the order production applies them in (server/index.ts).
  const applyResponseHeaders = (response: Response, pathname: string): void => {
    const suppressed = applyRouteRuleHeaders(
      response,
      resolveRouteRuleHeaders(compiledRouteRules, normalizePathname(pathname))
    );
    if (securityHeadersConfig) {
      applySecurityHeaders(response, securityHeadersConfig, suppressed);
    }
  };
  const clientEntry = resolveClientEntryPath(rootDir);
  const serverActionRegistry = options.serverActions?.registry ?? new ServerActionRegistry();
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

    // The client runtime imports `virtual:neutron-islands` (and friends),
    // which only this plugin can resolve. Vite's dep optimizer pre-bundles
    // dependencies with esbuild, which runs outside Vite's plugin pipeline
    // and fails the whole dev server on ids it cannot resolve. Two layers,
    // because the optimizer can reach the runtime two ways:
    //
    // - `exclude` keeps the runtime on Vite's own resolver when it is imported
    //   by its registry specifier. It is keyed on that specifier, so it does
    //   nothing for installs that change it — a `file:` install, a pnpm
    //   `link:`, a `resolve.alias` to a local checkout, or an
    //   `optimizeDeps.include` entry all route around the literal.
    // - The esbuild-level plugin closes that gap for every install shape at
    //   once: whatever path esbuild enters the runtime by, it leaves
    //   `virtual:neutron*` imports unresolved-but-external instead of failing
    //   (Vite's import analysis resolves them through this plugin when the
    //   chunk is served). Vite runs `optimizeDeps.rolldownOptions.plugins`
    //   (Vite 8+) or `optimizeDeps.esbuildOptions.plugins` (Vite 6/7) in both
    //   the dependency scanner and the pre-bundler. Vite 8 identifies itself
    //   through `this.meta.rolldownVersion` and warns on the esbuild option;
    //   Vite 6 calls this hook without a context.
    config() {
      const rolldown = this?.meta?.rolldownVersion !== undefined;
      return {
        optimizeDeps: {
          exclude: ["@neutron-build/core"],
          ...(rolldown
            ? { rolldownOptions: { plugins: [virtualNamespaceExternalRolldownPlugin] } }
            : { esbuildOptions: { plugins: [virtualNamespaceExternalPlugin] } }),
        },
      };
    },

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

      const loadDevGlobalMiddleware = async (): Promise<MiddlewareFn[]> => {
        const globalMiddlewareFile = findGlobalMiddlewareFile(rootDir);
        return globalMiddlewareFile
          ? await loadGlobalMiddleware((id) => server.ssrLoadModule(id), globalMiddlewareFile)
          : [];
      };
      const serverEntryState = createDevServerEntryState(rootDir, (id) => server.ssrLoadModule(id));
      server.httpServer?.on("upgrade", (req, socket, head) => {
        serverEntryState.upgrade(req, socket as import("node:net").Socket, head);
      });
      server.httpServer?.on("close", () => {
        void serverEntryState.close();
      });
      // src/server.entry.ts is loaded (and its WebSocket handler attached)
      // when the server starts, as start does, and again after an edit,
      // not on the first request that needs it.
      const loadServerEntry = () => {
        serverEntryState.load().catch((error) => {
          console.error("[neutron] Failed to load src/server.entry.ts:", error);
        });
      };
      server.httpServer?.once("listening", loadServerEntry);
      let serverEntryReload: ReturnType<typeof setTimeout> | undefined;
      server.watcher.on("all", (event) => {
        if (event !== "change" && event !== "add" && event !== "unlink") return;
        // After Vite has invalidated the module graph for this event.
        clearTimeout(serverEntryReload);
        serverEntryReload = setTimeout(loadServerEntry, 50);
      });

      server.middlewares.use(async (req, res, next) => {
        const requestStartMs = performance.now();
        let matchedRouteId: string | undefined;
        // A request a mount let fall through: its body was read once, and the
        // global middleware already ran ahead of the mount.
        let mountRequest: Request | undefined;
        let globalChainRan = false;
        try {
          const url = new URL(req.url || "/", `http://${sanitizeHost(req.headers.host)}`);
          const originalPathname = url.pathname;

          // Vite-internal and dev-machinery paths always bypass the router.
          if (
            originalPathname.startsWith("/@") ||
            originalPathname.startsWith("/node_modules") ||
            originalPathname === "/__vite_ping" ||
            originalPathname === "/__open-in-editor" ||
            originalPathname === "/favicon.ico"
          ) {
            return next();
          }

          // GET /health (FRAMEWORK_CONTRACT.md §7): same body and override rule
          // as the production server, so probes behave identically in dev.
          if (
            originalPathname === "/health" &&
            (req.method === "GET" || req.method === "HEAD") &&
            !appDefinesHealthRoute(state.routes)
          ) {
            sendBuiltIn(req, res, "application/json", JSON.stringify(healthBody(options.version)));
            return;
          }

          // GET /openapi.json + /docs (FRAMEWORK_CONTRACT.md §4) when
          // `server.openapi` is configured: same document, content types and
          // override rule as the production server. Checked before the
          // static-asset bypass, which would hand `/openapi.json` to Vite.
          if (
            options.openapi &&
            (originalPathname === "/openapi.json" || originalPathname === "/docs") &&
            (req.method === "GET" || req.method === "HEAD") &&
            !appDefinesSpecRoute(state.routes)
          ) {
            if (originalPathname === "/docs") {
              sendBuiltIn(req, res, "text/html; charset=UTF-8", swaggerDocsHtml(options.openapi.title));
            } else {
              const spec = serverOpenApiSpec(
                state.routes,
                options.openapi,
                options.version ?? DEFAULT_HEALTH_VERSION
              );
              sendBuiltIn(req, res, "application/json", JSON.stringify(spec));
            }
            return;
          }

          // src/server.entry.ts mounts (core/server-entry.ts), loaded through
          // this server's SSR graph like the routes, so they share module
          // instances. Production registers mounts after /health & co. and
          // before route rules and the routes, runs the global middleware
          // ahead of them, and lets a path the sub-app does not match fall
          // through to the routes with the chain already run.
          // Server Actions (core/server-actions.ts): the modules the client
          // graph has reached with "use server", through the same global
          // middleware and checks as `start`.
          if (originalPathname === SERVER_ACTION_PATH) {
            const limit = options.serverActions?.bodySizeLimit ?? DEFAULT_SERVER_ACTION_BODY_LIMIT;
            const declared = Number(req.headers["content-length"]);
            // createRequest buffers the body: an oversized one is refused
            // (413, from its declared length) without reading it.
            const actionRequest =
              Number.isFinite(declared) && declared > limit
                ? new Request(url.toString(), { method: req.method || "POST", headers: req.headers as HeadersInit })
                : await createRequest(req, url);
            const response = await handleServerActionRequest(actionRequest, {
              bodySizeLimit: options.serverActions?.bodySizeLimit,
              allowedOrigins: options.serverActions?.allowedOrigins,
              globalMiddleware: await loadDevGlobalMiddleware(),
              resolve: async (actionId) => {
                const entry = serverActionRegistry.lookup(actionId);
                if (!entry) return null;
                const mod = (await server.ssrLoadModule(path.resolve(rootDir, entry.file))) as Record<string, unknown>;
                const fn = mod[entry.export];
                return typeof fn === "function"
                  ? { fn: fn as (...args: unknown[]) => unknown, name: entry.export }
                  : null;
              },
              onError: (error) => {
                console.error("Server action error:", error);
                server.ws.send({
                  type: "custom",
                  event: "neutron:dev-toolbar:error",
                  data: parseError(error, "action", rootDir, undefined, req.url),
                });
              },
            });
            const sent = mutableResponse(response);
            applyResponseHeaders(sent, originalPathname);
            await sendWebResponse(req, res, sent);
            return;
          }

          const devMount = await serverEntryState.match(normalizePathname(originalPathname));
          if (devMount) {
            mountRequest = await createRequest(req, url);
            const chain = devMount.globalMiddleware
              ? await loadDevGlobalMiddleware()
              : [];
            let answer: Response;
            try {
              answer = await runMiddlewareChain(chain, mountRequest, {}, () =>
                Promise.resolve(devMount.dispatcher.fetch(mountRequest!))
              );
            } catch (error) {
              if (!isResponse(error)) throw error;
              answer = error;
            }
            if (answer.headers.get(DEV_PASS_THROUGH_HEADER) !== devPassThroughToken) {
              const sent = mutableResponse(toDataRedirectResponse(mountRequest, answer));
              applyResponseHeaders(sent, originalPathname);
              await sendWebResponse(req, res, sent);
              return;
            }
            globalChainRan = chain.length > 0;
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
            copyWebResponseHeaders(res, imageResponse.headers);

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
            // A data request gets the redirect reported, as in production
            // (core/data-redirect.ts).
            const answer = toDataRedirectResponse(
              new Request(url.toString(), { headers: req.headers as HeadersInit }),
              new Response(null, { status: redirect.status, headers: { Location: redirect.location } })
            );
            applyResponseHeaders(answer, originalPathname);
            res.statusCode = answer.status;
            copyWebResponseHeaders(res, answer.headers);
            res.end();
            return;
          }

          const rewrite = resolveRouteRuleRewrite(
            compiledRouteRules,
            normalizePathname(originalPathname)
          );
          const pathname = rewrite?.pathname || normalizePathname(originalPathname);

          // Static-asset bypass: only short-circuit if the LAST path segment
          // looks like a real file extension (a single trailing `.ext` with
          // alphanumeric characters). This preserves bypasses for things like
          // `/styles.css`, `/foo.js`, `/img/logo.svg` while still routing
          // dotted dynamic params such as Codex unit IDs `/u/03.09.02`, which
          // are param values rather than file paths.
          //
          // One exception, resolved through the router: a path the route
          // table claims as a RESOURCE route (`rss[.]xml.ts` → /rss.xml,
          // `feed.xml/index.ts` → /feed.xml) is dispatched, never handed to
          // Vite's static handler — production serves endpoints ahead of any
          // static file path (server/index.ts dispatches resource routes
          // before the dist fast path), and dev must answer the same. This
          // sits after redirect/rewrite resolution for the same reason the
          // match below waits: the rewritten path is the path that answers.
          if (looksLikeStaticAsset(pathname)) {
            const assetMatch = state.router.match(pathname);
            if (!assetMatch || !isResourceRoute(assetMatch.route)) {
              return next();
            }
          }

          // src/middleware.ts runs OUTERMOST for every routed request, as in
          // production (FRAMEWORK_CONTRACT §5). Resolved per request through
          // ssrLoadModule, so adding, editing or deleting the file takes effect
          // on the next request. A present file that fails to load surfaces as
          // an error rather than silently serving without its gate.
          const globalMiddleware = globalChainRan ? [] : await loadDevGlobalMiddleware();

          const match = state.router.match(pathname);
          if (!match) {
            if (globalMiddleware.length === 0) {
              return next();
            }
            // Unmatched: production answers through the global chain too. Here
            // Vite answers instead (SPA fallback, proxy, 404), so the chain runs
            // in front of it and Vite only proceeds when the chain does. The
            // request carries no body: reading it would drain the stream a
            // later Vite middleware (proxy) still needs.
            // A middleware may rebuild the response it got from next() (to
            // add headers), so the pass-through is recognised by a header the
            // rebuild carries along, not only by identity.
            const passThrough = new Response(null, {
              headers: { [DEV_PASS_THROUGH_HEADER]: devPassThroughToken },
            });
            const gateResponse = await runMiddlewareChain(
              globalMiddleware,
              new Request(url.toString(), {
                method: req.method || "GET",
                headers: req.headers as HeadersInit,
              }),
              {},
              async () => passThrough
            );
            if (
              gateResponse === passThrough ||
              gateResponse.headers.get(DEV_PASS_THROUGH_HEADER) === devPassThroughToken
            ) {
              return next();
            }
            const denied = mutableResponse(
              toDataRedirectResponse(
                new Request(url.toString(), { headers: req.headers as HeadersInit }),
                gateResponse
              )
            );
            applyResponseHeaders(denied, originalPathname);
            res.statusCode = denied.status;
            copyWebResponseHeaders(res, denied.headers);
            if (denied.body && req.method !== "HEAD") {
              res.write(Buffer.from(await denied.arrayBuffer()));
            }
            res.end();
            return;
          }

          const { route, params } = match;
          matchedRouteId = route.id;
          const layoutChain = getLayoutChain(route, state.routes);
          const allRoutes = [...layoutChain, route];

          const request = mountRequest ?? (await createRequest(req, url));
          const context: AppContext = {};

          // The app's nearest not-found page, status 404, for a 404 the page
          // itself answers (a loader's notFound(), an ungenerated static
          // param), rendered inside the chain already running: no middleware
          // of its own. Its stylesheets replace the matched route's below.
          let cssRoutes = allRoutes;
          const renderNotFound = async (notFoundRequest: Request): Promise<Response | null> => {
            const notFoundMatch = state.router.matchNotFound(pathname);
            if (!notFoundMatch) return null;
            const notFoundLayouts = getLayoutChain(notFoundMatch.route, state.routes);
            const notFoundRoutes = [...notFoundLayouts, notFoundMatch.route];
            const page = await handleRequest(server, notFoundMatch.route, notFoundMatch.params, notFoundLayouts, notFoundRoutes, notFoundRequest, {}, rootDir, clientEntry);
            cssRoutes = notFoundRoutes;
            return asNotFoundResponse(page, null);
          };

          // Dev timing context
          const devTiming: DevTimingContext = {
            loaders: [],
            renderMs: undefined,
          };

          // Collect middleware: global (outermost), then layouts + route
          const middlewares: MiddlewareFn[] = [...globalMiddleware];
          for (const r of allRoutes) {
            const loaded = await server.ssrLoadModule(r.file);
            const module = loaded as RouteModule;
            if (module.middleware) {
              middlewares.push(module.middleware);
            }
          }

          // Run middleware chain + final handler
          // Same as start: an immutable-header Response (Response.redirect())
          // still gets the route-rule headers below.
          const response = mutableResponse(
            toDataRedirectResponse(
              request,
              await runMiddlewareChain(
                middlewares,
                request,
                context,
                async () =>
                  // A static route asked for a param its getStaticPaths does
                  // not list has no file in a build, so production answers
                  // 404 there (the app's not-found page): dev does too.
                  (await isUngeneratedStaticPath(server, route, params))
                    ? ((await renderNotFound(request)) ?? notFound())
                    : handleRequest(server, route, params, layoutChain, allRoutes, request, context, rootDir, clientEntry, devTiming, renderNotFound)
              )
            )
          );
          applyResponseHeaders(response, originalPathname);

          // Send response
          copyWebResponseHeaders(res, response.headers);
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
              const cssUrls = collectCssFromModuleGraph(server, cssRoutes.map((r) => r.file));
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
                  // data-neutron-css: a route stylesheet the client router
                  // may unlink after navigation (client.unloadRouteStylesheets).
                  .map(url => `  <link rel="stylesheet" href="${url.replace(/"/g, '&quot;')}" data-neutron-css>`)
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
              const linkPattern = /<link rel="stylesheet" href="([^"]+)"(?: data-neutron-css)?>/g;
              let m: RegExpExecArray | null;
              while ((m = linkPattern.exec(transformedHtml)) !== null) {
                const href = m[1];
                if (href.includes("?direct")) {
                  directBases.add(href.replace(/\?direct$/, ""));
                }
              }
              if (directBases.size > 0) {
                transformedHtml = transformedHtml.replace(
                  /<link rel="stylesheet" href="([^"]+)"(?: data-neutron-css)?>/g,
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
          // A middleware that throws a Response is a documented short-circuit
          // (the denial path of requireOrganization()/requirePermissions()).
          // Production finalizes it as-is (server/index.ts catch-all); dev
          // must answer with the same status and body, not 500 via next(err).
          if (isResponse(err)) {
            res.statusCode = err.status;
            copyWebResponseHeaders(res, err.headers);
            if (err.body) {
              const reader = err.body.getReader();
              while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                res.write(Buffer.from(value));
              }
            }
            res.end();
            return;
          }

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
        return generateRoutesModule(state.routes, options.client);
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

      // CommonJS the SSR runner inlines cannot run there as written. Builds
      // bundle it with Rolldown instead (see vite/ssr-cjs.ts).
      if (options?.ssr && !isBuild && id.includes("/node_modules/")) {
        const cjs = transformSsrCommonJs(code, stripQueryFromId(id));
        if (cjs !== null) return { code: cjs, map: null };
      }

      const cleanId = normalizePathForComparison(stripQueryFromId(id));
      if (!isScriptModuleId(cleanId)) {
        return null;
      }

      // Server Actions (vite/server-actions.ts). App sources only: a package
      // shipping "use server" modules is not something the build registers.
      if (!cleanId.includes("/node_modules/") && mayContainUseServer(code)) {
        const label = path.relative(rootDir, cleanId).split(path.sep).join("/") || cleanId;
        const analysis = analyzeServerActionModule(code, label);
        if (analysis.errors.length > 0) {
          this.error(analysis.errors.join("\n"));
        }
        const moduleKey = serverActionModuleKey(rootDir, cleanId);
        if (analysis.isServerActionModule) {
          if (!options?.ssr) {
            // The client graph reached it: register it, and ship stubs.
            serverActionRegistry.set(moduleKey, analysis.exports);
            return { code: generateServerActionStub(moduleKey, analysis.exports), map: null };
          }
        } else if (!options?.ssr) {
          serverActionRegistry.delete(moduleKey);
        }
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
        for (const islandId of injected.ids) state.islandIds.add(islandId);
        if (injected.result) {
          result = injected.result;
          code = injected.result.code;
        }
        if (!options?.ssr) {
          for (const name of injected.unresolved) {
            this.warn(
              `<Island component={${name}}> in ${cleanId}: could not resolve ${name} to a ` +
                `project module, so its marker has no data-src and will not hydrate on ` +
                `static pages. Import the component from its own file under src/.`
            );
          }
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
 *
 * Only real imports count. A stylesheet's own `importedModules` also holds
 * Vite's file-only entries (`type: "asset"`, no id): every file a CSS plugin
 * registered with `addWatchFile`, so a change to it re-runs the transform.
 * Tailwind v4 registers each source file it scans — and where `dist/` is not
 * gitignored it scans the previous build's output, so `dist/assets/*.css`
 * hung off the app's own stylesheet and dev linked it: minified, stale CSS
 * from an old build on a dev page. Watch entries are skipped, and nothing
 * under the build output directory is ever linked.
 */
function collectCssFromModuleGraph(server: ViteDevServer, routeFiles: string[]): string[] {
  const cssUrls: string[] = [];
  const visited = new Set<string>();
  const outDir = normalizePathForComparison(
    path.resolve(server.config.root, server.config.build?.outDir ?? "dist")
  );
  const isBuildOutput = (file: string) => {
    const normalized = normalizePathForComparison(file);
    return normalized === outDir || normalized.startsWith(outDir + "/");
  };

  type GraphModule = {
    url: string;
    id?: string | null;
    type?: string;
    file: string | null;
    importedModules: Set<unknown>;
  };

  // The environment module graphs (Vite 6+)
  const environments = (server as unknown as Record<string, unknown>).environments as
    | Record<string, { moduleGraph?: { getModulesByFile?(f: string): Set<GraphModule> | undefined } }>
    | undefined;
  const ssrModuleGraph = environments?.ssr?.moduleGraph;
  // The graph keys real paths; a route discovered under a symlinked path
  // (macOS /tmp → /private/tmp) is looked up both ways.
  const modulesByFile = (graph: typeof ssrModuleGraph, file: string): GraphModule[] => {
    const found = [...(graph?.getModulesByFile?.(file) ?? [])];
    try {
      const real = fs.realpathSync(file);
      if (real !== file) found.push(...(graph?.getModulesByFile?.(real) ?? []));
    } catch {
      // not on disk
    }
    return found;
  };

  function walkSsr(mod: GraphModule) {
    const key = mod.url;
    if (visited.has(key)) return;
    visited.add(key);
    // A watched file, not an import (see above).
    if (mod.type === "asset" || !mod.id) return;
    if (mod.file && isBuildOutput(mod.file)) return;

    if (mod.file && /\.css($|\?)/.test(mod.file)) {
      cssUrls.push(mod.url);
    }

    for (const dep of mod.importedModules) {
      walkSsr(dep as GraphModule);
    }
  }

  if (ssrModuleGraph?.getModulesByFile) {
    for (const file of routeFiles) {
      for (const mod of modulesByFile(ssrModuleGraph, file)) walkSsr(mod);
    }
  }

  // Fallback: every stylesheet the graphs hold (same filters).
  if (cssUrls.length === 0) {
    for (const mods of server.moduleGraph.fileToModulesMap.values()) {
      for (const mod of mods) {
        if (
          mod.file &&
          mod.id &&
          mod.type !== "asset" &&
          /\.css($|\?)/.test(mod.file) &&
          !isBuildOutput(mod.file) &&
          !visited.has(mod.url)
        ) {
          visited.add(mod.url);
          cssUrls.push(mod.url);
        }
      }
    }
  }

  // Last resort: the app's top-level src/*.css (an entry stylesheet).
  if (cssUrls.length === 0) {
    const srcDir = path.resolve(server.config.root, "src");
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

/**
 * The source index.html's head tags (core/document-head.ts), re-read when
 * the file changes. Vite's transformIndexHtml runs over the whole document
 * afterwards, as for the rest of dev's HTML.
 */
const devDocumentHeads = new Map<string, { mtimeMs: number; tags: DocumentHeadTag[] }>();
function readDevDocumentHead(rootDir: string): DocumentHeadTag[] {
  const file = path.join(rootDir, "index.html");
  let mtimeMs: number;
  try {
    mtimeMs = fs.statSync(file).mtimeMs;
  } catch {
    devDocumentHeads.delete(file);
    return [];
  }
  const cached = devDocumentHeads.get(file);
  if (cached && cached.mtimeMs === mtimeMs) return cached.tags;
  const tags = extractDocumentHead(fs.readFileSync(file, "utf-8"));
  devDocumentHeads.set(file, { mtimeMs, tags });
  return tags;
}

async function resolveDevHeadHtml(
  allRoutes: Route[],
  moduleCache: Map<string, RouteModule>,
  args: HeadArgs,
  documentHead: DocumentHeadTag[] = []
): Promise<{ headHtml: string; seo: SeoMetaInput | null }> {
  let mergedSeo: SeoMetaInput | null = null;
  const headFragments: string[] = [];

  for (const route of allRoutes) {
    const mod = moduleCache.get(route.id);
    if (!mod?.head) continue;
    const resolved = await mod.head({ ...args, data: args.loaderData[route.id] });
    if (!resolved) continue;
    if (typeof resolved === "string") {
      // A raw string from head() is developer-authored markup — the explicit
      // escape hatch, emitted faithfully. This used to run a regex
      // "sanitizer" over it in dev while prod emitted verbatim: a bypassable
      // approximation that implied protection the server never had. Prod is
      // canonical here; use the structured SeoMetaInput return for
      // data-driven content (escaped at render).
      headFragments.push(resolved);
      continue;
    }
    mergedSeo = mergeSeoMetaInput(mergedSeo, resolved);
  }

  return {
    headHtml: renderDocumentHead(args.pathname, mergedSeo, headFragments, undefined, documentHead),
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
  devTiming?: DevTimingContext,
  renderNotFound?: (request: Request) => Promise<Response | null>
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

  // Resource route: dispatch the method handler, mirroring the production
  // server (core/resource-routes.ts). The layout + route middleware chain has
  // already run around this handler (see the middleware collection above), so
  // gating matches production; loaders never run.
  if (isResourceRoute(route)) {
    return dispatchResourceHandler(moduleCache.get(route.id)!, request, params, context, {
      // Same reporting as an uncaught page error in dev: terminal + overlay.
      onError: (error) => {
        console.error("Resource route error:", error);
        server.ws.send({
          type: "custom",
          event: "neutron:dev-toolbar:error",
          data: parseError(
            error instanceof Error ? error : new Error(String(error)),
            "unknown",
            rootDir,
            route.id,
            request.url
          ),
        });
      },
    });
  }

  // Handle action for mutations
  if (isMutation) {
    const module = moduleCache.get(route.id)!;
    if (module.action) {
      const actionArgs: ActionArgs = { request, params, context };
      try {
        actionData = await module.action(actionArgs);
        if (isResponse(actionData)) {
          return actionData;
        }
      } catch (error) {
        if (isResponse(error)) {
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

  // A loader's notFound() / 404 ProblemError on a document request renders
  // the app's not-found page, as production does (render-app-route).
  if (renderNotFound && !wantsJson && !isMutation) {
    const answered = loaderResults.find((r) => r.error || isResponse(r.data));
    const signal = answered?.error ?? answered?.data;
    if (answered && isNotFoundSignal(signal)) {
      const page = await renderNotFound(request);
      if (page) return asNotFoundResponse(page, isResponse(signal) ? signal : null);
    }
  }

  // A loader may RETURN a Response (redirect, custom status, JSON) — serve it
  // directly, as production does (render-app-route). Route order decides,
  // so an earlier loader's error still wins over a later one's Response.
  for (const result of loaderResults) {
    if (result.error) break;
    if (isResponse(result.data)) return result.data;
  }

  // Check for loader errors
  const loaderError = loaderResults.find((r) => r.error);
  if (loaderError?.error) {
    if (isResponse(loaderError.error)) return loaderError.error;
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

  // headers() exports, merged as in production (render-app-route).
  const routeHeaders = await resolveRouteHeaders(allRoutes, moduleCache, {
    request,
    params,
    context,
    loaderData,
    actionData,
  });

  // Resolve head() exports (mirrors production server behavior)
  const pathname = new URL(request.url).pathname;
  const { headHtml, seo: mergedSeo } = await resolveDevHeadHtml(allRoutes, moduleCache, {
    request,
    params,
    context,
    loaderData,
    actionData,
    pathname,
  }, readDevDocumentHead(rootDir));

  // For JSON requests (client-side navigation), return data + head + CSS
  if (wantsJson) {
    const allData: Record<string, unknown> = { ...loaderData };
    if (actionData !== undefined) {
      allData.__action__ = actionData;
    }
    allData.__head__ = headHtml;
    allData.__css__ = collectCssFromModuleGraph(server, allRoutes.map((r) => r.file));
    routeHeaders.set("Content-Type", "application/json");
    // Same representation dimensions the production render path declares
    // (render-app-route.ts): the dev data response shares its URL with the
    // HTML document and must never be reusable for a navigation. Merged into
    // a Vary the route's headers() declared, as production does.
    declareRepresentationDimensions(routeHeaders);
    return new Response(encodeSerializedPayloadAsJson(allData), {
      headers: routeHeaders,
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

    for (const layoutRoute of layoutChain) {
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

    if (!routeHeaders.has("Content-Type")) {
      routeHeaders.set("Content-Type", "text/html; charset=utf-8");
    }
    // Mirrors the data branch above: the document shares its URL with the
    // JSON data protocol and must not answer a data request from a cache.
    declareRepresentationDimensions(routeHeaders);
    return new Response(fullHtml, {
      headers: routeHeaders,
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

interface DevMount {
  globalMiddleware: boolean;
  /** The sub-app under its prefix; a path it does not match answers the pass-through marker. */
  dispatcher: Hono;
}

/**
 * The dev server's view of src/server.entry.ts. The module is loaded through
 * Vite on every request (cheap: cached until an edit invalidates it), and the
 * entry is rebuilt only when Vite hands back a new module, closing the old
 * entry and its WebSocket clients first.
 */
function createDevServerEntryState(
  rootDir: string,
  loadModule: (id: string) => Promise<unknown>
) {
  interface EntryState {
    mod: unknown;
    entry: NeutronServerEntry;
    mounts: Array<{ prefix: string; mount: DevMount }>;
    wss?: WebSocketServer;
  }
  let current: EntryState | null = null;
  let loading: Promise<EntryState | null> | null = null;
  // Set on shutdown: a reload scheduled by the watcher must not rebuild it.
  let closed = false;

  const retire = async (previous: EntryState | null): Promise<void> => {
    if (!previous) return;
    if (previous.wss) {
      const wss = previous.wss;
      for (const client of wss.clients) client.terminate();
      await new Promise<void>((resolve) => wss.close(() => resolve()));
    }
    await previous.entry.close?.();
  };

  // As in production, `attach` runs when the entry is loaded, before any
  // upgrade reaches `authorize`.
  const attach = async (entry: NeutronServerEntry): Promise<WebSocketServer | undefined> => {
    if (!entry.websocket) return undefined;
    const { WebSocketServer: WSServer } = await import("ws");
    const wss = new WSServer({ noServer: true });
    try {
      await entry.websocket.attach(wss);
    } catch (error) {
      wss.close();
      await entry.close?.();
      throw error;
    }
    return wss;
  };

  const load = async (): Promise<EntryState | null> => {
    if (closed) return null;
    const file = findServerEntryFile(rootDir);
    if (!file) {
      const previous = current;
      current = null;
      await retire(previous);
      return null;
    }
    const mod = await loadModule(file);
    if (current && current.mod === mod) return current;
    const entry = await resolveServerEntryModule(mod, file);
    const mounts = Object.entries(entry.mount ?? {}).map(([prefix, value]) => {
      const { app, globalMiddleware = true } =
        typeof (value as { fetch?: unknown }).fetch === "function"
          ? { app: value as Hono, globalMiddleware: true }
          : (value as { app: Hono; globalMiddleware?: boolean });
      const dispatcher = new Hono();
      dispatcher.route(prefix, app);
      dispatcher.notFound(
        () => new Response(null, { status: 404, headers: { [DEV_PASS_THROUGH_HEADER]: devPassThroughToken } })
      );
      return { prefix: prefix.replace(/\/+$/, "") || "/", mount: { globalMiddleware, dispatcher } };
    });
    // The previous entry goes first (its clients, its server, its close()),
    // so the two never hold resources at the same time.
    const previous = current;
    current = null;
    await retire(previous);
    const wss = await attach(entry);
    current = { mod, entry, mounts, wss };
    if (closed) {
      const late = current;
      current = null;
      await retire(late);
      return null;
    }
    return current;
  };

  const resolve = async (): Promise<EntryState | null> => {
    // One load at a time: concurrent first requests share it.
    loading ??= load().finally(() => {
      loading = null;
    });
    return loading;
  };

  return {
    /** Load (or reload after an edit) the entry now; a no-op while it is unchanged. */
    async load(): Promise<void> {
      await resolve();
    },

    async match(pathname: string): Promise<DevMount | null> {
      const state = await resolve();
      if (!state) return null;
      for (const { prefix, mount } of state.mounts) {
        if (pathname === prefix || pathname.startsWith(prefix === "/" ? "/" : prefix + "/")) {
          return mount;
        }
      }
      return null;
    },

    /** Upgrades for the entry's `websocket`; Vite's own HMR socket is left to Vite. */
    upgrade(req: import("node:http").IncomingMessage, socket: import("node:net").Socket, head: Buffer): void {
      const protocol = req.headers["sec-websocket-protocol"];
      if (protocol === "vite-hmr" || protocol === "vite-ping") return;
      void (async () => {
        let state: EntryState | null;
        try {
          state = await resolve();
        } catch {
          socket.destroy();
          return;
        }
        const options = state?.entry.websocket;
        if (!state || !options) return;
        if (options.path) {
          let pathname: string;
          try {
            pathname = new URL(req.url ?? "/", "http://localhost").pathname;
          } catch {
            return;
          }
          if (pathname !== options.path) return;
        }
        if (options.authorize) {
          // Fail closed with a deadline, as in production (TS-28).
          let timer: ReturnType<typeof setTimeout> | undefined;
          const allowed = await Promise.race([
            Promise.resolve()
              .then(() => options.authorize!(req))
              .catch(() => false),
            new Promise<false>((done) => {
              timer = setTimeout(() => done(false), 5_000);
            }),
          ]);
          clearTimeout(timer);
          if (allowed !== true || socket.destroyed) {
            socket.destroy();
            return;
          }
        }
        const wss = state.wss;
        if (!wss || socket.destroyed) {
          socket.destroy();
          return;
        }
        wss.handleUpgrade(req, socket, head, (ws) => {
          wss.emit("connection", ws, req);
        });
      })();
    },

    async close(): Promise<void> {
      closed = true;
      // An in-flight load finishes first (it retires what it built).
      await loading?.catch(() => undefined);
      const previous = current;
      current = null;
      await retire(previous);
    },
  };
}

/**
 * Copy a Fetch response's headers onto a Node response. Headers iterates each
 * Set-Cookie as its own entry and setHeader replaces, so copying entry by
 * entry would keep only the last cookie; they are sent as one array instead.
 */
function copyWebResponseHeaders(res: import("http").ServerResponse, headers: Headers): void {
  headers.forEach((value, key) => {
    if (key.toLowerCase() !== "set-cookie") res.setHeader(key, value);
  });
  const cookies = headers.getSetCookie();
  if (cookies.length > 0) res.setHeader("set-cookie", cookies);
}

async function sendWebResponse(
  req: import("http").IncomingMessage,
  res: import("http").ServerResponse,
  response: Response
): Promise<void> {
  res.statusCode = response.status;
  copyWebResponseHeaders(res, response.headers);
  if (response.body && req.method !== "HEAD") {
    const reader = response.body.getReader();
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      res.write(Buffer.from(value));
    }
  }
  res.end();
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

const staticPathsByModule = new WeakMap<RouteModule, Promise<Array<Record<string, unknown>>>>();

/**
 * True when `route` is a static route whose `getStaticPaths` does not list
 * `params`: a build writes no file for it, so `start` answers 404.
 */
async function isUngeneratedStaticPath(
  server: ViteDevServer,
  route: Route,
  params: Record<string, string>
): Promise<boolean> {
  if (route.config.mode !== "static" || route.params.length === 0 || isResourceRoute(route)) return false;
  const module = (await server.ssrLoadModule(route.file)) as RouteModule;
  if (!module.getStaticPaths) return false;
  let listed = staticPathsByModule.get(module);
  if (!listed) {
    listed = Promise.resolve(module.getStaticPaths()).then((result) =>
      (Array.isArray(result) ? result : result.paths).map((entry) => entry.params as Record<string, unknown>)
    );
    staticPathsByModule.set(module, listed);
  }
  const asText = (value: unknown) => (Array.isArray(value) ? value.join("/") : String(value ?? ""));
  return !(await listed).some((entry) =>
    route.params.every((name) => {
      const requested = params[name] ?? "";
      const value = asText(entry[name]);
      return value === requested || value === safeDecode(requested);
    })
  );
}

function safeDecode(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
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

/**
 * Routes whose client module is never loaded: page routes whose whole chain
 * renders at the static client tier (`resolveClientTier` is not `"full"`),
 * plus layouts only such routes use. Their documents ship no router — a
 * browser navigation reaches them, and the router never renders one in place —
 * so the client route table carries their facts (path, mode) without an
 * `import()`, and their modules, with everything they import, stay out of the
 * client build. The build takes their CSS from its CSS pass instead.
 */
export function staticTierRouteIds(routes: Route[]): Set<string> {
  const pages = routes.filter((route) => !isResourceRoute(route));
  const byId = new Map(pages.map((route) => [route.id, route]));
  const chainOf = (route: Route): Route[] => {
    const chain: Route[] = [route];
    for (let id = route.parentId; id; ) {
      const parent = byId.get(id);
      if (!parent) break;
      chain.unshift(parent);
      id = parent.parentId;
    }
    return chain;
  };
  const isLayout = (route: Route) => route.isLayout === true || route.file.includes("_layout");
  const stubbed = new Set<string>();
  const needed = new Set<string>();
  for (const route of pages) {
    if (isLayout(route)) continue;
    const chain = chainOf(route);
    if (!route.isNotFound && resolveClientTier(chain) !== "full") {
      stubbed.add(route.id);
    } else {
      for (const member of chain) needed.add(member.id);
    }
  }
  for (const route of pages) {
    if (isLayout(route) && !needed.has(route.id)) stubbed.add(route.id);
  }
  return stubbed;
}

export function generateRoutesModule(routes: Route[], client?: NeutronClientConfig): string {
  const cwd = process.cwd().replace(/\\/g, "/");
  // Resource routes are endpoints, not pages: excluded from the client route
  // table entirely so no client chunk is ever emitted for them. Their method
  // handlers are additionally stripped from any module that does reach the
  // browser (stripServerOnlyRouteModule) — belt and braces, because handler
  // code must never ship to the client.
  const clientRoutes = routes.filter((route) => !isResourceRoute(route));
  const staticTier = staticTierRouteIds(routes);
  const routeMap: string[] = clientRoutes.map((route) => {
    const absPath = route.file.replace(/\\/g, "/");
    // Make path relative to project root so Vite can resolve it.
    // Absolute paths would produce "//Users/..." which browsers treat as protocol-relative URLs.
    const relativePath = absPath.startsWith(cwd + "/")
      ? absPath.slice(cwd.length)
      : "/@fs" + absPath;
    const moduleField = staticTier.has(route.id)
      ? "clientModule: false"
      : `load: () => import(${JSON.stringify(relativePath + "?" + CLIENT_ROUTE_QUERY)})`;
    return `  ${JSON.stringify(route.id)}: {
    id: ${JSON.stringify(route.id)},
    path: ${JSON.stringify(route.path)},
    parentId: ${route.parentId ? JSON.stringify(route.parentId) : "null"},
    isLayout: ${!!route.isLayout},
    isNotFound: ${!!route.isNotFound},
    mode: ${JSON.stringify(route.config.mode)},
    hasLoader: ${route.hasLoader !== false},
    ${moduleField}
  }`;
  });

  // neutron.config.ts `client`, applied before the app's init() starts the
  // router (the entry imports this module first). Absent: nothing emitted.
  const routerOptions =
    client?.unloadRouteStylesheets === true
      ? `import { configureRouter } from "@neutron-build/core/client";
configureRouter(${JSON.stringify({ unloadRouteStylesheets: true })});

`
      : "";

  return `${routerOptions}export const routes = {
${routeMap.join(",\n")}
};

export const routeIds = ${JSON.stringify(clientRoutes.map((r) => r.id))};
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
): {
  result: { code: string; map: ReturnType<MagicString["generateMap"]> } | null;
  ids: string[];
  unresolved: string[];
} {
  ISLAND_TAG_RE.lastIndex = 0;
  const s = new MagicString(code);
  const ids: string[] = [];
  const unresolved: string[] = [];
  let changed = false;
  let match: RegExpExecArray | null;

  while ((match = ISLAND_TAG_RE.exec(code)) !== null) {
    const componentName = match[2];
    // Skip if this tag already has an __src (idempotent re-transform).
    const tagStart = match.index;
    const tagSlice = code.slice(tagStart, tagStart + match[0].length + 1);
    if (/\b__src=/.test(tagSlice)) continue;

    const id = resolveImportSourceId(code, componentName, cleanId, rootDir);
    if (!id) {
      // Package imports are legitimately path-less; anything else is a local
      // component whose marker would ship without data-src (inert on static
      // pages).
      const specifier = findImportSpecifier(code, componentName);
      if (!specifier || specifier.startsWith(".") || specifier.startsWith("/")) {
        unresolved.push(componentName);
      }
      continue;
    }

    // Insert the prop immediately after `<Island` so it survives whatever
    // attribute layout follows.
    const insertAt = tagStart + "<Island".length;
    s.appendLeft(insertAt, ` __src=${JSON.stringify(id)}`);
    ids.push(id);
    changed = true;
  }

  return {
    result: changed ? { code: s.toString(), map: s.generateMap({ hires: true }) } : null,
    ids,
    unresolved,
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
  const specifier = findImportSpecifier(code, componentName);
  if (!specifier) return null;

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

const IMPORT_DECL_RE = /\bimport\s+([\w$*{][^;'"]*?)\s+from\s*["']([^"']+)["']/g;

/**
 * Find the module specifier a local binding is imported from. Handles every
 * value-import clause shape: `import X`, `import { X }`, `import { Y as X }`,
 * `import D, { X }`, `import X, { type T }`, `import X, * as ns`, multi-line
 * clauses. Type-only imports (`import type ...`, `{ type X }`) never bind a
 * runtime value, so they are skipped.
 */
export function findImportSpecifier(code: string, localName: string): string | null {
  IMPORT_DECL_RE.lastIndex = 0;
  let m: RegExpExecArray | null;
  while ((m = IMPORT_DECL_RE.exec(code)) !== null) {
    const clause = m[1].trim();
    if (/^type\s/.test(clause)) continue;
    if (importClauseBinds(clause, localName)) return m[2];
  }
  return null;
}

function importClauseBinds(clause: string, localName: string): boolean {
  const braceStart = clause.indexOf("{");
  const head = (braceStart === -1 ? clause : clause.slice(0, braceStart)).trim();
  // Default binding and/or namespace: `X`, `X,`, `X, * as ns`, `* as ns`.
  for (const part of head.split(",")) {
    const binding = part.trim().replace(/^\*\s+as\s+/, "");
    if (binding === localName) return true;
  }
  if (braceStart === -1) return false;
  const braceEnd = clause.indexOf("}", braceStart);
  const named = clause.slice(braceStart + 1, braceEnd === -1 ? undefined : braceEnd);
  for (const raw of named.split(",")) {
    const spec = raw.trim();
    if (!spec || /^type\s/.test(spec)) continue;
    const alias = spec.match(/\bas\s+([\w$]+)$/);
    if ((alias ? alias[1] : spec) === localName) return true;
  }
  return false;
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

function isScriptModuleId(id: string): boolean {
  const ext = path.extname(id).toLowerCase();
  return ext === ".js" || ext === ".mjs" || ext === ".cjs" || ext === ".ts" || ext === ".tsx" || ext === ".jsx";
}
