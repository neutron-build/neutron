import * as path from "node:path";
import * as fs from "node:fs";
import type {
  Route,
  RouteModule,
  AppContext,
  LoaderArgs,
} from "./types.js";
import { discoverRoutes } from "./manifest.js";
import { assertRenderedFragment } from "./fragment-guard.js";
import { renderDocumentHead } from "./seo.js";
import { resolveHeadHtml } from "./head.js";
import { resolvePreactSsr, importPreactSsr } from "./preact-ssr.js";

export interface StaticRenderOptions {
  routesDir: string;
  outputDir: string;
  baseUrl?: string;
  /**
   * App root used to resolve preact / preact-render-to-string. Defaults to the
   * parent of `routesDir` (…/src → app root). Override when routes live outside
   * the usual src/routes layout.
   */
  appRoot?: string;
}

export async function renderStatic(options: StaticRenderOptions): Promise<void> {
  const { routesDir, outputDir, baseUrl = "" } = options;
  // Prefer explicit appRoot; otherwise process.cwd() (CLI runs from the app).
  // Falling back to parent-of-routes only when cwd has no package.json.
  const appRoot =
    options.appRoot ??
    (fs.existsSync(path.join(process.cwd(), "package.json"))
      ? process.cwd()
      : path.resolve(routesDir, ".."));

  // Resolve + import preact and the renderer from one physical graph so hooks
  // on route modules share options.__r with renderToString. A top-level static
  // import of either package would bind the caller's node_modules copy and
  // dual-instance crash under pnpm / monorepos.
  const preactSsr = resolvePreactSsr(appRoot);
  const { h, renderToString } = await importPreactSsr(preactSsr);

  const allRoutes = discoverRoutes({ routesDir });

  const layouts = new Map<string, Route>();
  const pageRoutes: Route[] = [];

  for (const route of allRoutes) {
    if (route.file.includes("_layout")) {
      layouts.set(route.id, route);
    } else {
      pageRoutes.push(route);
    }
  }

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

  const moduleCache = new Map<string, RouteModule>();

  for (const pageRoute of pageRoutes) {
    if (pageRoute.config.mode !== "static") {
      console.log(`  Skipping ${pageRoute.path} (app route)`);
      continue;
    }

    try {
      const module = await loadRouteModule(pageRoute.file, moduleCache);

      if (!module?.default) {
        console.log(`  Skipping ${pageRoute.path} (no component)`);
        continue;
      }

      const context: AppContext = {};
      const requestOrigin = baseUrl || "http://localhost";
      const request = new Request(requestOrigin + pageRoute.path);
      let loaderData: unknown = undefined;
      if (module.loader) {
        loaderData = await module.loader({
          request,
          params: {},
          context,
        } as LoaderArgs);
      }

      const layoutChain = getLayoutChain(pageRoute);

      // Pre-load all layout modules so head resolution can read them straight
      // from moduleCache without re-importing.
      for (const layoutRoute of layoutChain) {
        await loadRouteModule(layoutRoute.file, moduleCache);
      }

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      let element: any = h(module.default as any, {
        data: loaderData,
        params: {},
      });

      for (const layoutRoute of [...layoutChain].reverse()) {
        const layoutModule = moduleCache.get(path.resolve(layoutRoute.file))!;
        if (layoutModule?.default) {
          element = h(layoutModule.default as any, {}, element);
        }
      }

      const html = renderToString(element);
      // The rendered output is mounted inside the shell's `<div id="app">`
      // (wrapHtml owns `<html>`/`<head>`/`<body>`). A full-document render would
      // nest a second document inside #app — reject it before it is written.
      assertRenderedFragment(html, layoutChain[0]?.file ?? pageRoute.file);
      // Outermost layout first, page route last — the same chain order the
      // app-route renderer uses, so shared head resolution merges identically.
      const orderedRoutes = [...layoutChain].reverse();
      orderedRoutes.push(pageRoute);
      const headHtml = await resolveHeadHtml(
        orderedRoutes.map((route) => ({
          route,
          module: moduleCache.get(path.resolve(route.file)),
        })),
        {
          request,
          params: {},
          context,
          pathname: pageRoute.path,
          loaderData: loaderData !== undefined ? { [pageRoute.id]: loaderData } : {},
          // No nonce at build time — SSG runs no CSP-nonce middleware.
        }
      );
      const fullHtml = wrapHtml(html, pageRoute.path, headHtml);

      const outPath = getOutputPath(outputDir, pageRoute.path);
      fs.mkdirSync(path.dirname(outPath), { recursive: true });
      fs.writeFileSync(outPath, fullHtml);

      console.log(`  ${pageRoute.path} → ${path.relative(outputDir, outPath)}`);
    } catch (error) {
      console.error(`  Error rendering ${pageRoute.path}:`, error);
    }
  }
}

async function loadRouteModule(file: string, cache?: Map<string, RouteModule>): Promise<RouteModule> {
  const absolutePath = path.resolve(file);

  if (cache?.has(absolutePath)) {
    return cache.get(absolutePath)!;
  }

  // Convert to file:// URL for Windows compatibility
  const fileUrl = process.platform === 'win32'
    ? `file:///${absolutePath.replace(/\\/g, '/')}`
    : `file://${absolutePath}`;

  // Clear any cached version
  const timestamp = Date.now();
  const module = await import(/* @vite-ignore */ `${fileUrl}?t=${timestamp}`) as RouteModule;

  cache?.set(absolutePath, module);
  return module;
}

function wrapHtml(
  content: string,
  routePath: string,
  headHtml: string = renderDocumentHead(routePath, null)
): string {
  return `<!DOCTYPE html>
<html lang="en">
<head>
${headHtml}
</head>
<body>
<div id="app">${content}</div>
</body>
</html>`;
}

function getOutputPath(outputDir: string, routePath: string): string {
  if (routePath === "/") {
    return path.join(outputDir, "index.html");
  }

  const cleanPath = routePath.replace(/\/$/, "");
  return path.join(outputDir, cleanPath, "index.html");
}

// Re-export for callers that only need the string renderer from this module.
// Prefer resolvePreactSsr + importPreactSsr when sharing a graph with app code.
export { renderToString } from "preact-render-to-string";
