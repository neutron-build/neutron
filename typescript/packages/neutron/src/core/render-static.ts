import * as path from "node:path";
import * as fs from "node:fs";
import { renderToString } from "preact-render-to-string";
import { h } from "preact";
import type {
  Route,
  RouteModule,
  AppContext,
  LoaderArgs,
  HeadArgs,
} from "./types.js";
import { discoverRoutes } from "./manifest.js";
import { serializeForInlineScript } from "./serialization.js";
import {
  mergeSeoMetaInput,
  renderDocumentHead,
  type SeoMetaInput,
} from "./seo.js";

export interface StaticRenderOptions {
  routesDir: string;
  outputDir: string;
  baseUrl?: string;
}

export async function renderStatic(options: StaticRenderOptions): Promise<void> {
  const { routesDir, outputDir, baseUrl = "" } = options;

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

      // Pre-load all layout modules to avoid redundant loads in resolveRouteHeadHtml
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
      const headHtml = await resolveRouteHeadHtml(
        pageRoute,
        layoutChain,
        request,
        context,
        {},
        loaderData,
        pageRoute.path,
        moduleCache
      );
      const fullHtml = wrapHtml(html, pageRoute.path, loaderData, headHtml);

      const outPath = getOutputPath(outputDir, pageRoute.path);
      fs.mkdirSync(path.dirname(outPath), { recursive: true });
      fs.writeFileSync(outPath, fullHtml);

      console.log(`  ${pageRoute.path} → ${path.relative(outputDir, outPath)}`);
    } catch (error) {
      console.error(`  Error rendering ${pageRoute.path}:`, error);
    }
  }
}

async function resolveRouteHeadHtml(
  route: Route,
  layoutChain: Route[],
  request: Request,
  context: AppContext,
  params: Record<string, string>,
  loaderData: unknown,
  pathname: string,
  moduleCache?: Map<string, RouteModule>
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
    const currentModule = await loadRouteModule(currentRoute.file, moduleCache);
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
    const resolved = await currentModule.head(args);
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
  loaderData?: unknown,
  headHtml: string = renderDocumentHead(routePath, null)
): string {
  const dataScript = loaderData !== undefined
    ? `<script>window.__NEUTRON_DATA_SERIALIZED__=${serializeForInlineScript({ page: loaderData })};</script>`
    : "";

  return `<!DOCTYPE html>
<html lang="en">
<head>
${headHtml}
</head>
<body>
<div id="app">${content}</div>
${dataScript}
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

export { renderToString };
