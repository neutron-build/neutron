import { h } from "preact";
import { createRouter, runMiddlewareChain, renderToString, encodeSerializedPayloadAsJson, serializeForInlineScript, mergeSeoMetaInput, renderDocumentHead, compileRouteRules, resolveRouteRuleRedirect, resolveRouteRuleRewrite, resolveRouteRuleHeaders } from "neutron/runtime-edge";
import * as routeModule0 from "../../src/routes/_layout.tsx";
import * as routeModule1 from "../../src/routes/admin.tsx";
import * as routeModule2 from "../../src/routes/big.tsx";
import * as routeModule3 from "../../src/routes/compute.tsx";
import * as routeModule4 from "../../src/routes/dashboard.tsx";
import * as routeModule5 from "../../src/routes/islands.tsx";
import * as routeModule6 from "../../src/routes/protected.tsx";
import * as routeModule7 from "../../src/routes/todos.tsx";
import * as routeModule8 from "../../src/routes/users/index.tsx";
import * as routeModule9 from "../../src/routes/api/cache.tsx";
import * as routeModule10 from "../../src/routes/api/mutate.tsx";
import * as routeModule11 from "../../src/routes/api/revalidate.tsx";
import * as routeModule12 from "../../src/routes/api/stream.tsx";
import * as routeModule13 from "../../src/routes/users/[id].tsx";
import * as routeModule14 from "../../src/routes/api/session/refresh.tsx";

const CLIENT_ENTRY_SCRIPT_SRC = "/assets/index-DUId0ORO.js";
const ROUTE_RULES = compileRouteRules({});

const ROUTE_DEFS = [
  {
    id: "route:_layout.tsx",
    path: "/",
    parentId: null,
    params: [],
    mode: "static",
    cache: null,
    isLayout: true,
  },
  {
    id: "route:admin.tsx",
    path: "/admin",
    parentId: "route:_layout.tsx",
    params: [],
    mode: "app",
    cache: null,
    isLayout: false,
  },
  {
    id: "route:big.tsx",
    path: "/big",
    parentId: "route:_layout.tsx",
    params: [],
    mode: "app",
    cache: null,
    isLayout: false,
  },
  {
    id: "route:compute.tsx",
    path: "/compute",
    parentId: "route:_layout.tsx",
    params: [],
    mode: "app",
    cache: null,
    isLayout: false,
  },
  {
    id: "route:dashboard.tsx",
    path: "/dashboard",
    parentId: "route:_layout.tsx",
    params: [],
    mode: "app",
    cache: null,
    isLayout: false,
  },
  {
    id: "route:islands.tsx",
    path: "/islands",
    parentId: "route:_layout.tsx",
    params: [],
    mode: "app",
    cache: null,
    isLayout: false,
  },
  {
    id: "route:protected.tsx",
    path: "/protected",
    parentId: "route:_layout.tsx",
    params: [],
    mode: "app",
    cache: null,
    isLayout: false,
  },
  {
    id: "route:todos.tsx",
    path: "/todos",
    parentId: "route:_layout.tsx",
    params: [],
    mode: "app",
    cache: null,
    isLayout: false,
  },
  {
    id: "route:users/index.tsx",
    path: "/users",
    parentId: "route:_layout.tsx",
    params: [],
    mode: "app",
    cache: {"maxAge":30},
    isLayout: false,
  },
  {
    id: "route:api/cache.tsx",
    path: "/api/cache",
    parentId: "route:_layout.tsx",
    params: [],
    mode: "app",
    cache: null,
    isLayout: false,
  },
  {
    id: "route:api/mutate.tsx",
    path: "/api/mutate",
    parentId: "route:_layout.tsx",
    params: [],
    mode: "app",
    cache: null,
    isLayout: false,
  },
  {
    id: "route:api/revalidate.tsx",
    path: "/api/revalidate",
    parentId: "route:_layout.tsx",
    params: [],
    mode: "app",
    cache: null,
    isLayout: false,
  },
  {
    id: "route:api/stream.tsx",
    path: "/api/stream",
    parentId: "route:_layout.tsx",
    params: [],
    mode: "app",
    cache: null,
    isLayout: false,
  },
  {
    id: "route:users/[id].tsx",
    path: "/users/:id",
    parentId: "route:_layout.tsx",
    params: ["id"],
    mode: "app",
    cache: {"maxAge":30},
    isLayout: false,
  },
  {
    id: "route:api/session/refresh.tsx",
    path: "/api/session/refresh",
    parentId: "route:_layout.tsx",
    params: [],
    mode: "app",
    cache: null,
    isLayout: false,
  },
];

const ROUTE_MODULES = {
  "route:_layout.tsx": routeModule0,
  "route:admin.tsx": routeModule1,
  "route:big.tsx": routeModule2,
  "route:compute.tsx": routeModule3,
  "route:dashboard.tsx": routeModule4,
  "route:islands.tsx": routeModule5,
  "route:protected.tsx": routeModule6,
  "route:todos.tsx": routeModule7,
  "route:users/index.tsx": routeModule8,
  "route:api/cache.tsx": routeModule9,
  "route:api/mutate.tsx": routeModule10,
  "route:api/revalidate.tsx": routeModule11,
  "route:api/stream.tsx": routeModule12,
  "route:users/[id].tsx": routeModule13,
  "route:api/session/refresh.tsx": routeModule14,
};

const APP_ROUTE_IDS = new Set(["route:admin.tsx","route:big.tsx","route:compute.tsx","route:dashboard.tsx","route:islands.tsx","route:protected.tsx","route:todos.tsx","route:users/index.tsx","route:api/cache.tsx","route:api/mutate.tsx","route:api/revalidate.tsx","route:api/stream.tsx","route:users/[id].tsx","route:api/session/refresh.tsx"]);
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

export async function handleNeutronRequest(request) {
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
  return `${url.pathname}::${url.search}::${routeId}::${encodedParams}`;
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
  const prefix = `${normalized}::`;
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
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Error - Neutron</title>
</head>
<body>
  <h1>Application Error</h1>
  <pre>${escapeHtml(error.message || "Unknown error")}</pre>
</body>
</html>`;
}

function wrapHtml(content, pathname, loaderData, actionData, headHtml = "") {
  const allData = { ...loaderData };
  if (actionData !== undefined) {
    allData.__action__ = actionData;
  }
  const dataScript = Object.keys(allData).length > 0
    ? `<script>window.__NEUTRON_DATA_SERIALIZED__=${serializeForInlineScript(allData)};</script>`
    : "";
  const clientScript = CLIENT_ENTRY_SCRIPT_SRC
    ? `<script type="module" src="${escapeHtml(CLIENT_ENTRY_SCRIPT_SRC)}"></script>`
    : "";

  return `<!DOCTYPE html>
<html lang="en">
<head>
${headHtml || renderDocumentHead(pathname, null)}
</head>
<body>
<div id="app">${content}</div>
${dataScript}
${clientScript}
</body>
</html>`;
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
