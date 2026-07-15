import { createRouter, compileRouteRules, resolveRouteRuleRedirect, resolveRouteRuleRewrite, resolveRouteRuleHeaders, renderAppRoute, isMutationMethod, createMemoryLoaderCacheStore } from "@neutron-build/core/runtime-edge";
import * as routeModule0 from "../../src/routes/_layout.tsx";
import * as routeModule1 from "../../src/routes/admin.tsx";
import * as routeModule2 from "../../src/routes/big.tsx";
import * as routeModule3 from "../../src/routes/compute.tsx";
import * as routeModule4 from "../../src/routes/dashboard.tsx";
import * as routeModule5 from "../../src/routes/protected.tsx";
import * as routeModule6 from "../../src/routes/todos.tsx";
import * as routeModule7 from "../../src/routes/users/index.tsx";
import * as routeModule8 from "../../src/routes/api/cache.tsx";
import * as routeModule9 from "../../src/routes/api/mutate.tsx";
import * as routeModule10 from "../../src/routes/api/revalidate.tsx";
import * as routeModule11 from "../../src/routes/api/stream.tsx";
import * as routeModule12 from "../../src/routes/users/[id].tsx";
import * as routeModule13 from "../../src/routes/api/session/refresh.tsx";

const CLIENT_ENTRY_SCRIPT_SRC = "/assets/index-DKYn4kdV.js";
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
    cache: null,
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
  "route:protected.tsx": routeModule5,
  "route:todos.tsx": routeModule6,
  "route:users/index.tsx": routeModule7,
  "route:api/cache.tsx": routeModule8,
  "route:api/mutate.tsx": routeModule9,
  "route:api/revalidate.tsx": routeModule10,
  "route:api/stream.tsx": routeModule11,
  "route:users/[id].tsx": routeModule12,
  "route:api/session/refresh.tsx": routeModule13,
};

const APP_ROUTE_IDS = new Set(["route:admin.tsx","route:big.tsx","route:compute.tsx","route:dashboard.tsx","route:protected.tsx","route:todos.tsx","route:users/index.tsx","route:api/cache.tsx","route:api/mutate.tsx","route:api/revalidate.tsx","route:api/stream.tsx","route:users/[id].tsx","route:api/session/refresh.tsx"]);
const ROUTE_DEF_BY_ID = new Map(ROUTE_DEFS.map((route) => [route.id, route]));
const ROUTES_BY_ID = new Map(ROUTE_DEFS.map((route) => [route.id, toRuntimeRoute(route)]));
const LOADER_DATA_CACHE = createMemoryLoaderCacheStore();
const GLOBAL_MIDDLEWARE = [];

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
