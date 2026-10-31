/**
 * Resource routes — route modules that are HTTP endpoints, not pages.
 *
 * A route module exporting HTTP method handlers (`GET`, `POST`, `PUT`,
 * `PATCH`, `DELETE`, `OPTIONS`, `HEAD`) and NO default component is served
 * as an endpoint: the request is dispatched to the handler for its method,
 * through the standard middleware chain (global + layout + route middleware,
 * FRAMEWORK_CONTRACT.md §5 order), and loaders never run. The convention is
 * how an SSR app carries its REST/SSE surface in the same server and route
 * table as its pages.
 *
 * Build-time rules (see discoverRoutes / render-static / neutron-cli build):
 * - a module with BOTH a default component and method handlers is a build
 *   error — the page pipeline would silently never dispatch the handlers;
 * - resource routes are NEVER prerendered — there is no component to render,
 *   and an endpoint's middleware must run per request (A-020: prerendering
 *   and gating contradict; resource routes simply never prerender, so a
 *   gated resource route is legal and answers per request);
 * - method handlers are stripped from the client route module
 *   (vite/server-only.ts) and resource routes are excluded from the client
 *   route table entirely — handler code never ships to the browser.
 */
import type {
  AppContext,
  ResourceRouteHandler,
  ResourceRouteHandlerArgs,
  Route,
} from "./types.js";
import { isResponse } from "./response.js";
import { isProblemError, internalError } from "./problem.js";

/** Method-handler exports that make a route module a resource route. */
export const RESOURCE_ROUTE_METHODS = [
  "GET",
  "POST",
  "PUT",
  "PATCH",
  "DELETE",
  "OPTIONS",
  "HEAD",
] as const;

export type ResourceRouteMethod = (typeof RESOURCE_ROUTE_METHODS)[number];

/**
 * Whether a discovered route is a resource route: method handlers exported,
 * no default component, not a layout or not-found page. Facts are derived at
 * discovery; `hasMethodHandlers === true` (not `!== false`) is deliberate —
 * unknown means "not a resource route", the safe direction (an endpoint
 * mis-served as a page-shaped 404 is today's behavior; a page mis-served as
 * an endpoint would lose its HTML).
 */
export function isResourceRoute(route: Route): boolean {
  if (route.isLayout || route.isNotFound) return false;
  return route.hasMethodHandlers === true && route.hasDefaultExport === false;
}

/**
 * The methods a resource route module answers, for the `Allow` header.
 * `HEAD` counts when `GET` exists (RFC 9110 §9.3.2: a server MUST accept
 * HEAD for any resource that supports GET).
 */
export function resourceRouteAllowedMethods(
  module: Partial<Record<ResourceRouteMethod, unknown>>
): ResourceRouteMethod[] {
  const allowed = new Set<ResourceRouteMethod>();
  for (const method of RESOURCE_ROUTE_METHODS) {
    if (module[method] !== undefined) {
      allowed.add(method);
      if (method === "GET") allowed.add("HEAD");
    }
  }
  return RESOURCE_ROUTE_METHODS.filter((method) => allowed.has(method));
}

function methodNotAllowed(
  module: Partial<Record<ResourceRouteMethod, unknown>>,
  pathname: string
): Response {
  // 405 problem+json with Allow — the A-024/TS-27 shape: method mismatches
  // are RFC 7807 problems that say what IS supported, not empty responses.
  const allowed = resourceRouteAllowedMethods(module);
  const body = {
    type: "https://neutron.dev/errors/method-not-allowed",
    title: "Method Not Allowed",
    status: 405,
    detail:
      allowed.length > 0
        ? `This resource route has no handler for the request method. Allowed: ${allowed.join(", ")}.`
        : "This resource route has no handler for the request method.",
    instance: pathname,
  };
  const headers: Record<string, string> = { "Content-Type": "application/problem+json" };
  if (allowed.length > 0) {
    headers.Allow = allowed.join(", ");
  }
  return new Response(JSON.stringify(body), { status: 405, headers });
}

/**
 * Dispatch one request to a resource route module's method handler. Shared by
 * the production server (server/index.ts) and the dev server (vite/plugin.ts)
 * so both answer identically. `context` is the middleware chain's context —
 * the same object route/global middleware mutated.
 *
 * - the handler for the request method runs with `{ request, params, context }`;
 * - `HEAD` falls back to `GET` (same headers, no body);
 * - an unsupported method answers 405 problem+json with `Allow`;
 * - a returned/thrown `Response` is answered as-is; a thrown `ProblemError`
 *   becomes its RFC 7807 response; any other thrown error goes to
 *   `options.onError` (the server's error hook, dev's terminal/overlay) and
 *   is answered with a generic 500 problem that carries none of its text
 *   (FRAMEWORK_CONTRACT.md §2 — this surface has no HTML error boundary to
 *   defer to);
 * - a non-Response return value is answered as JSON (200), the ergonomic
 *   default for REST endpoints.
 */
export async function dispatchResourceHandler(
  module: Partial<Record<ResourceRouteMethod, ResourceRouteHandler>>,
  request: Request,
  params: Record<string, string>,
  context: AppContext,
  options: { onError?: (error: unknown) => void } = {}
): Promise<Response> {
  const method = request.method.toUpperCase();
  const known = RESOURCE_ROUTE_METHODS.includes(method as ResourceRouteMethod)
    ? (method as ResourceRouteMethod)
    : undefined;
  // RFC 9110 §9.3.2: HEAD is implicitly supported wherever GET is.
  const fallsBackToGet =
    method === "HEAD" && module.HEAD === undefined && module.GET !== undefined;
  let handlerName: ResourceRouteMethod | undefined;
  if (fallsBackToGet) {
    handlerName = "GET";
  } else if (known !== undefined && module[known] !== undefined) {
    handlerName = known;
  }

  if (handlerName === undefined) {
    return methodNotAllowed(module, new URL(request.url).pathname);
  }

  const args: ResourceRouteHandlerArgs = { request, params, context };
  try {
    const result = await module[handlerName]!(args);
    let response = toResourceResponse(result);
    if (fallsBackToGet) {
      response = new Response(null, { status: response.status, headers: response.headers });
    }
    return response;
  } catch (error) {
    const pathname = new URL(request.url).pathname;
    if (isProblemError(error)) {
      return error.toResponse(pathname);
    }
    if (isResponse(error)) {
      return error;
    }
    options.onError?.(error);
    return internalError("The resource route handler failed.").toResponse(pathname);
  }
}

function toResourceResponse(result: unknown): Response {
  if (isResponse(result)) {
    return result;
  }
  return new Response(JSON.stringify(result ?? null), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
}
