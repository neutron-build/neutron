import type * as preact from "preact";
import type { SeoMetaInput } from "./seo.js";
import type { ResourceRouteMethod } from "./resource-routes.js";

export interface RouteCacheConfig {
  maxAge?: number;
  loaderMaxAge?: number;
}

export interface RouteConfig {
  mode: "static" | "app";
  cache?: RouteCacheConfig;
  hydrate?: boolean;
}

export interface Route {
  id: string;
  path: string;
  file: string;
  params: string[];
  config: RouteConfig;
  /**
   * Whether the route source exports a `loader`. Derived at discovery time,
   * not declared: the client build strips `loader`, so this is the only way
   * the browser can know a route needs server data. Absent means unknown,
   * which callers must treat as "yes" — see `parseRouteFacts`.
   */
  hasLoader?: boolean;
  /**
   * Whether the route source exports `middleware`. Derived at discovery time
   * like `hasLoader`, and read by the static-serving path: answering from a
   * prebuilt file returns before `renderAppRoute`, the only place middleware
   * runs, so a gated route served that way is served ungated. Absent means
   * unknown, which callers must treat as "yes" — a route wrongly kept off the
   * static fast path is slow, one wrongly put on it is public. See A-020.
   */
  hasMiddleware?: boolean;
  /**
   * Whether the route source exports an `action`. Derived at discovery time
   * like `hasLoader`; read by OpenAPI generation, which documents a POST
   * operation only when an action exists to serve it. Absent means unknown,
   * which callers must treat as "yes".
   */
  hasAction?: boolean;
  /**
   * Whether the route source exports HTTP method handlers (`GET`, `POST`,
   * `PUT`, `PATCH`, `DELETE`, `OPTIONS`, `HEAD`). Derived at discovery time
   * like `hasLoader` — the client build strips these exports, so the route
   * table is the only place the fact can live. Absent means unknown, which
   * callers must treat as "no" (a page mis-served as an endpoint is worse
   * than an endpoint mis-served as a 404).
   */
  hasMethodHandlers?: boolean;
  /**
   * Which HTTP method handlers the route source exports (`GET`, `POST`, …),
   * derived at discovery. Present only where facts were derived; an absent
   * value means unknown and callers must treat it as "none" — OpenAPI
   * generation documents an operation per entry and fabricates nothing when
   * the list is unavailable.
   */
  resourceMethods?: ResourceRouteMethod[];
  /**
   * Whether the route source has a default export (a component). Derived at
   * discovery time; combined with `hasMethodHandlers` it decides resource
   * route classification — see `isResourceRoute` in core/resource-routes.ts.
   */
  hasDefaultExport?: boolean;
  parentId: string | null;
  isLayout?: boolean;
  /**
   * A `not-found.tsx` — the page rendered when nothing in this directory's
   * subtree matched. It is never matched by URL (`/not-found` must 404 like
   * any other unknown path) and is reached only by the 404 handler, which
   * renders it through its own layout chain like any other route.
   */
  isNotFound?: boolean;
}

export interface RouteMatch {
  route: Route;
  params: Record<string, string>;
  layouts: Route[];
}

export interface LoaderArgs {
  request: Request;
  params: Record<string, string>;
  context: AppContext;
}

export interface ActionArgs {
  request: Request;
  params: Record<string, string>;
  context: AppContext;
}

/** Arguments handed to a resource route's HTTP method handlers. */
export interface ResourceRouteHandlerArgs {
  request: Request;
  params: Record<string, string>;
  context: AppContext;
}

/**
 * One HTTP method handler on a resource route module. Return a `Response` to
 * answer it verbatim, or any other value to answer it as JSON; throwing a
 * `ProblemError` or a `Response` works exactly like it does in loaders.
 */
export type ResourceRouteHandler = (
  args: ResourceRouteHandlerArgs
) => Promise<Response | unknown> | Response | unknown;

export interface HeadersArgs {
  request: Request;
  params: Record<string, string>;
  context: AppContext;
  loaderData: Record<string, unknown>;
  actionData?: unknown;
}

export interface HeadArgs {
  request: Request;
  params: Record<string, string>;
  context: AppContext;
  loaderData: Record<string, unknown>;
  /** The current route's own loader data (same as what the component receives via `data` prop). */
  data?: unknown;
  actionData?: unknown;
  pathname: string;
}

export interface AppContext {
  [key: string]: unknown;
}

export type MiddlewareFn = (
  request: Request,
  context: AppContext,
  next: () => Promise<Response>
) => Promise<Response>;

export interface ErrorBoundaryProps {
  error: Error;
  reset?: () => void;
}

export interface GetStaticPathsResult {
  paths: Array<{
    params: Record<string, string>;
    props?: Record<string, unknown>;
  }>;
}

export interface ShouldRevalidateFunctionArgs {
  currentUrl: URL;
  nextUrl: URL;
  formMethod?: string;
  formAction?: string;
  formEncType?: string;
  defaultShouldRevalidate: boolean;
  actionStatus?: number;
  actionResult?: unknown;
}

export interface ClientLoaderArgs {
  request: Request;
  params: Record<string, string>;
  serverLoader: () => Promise<unknown>;
}

export interface ClientActionArgs {
  request: Request;
  params: Record<string, string>;
  serverAction: () => Promise<unknown>;
}

export interface RouteModule {
  config?: RouteConfig;
  loader?: (args: LoaderArgs) => Promise<unknown>;
  action?: (args: ActionArgs) => Promise<unknown>;
  /**
   * HTTP method handlers for resource routes — route modules with method
   * handlers and NO default component, served as endpoints rather than pages
   * (see `isResourceRoute`). A `Response` (or a thrown Response/ProblemError)
   * is answered as-is; any other return value is serialized as JSON.
   * `HEAD` falls back to `GET` per RFC 9110 §9.3.2; an unsupported method
   * answers 405 `application/problem+json` with an `Allow` header.
   */
  GET?: ResourceRouteHandler;
  POST?: ResourceRouteHandler;
  PUT?: ResourceRouteHandler;
  PATCH?: ResourceRouteHandler;
  DELETE?: ResourceRouteHandler;
  OPTIONS?: ResourceRouteHandler;
  HEAD?: ResourceRouteHandler;
  clientLoader?: (args: ClientLoaderArgs) => Promise<unknown>;
  clientAction?: (args: ClientActionArgs) => Promise<unknown>;
  headers?:
    | ((
        args: HeadersArgs
      ) => Headers | Record<string, string> | Promise<Headers | Record<string, string>>);
  head?: (
    args: HeadArgs
  ) =>
    | SeoMetaInput
    | string
    | null
    | undefined
    | Promise<SeoMetaInput | string | null | undefined>;
  middleware?: MiddlewareFn;
  getStaticPaths?: () => Promise<GetStaticPathsResult>;
  shouldRevalidate?: (args: ShouldRevalidateFunctionArgs) => boolean;
  handle?: unknown;
  default?: preact.FunctionComponent<any>;
  ErrorBoundary?: preact.FunctionComponent<ErrorBoundaryProps>;
}

/**
 * Extracts the serializable return type from a loader or action function.
 *
 * Usage:
 *   useLoaderData<typeof loader>()  ->  resolves to the loader's return type
 *   useLoaderData<{ name: string }>()  ->  passes through as-is
 *
 * Response types are excluded since thrown responses (redirects, 404s)
 * are not part of the data contract.
 */
export type SerializeFrom<T> = T extends (...args: any[]) => Promise<infer R>
  ? R extends Response ? never : R
  : T extends (...args: any[]) => infer R
  ? R extends Response ? never : R
  : T;
