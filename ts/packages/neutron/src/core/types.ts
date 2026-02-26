import type * as preact from "preact";
import type { SeoMetaInput } from "./seo.js";

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
  pattern: RegExp;
  params: string[];
  config: RouteConfig;
  parentId: string | null;
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
