export interface NeutronGeneratedRouteMap {}

type GeneratedRoutePath = NeutronGeneratedRouteMap extends { paths: infer TPaths }
  ? TPaths
  : string;

export type RoutePath = GeneratedRoutePath extends string ? GeneratedRoutePath : string;

export type RouteHref =
  | RoutePath
  | `${RoutePath}?${string}`
  | `${RoutePath}#${string}`
  | `${RoutePath}?${string}#${string}`
  | `http://${string}`
  | `https://${string}`
  | `//${string}`;

export function route<T extends RouteHref>(value: T): T {
  return value;
}
