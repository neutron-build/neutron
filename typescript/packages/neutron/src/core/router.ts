import { findNotFoundMatch } from "./manifest.js";
import { parsePath, bindPathParams, parseUrlPath } from "./route-path.js";
import type { Route, RouteMatch } from "./types.js";

interface TrieNode {
  children: Map<string, TrieNode>;
  // Dynamic children key by their literal suffix — `/users/:id` and
  // `/users/:slug` share one edge, which is the point. The edge still records
  // a name for traversal ordering, but it is a hint, never the truth: names
  // are re-bound from the WINNING route's own tokens after selection
  // (NA-06 — the first inserter's name used to leak into every sibling's
  // params).
  paramChildren: Map<string, { node: TrieNode; name: string }>;
  wildcardChildren: Map<string, { node: TrieNode; name: string }>;
  route: Route | null;
}

function createNode(): TrieNode {
  return {
    children: new Map(),
    paramChildren: new Map(),
    wildcardChildren: new Map(),
    route: null,
  };
}

export function createRouter() {
  const root = createNode();
  const routes: Route[] = [];

  function insert(route: Route): void {
    routes.push(route);

    // A not-found page is reachable only through the 404 handler. Inserting it
    // into the trie would put it at its directory's own path, where it would
    // shadow that directory's index route with a "not found" page.
    if (route.isNotFound) {
      return;
    }

    const segments = parsePath(route.path);
    // Reject malformed routes here rather than discovering a duplicate or
    // misplaced parameter on the first request (NA-06): a route whose own
    // pattern cannot bind is a route-table bug, not a runtime 500.
    const seen = new Set<string>();
    for (const segment of segments) {
      if (segment.type === "static") continue;
      if (seen.has(segment.value)) {
        throw new Error(`Duplicate route parameter :${segment.value} in ${route.path} (${route.file})`);
      }
      seen.add(segment.value);
      if (segment.type === "wildcard" && segment !== segments[segments.length - 1]) {
        throw new Error(`Wildcard *${segment.value} must be the final segment of ${route.path} (${route.file})`);
      }
    }

    let node = root;

    for (const segment of segments) {
      if (segment.type === "static") {
        if (!node.children.has(segment.value)) {
          node.children.set(segment.value, createNode());
        }
        node = node.children.get(segment.value)!;
      } else if (segment.type === "param") {
        let child = node.paramChildren.get(segment.suffix);
        if (!child) {
          child = { node: createNode(), name: segment.value };
          node.paramChildren.set(segment.suffix, child);
        }
        node = child.node;
      } else if (segment.type === "wildcard") {
        let child = node.wildcardChildren.get(segment.suffix);
        if (!child) {
          child = { node: createNode(), name: segment.value || "*" };
          node.wildcardChildren.set(segment.suffix, child);
        }
        node = child.node;
      }
    }

    node.route = route;
  }

  function match(urlPath: string): RouteMatch | null {
    const segments = parseUrlPath(urlPath);
    // The scratch map steers traversal only (backtracking deletes as it
    // goes); it must not escape as public params. Names are bound from the
    // winning route's tokens below, so a shared dynamic edge built by a
    // differently-named sibling cannot misname the parameter (NA-06).
    const scratch: Record<string, string> = Object.create(null);

    const result = matchNode(root, segments, 0, scratch);
    if (!result) return null;

    const params = bindPathParams(result.path, segments);
    if (params === null) {
      // The trie selected a route whose own pattern cannot bind the URL it
      // just matched — a corrupted route table, not a client error.
      throw new Error(`Router matched an inconsistent route pattern: ${result.path} for ${urlPath}`);
    }

    const layouts = getLayouts(result, routes);

    return {
      route: result,
      params,
      layouts,
    };
  }

  /**
   * The `not-found.tsx` covering `urlPath`, as a match ready to render.
   *
   * Returned as a full `RouteMatch` — with its layout chain, and with the
   * scope's parameters bound (NA-07: a `not-found.tsx` under `[orgId]` used
   * to never match its dynamic scope, and even a matched one arrived with
   * `params: {}`) — because that is the entire reason the convention exists:
   * `notFound()` can only produce a standalone document, so a 404 arrived
   * with none of the app's chrome. This lets the 404 render through exactly
   * the same path as any other page.
   */
  function matchNotFound(urlPath: string): RouteMatch | null {
    const found = findNotFoundMatch(routes, urlPath);
    if (!found) return null;
    return { route: found.route, params: found.params, layouts: getLayouts(found.route, routes) };
  }

  function matchNode(
    node: TrieNode,
    segments: string[],
    index: number,
    params: Record<string, string>
  ): Route | null {
    if (index === segments.length) {
      return node.route;
    }

    const segment = segments[index];

    const staticChild = node.children.get(segment);
    if (staticChild) {
      const result = matchNode(staticChild, segments, index + 1, params);
      if (result) return result;
    }

    for (const [suffix, child] of dynamicChildren(node.paramChildren)) {
      if (suffix && !segment.endsWith(suffix)) continue;
      const value = suffix ? segment.slice(0, -suffix.length) : segment;
      if (!value) continue;
      params[child.name] = value;
      const result = matchNode(child.node, segments, index + 1, params);
      if (result) return result;
      delete params[child.name];
    }

    const remaining = segments.slice(index).join("/");
    for (const [suffix, child] of dynamicChildren(node.wildcardChildren)) {
      if (suffix && !remaining.endsWith(suffix)) continue;
      const value = suffix ? remaining.slice(0, -suffix.length) : remaining;
      if (!value) continue;
      params[child.name] = value;
      const result = matchNode(child.node, segments, segments.length, params);
      if (result) return result;
      delete params[child.name];
    }

    return null;
  }

  function getRoutes(): Route[] {
    return [...routes];
  }

  return { insert, match, matchNotFound, getRoutes };
}

// A suffixed route such as `*slug.md` is more specific than a plain catch-all.
// Try it first so `/docs/intro.md` reaches the markdown resource route while
// `/docs/intro` continues to reach the page route.
function dynamicChildren<T>(children: Map<string, T>): Array<[string, T]> {
  return [...children.entries()].sort(([a], [b]) => b.length - a.length);
}

function getLayouts(route: Route, allRoutes: Route[]): Route[] {
  const routeMap = new Map(allRoutes.map((r) => [r.id, r]));
  const layouts: Route[] = [];
  let currentId: string | null = route.parentId;

  while (currentId) {
    const parent = routeMap.get(currentId);
    if (parent) {
      layouts.unshift(parent);
      currentId = parent.parentId;
    } else {
      break;
    }
  }

  return layouts;
}
