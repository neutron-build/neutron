import { findNotFoundRoute } from "./manifest.js";
import type { Route, RouteMatch } from "./types.js";

interface TrieNode {
  children: Map<string, TrieNode>;
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
    const params: Record<string, string> = {};
    
    const result = matchNode(root, segments, 0, params);
    if (!result) return null;

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
   * Returned as a full `RouteMatch` — with its layout chain — because that is
   * the entire reason the convention exists: `notFound()` can only produce a
   * standalone document, so a 404 arrived with none of the app's chrome. This
   * lets the 404 render through exactly the same path as any other page.
   */
  function matchNotFound(urlPath: string): RouteMatch | null {
    const route = findNotFoundRoute(routes, urlPath);
    if (!route) return null;
    return { route, params: {}, layouts: getLayouts(route, routes) };
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

type PathSegment =
  | { type: "static"; value: string }
  | { type: "param"; value: string; suffix: string }
  | { type: "wildcard"; value: string; suffix: string };

function parsePath(path: string): PathSegment[] {
  const parts = path.split("/").filter(Boolean);
  const segments: PathSegment[] = [];

  for (const part of parts) {
    if (part.startsWith("*")) {
      const { name, suffix } = splitDynamicSegment(part.slice(1), "*");
      segments.push({ type: "wildcard", value: name, suffix });
    } else if (part.startsWith(":")) {
      const { name, suffix } = splitDynamicSegment(part.slice(1), "");
      segments.push({ type: "param", value: name, suffix });
    } else {
      segments.push({ type: "static", value: part });
    }
  }

  return segments;
}

function splitDynamicSegment(value: string, fallback: string): { name: string; suffix: string } {
  const dot = value.indexOf(".");
  if (dot === -1) return { name: value || fallback, suffix: "" };
  return { name: value.slice(0, dot) || fallback, suffix: value.slice(dot) };
}

// A suffixed route such as `*slug.md` is more specific than a plain catch-all.
// Try it first so `/docs/intro.md` reaches the markdown resource route while
// `/docs/intro` continues to reach the page route.
function dynamicChildren<T>(children: Map<string, T>): Array<[string, T]> {
  return [...children.entries()].sort(([a], [b]) => b.length - a.length);
}

function parseUrlPath(path: string): string[] {
  return path.split("/").filter(Boolean);
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
