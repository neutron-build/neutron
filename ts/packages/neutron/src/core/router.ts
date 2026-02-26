import type { Route, RouteMatch } from "./types.js";

interface TrieNode {
  children: Map<string, TrieNode>;
  paramChild: TrieNode | null;
  paramName: string | null;
  wildcardChild: TrieNode | null;
  route: Route | null;
}

function createNode(): TrieNode {
  return {
    children: new Map(),
    paramChild: null,
    paramName: null,
    wildcardChild: null,
    route: null,
  };
}

export function createRouter() {
  const root = createNode();
  const routes: Route[] = [];

  function insert(route: Route): void {
    routes.push(route);
    const segments = parsePath(route.path);
    let node = root;

    for (const segment of segments) {
      if (segment.type === "static") {
        if (!node.children.has(segment.value)) {
          node.children.set(segment.value, createNode());
        }
        node = node.children.get(segment.value)!;
      } else if (segment.type === "param") {
        if (!node.paramChild) {
          node.paramChild = createNode();
        }
        node.paramName = segment.value;
        node = node.paramChild;
      } else if (segment.type === "wildcard") {
        if (!node.wildcardChild) {
          node.wildcardChild = createNode();
        }
        node = node.wildcardChild;
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

    if (node.paramChild && node.paramName) {
      params[node.paramName] = segment;
      const result = matchNode(node.paramChild, segments, index + 1, params);
      if (result) return result;
      delete params[node.paramName];
    }

    if (node.wildcardChild) {
      const wildcardParam = segments.slice(index).join("/");
      params["*"] = wildcardParam;
      const result = matchNode(node.wildcardChild, segments, segments.length, params);
      if (result) return result;
      delete params["*"];
    }

    return null;
  }

  function getRoutes(): Route[] {
    return [...routes];
  }

  return { insert, match, getRoutes };
}

type PathSegment =
  | { type: "static"; value: string }
  | { type: "param"; value: string }
  | { type: "wildcard"; value: string };

function parsePath(path: string): PathSegment[] {
  const parts = path.split("/").filter(Boolean);
  const segments: PathSegment[] = [];

  for (const part of parts) {
    if (part === "*") {
      segments.push({ type: "wildcard", value: "*" });
    } else if (part.startsWith(":")) {
      segments.push({ type: "param", value: part.slice(1) });
    } else {
      segments.push({ type: "static", value: part });
    }
  }

  return segments;
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
