import * as fs from "node:fs";
import * as path from "node:path";
import type { Route, RouteConfig } from "./types.js";

export interface DiscoverOptions {
  routesDir: string;
}

const VALID_EXTENSIONS = [".tsx", ".ts", ".jsx", ".js", ".mdx"];

export function discoverRoutes(options: DiscoverOptions): Route[] {
  const { routesDir } = options;
  const routes: Route[] = [];

  if (!fs.existsSync(routesDir)) {
    return routes;
  }

  walkDir(routesDir, "", routes, routesDir, null);
  validateRoutes(routes);
  sortRoutes(routes);

  return routes;
}

function walkDir(
  dir: string,
  parentPath: string,
  routes: Route[],
  routesDir: string,
  parentId: string | null
): void {
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  let layoutId: string | null = parentId;

  const layoutFile = entries.find(
    (e) => e.isFile() && isLayoutFilename(e.name)
  );
  if (layoutFile) {
    const layoutRoute = createRoute(
      path.join(dir, layoutFile.name),
      parentPath,
      routesDir,
      parentId,
      true
    );
    if (layoutRoute) {
      routes.push(layoutRoute);
      layoutId = layoutRoute.id;
    }
  }

  for (const entry of entries) {
    if (entry.name.startsWith(".")) {
      continue;
    }

    if (entry.name.startsWith("_") && !isLayoutFilename(entry.name)) {
      continue;
    }

    if (isLayoutFilename(entry.name)) {
      continue;
    }

    const fullPath = path.join(dir, entry.name);

    if (entry.isDirectory()) {
      const dirPath = isRouteGroupDirectory(entry.name)
        ? parentPath
        : parentPath + "/" + dirToRouteSegment(entry.name);
      walkDir(fullPath, dirPath, routes, routesDir, layoutId);
    } else if (entry.isFile()) {
      if (entry.name.endsWith(".d.ts")) {
        continue;
      }
      const ext = path.extname(entry.name);
      if (!VALID_EXTENSIONS.includes(ext)) continue;

      const route = createRoute(fullPath, parentPath, routesDir, layoutId, false);
      if (route) {
        routes.push(route);
      }
    }
  }
}

/**
 * Reject route tables that cannot do what the filenames say they do.
 *
 * Every case here used to be silent: the route was still registered, just at a
 * path nothing would ever request, so the only symptom was a 404 at runtime with
 * no build error and no warning. A wrong route table is a build failure.
 */
function validateRoutes(routes: Route[]): void {
  for (const route of routes) {
    const segments = route.path.split("/").filter(Boolean);

    for (const segment of segments) {
      if (segment.includes("[") || segment.includes("]")) {
        throw new Error(
          `Neutron: malformed dynamic segment "${segment}" in route "${route.path}"\n` +
            `  from ${route.file}\n` +
            `  A dynamic segment is [name] and a catch-all is [...name]; ` +
            `write [.] for a literal dot.`
        );
      }
    }

    const catchAllIndex = segments.findIndex((s) => s.startsWith("*"));
    if (catchAllIndex !== -1 && catchAllIndex !== segments.length - 1) {
      throw new Error(
        `Neutron: catch-all segment "${segments[catchAllIndex]}" is not last in route "${route.path}"\n` +
          `  from ${route.file}\n` +
          `  A catch-all consumes the rest of the URL, so nothing after it can ever match. ` +
          `Use [name] instead of [...name] for that directory.`
      );
    }
  }

  // Two files that resolve to the same shape: only one of them can ever be
  // matched, and which one is an accident of discovery order. Param names are
  // erased first, so /users/:id and /users/:name collide too — the router keeps
  // one name per position, so the loser's handler would read an empty param.
  // Layouts are excluded: a layout legitimately shares its directory's path.
  const byShape = new Map<string, Route[]>();
  for (const route of routes) {
    // A not-found page shares its directory's path the way a layout does, and
    // is never matched by URL, so it cannot collide with anything.
    if (route.isLayout || route.isNotFound) continue;
    // Erase param names but keep any literal suffix, since that suffix is what
    // distinguishes /docs/*slug from /docs/*slug.md.
    const shape = route.path
      .split("/")
      .map((segment) => segment.replace(/^([:*])[^./]*/, "$1"))
      .join("/");
    const existing = byShape.get(shape);
    if (existing) {
      existing.push(route);
    } else {
      byShape.set(shape, [route]);
    }
  }
  for (const group of byShape.values()) {
    if (group.length < 2) continue;
    const detail = group.map((r) => `    ${r.path}  <- ${r.file}`).join("\n");
    throw new Error(
      `Neutron: ${group.length} routes resolve to the same URL shape\n${detail}\n` +
        `  Only one of them can ever match. Rename or remove the others.`
    );
  }
}

function isRouteGroupDirectory(name: string): boolean {
  return name.startsWith("(") && name.endsWith(")");
}

function isLayoutFilename(fileName: string): boolean {
  const ext = path.extname(fileName);
  const baseName = path.basename(fileName, ext);
  return baseName === "_layout" && VALID_EXTENSIONS.includes(ext);
}

/**
 * `not-found.tsx` is the 404 page for its directory's subtree.
 *
 * It is discovered like a route so it inherits a layout chain — that is the
 * whole point of the convention, since `notFound()` alone can only return a
 * standalone document with none of the app's chrome. It is then withheld from
 * URL matching, because a file that renders "not found" must not itself be
 * reachable at `/not-found`.
 */
function isNotFoundFilename(fileName: string): boolean {
  const ext = path.extname(fileName);
  const baseName = path.basename(fileName, ext);
  return baseName === "not-found" && VALID_EXTENSIONS.includes(ext);
}

function createRoute(
  filePath: string,
  parentPath: string,
  routesDir: string,
  parentId: string | null,
  isLayout: boolean
): Route | null {
  const relativePath = path.relative(routesDir, filePath).replace(/\\/g, "/");
  const ext = path.extname(filePath);
  let name = path.basename(filePath, ext);
  const config = readRouteConfig(filePath);
  const derived = readRouteFacts(filePath);

  if (isLayout) {
    const routePath = parentPath || "/";
    return {
      id: `route:${relativePath}`,
      path: routePath,
      file: filePath,
      pattern: new RegExp(`^${routePath === "/" ? "/" : routePath}$`),
      params: [],
      config,
      hasLoader: derived.hasLoader,
      hasMiddleware: derived.hasMiddleware,
      parentId,
      isLayout: true,
    };
  }

  // A not-found page belongs to its directory, not to a URL segment named
  // after the file. Giving it the directory's path is what lets the 404
  // handler pick the deepest one covering the request.
  if (isNotFoundFilename(path.basename(filePath))) {
    const routePath = parentPath || "/";
    return {
      id: `route:${relativePath}`,
      path: routePath,
      file: filePath,
      pattern: new RegExp(`^${routePath === "/" ? "/" : routePath}$`),
      params: [],
      config,
      hasLoader: derived.hasLoader,
      hasMiddleware: derived.hasMiddleware,
      parentId,
      isLayout: false,
      isNotFound: true,
    };
  }

  const routePath = fileToRoutePath(name, parentPath);
  const { pattern, params } = pathToRegExp(routePath);

  return {
    id: `route:${relativePath}`,
    path: routePath,
    file: filePath,
    pattern,
    params,
    config,
    hasLoader: derived.hasLoader,
    hasMiddleware: derived.hasMiddleware,
    parentId,
    isLayout: false,
  };
}

/**
 * The not-found page covering `urlPath`: the deepest one whose directory is a
 * prefix of the request.
 *
 * Deepest wins so a section can present its own 404 — a miss under `/admin`
 * should look like the admin app, not like the marketing site — while the root
 * page still catches everything else.
 */
export function findNotFoundRoute(routes: Route[], urlPath: string): Route | undefined {
  let best: Route | undefined;
  for (const route of routes) {
    if (!route.isNotFound) continue;
    const base = route.path === "/" ? "/" : route.path + "/";
    const candidate = urlPath.endsWith("/") ? urlPath : urlPath + "/";
    if (base === "/" || candidate.startsWith(base)) {
      if (!best || route.path.length > best.path.length) {
        best = route;
      }
    }
  }
  return best;
}

/**
 * Facts about a route derived from its source, as opposed to declared in its
 * `config`. The client needs these to decide how much runtime a page requires,
 * and it cannot ask the module: `stripServerOnlyRouteModule` removes `loader`,
 * `action`, `middleware` and `headers` from the client build, so in the browser
 * every route looks loader-free. If it is not carried in the route table, it is
 * not knowable client-side.
 */
export interface RouteFacts {
  hasLoader: boolean;
  /**
   * Whether the route source exports `middleware`. The static-serving path
   * needs this before it decides to answer from a prebuilt file: serving one
   * skips `renderAppRoute`, which is the only place middleware runs, so a
   * gated route would be served ungated. See A-020.
   */
  hasMiddleware: boolean;
}

/**
 * Detects a `loader` export by source shape, matching how the server-only
 * stripper identifies the same export.
 *
 * Deliberately conservative: anything ambiguous counts as HAVING a loader, so a
 * missed detection costs a network request that was already being made rather
 * than a page rendered without its data.
 */
export function parseRouteFacts(fileContent: string): RouteFacts {
  const declared =
    /export\s+(?:async\s+)?(?:function|const|let|var)\s+loader\b/.test(fileContent);
  // `export { loader }` and `export { x as loader }`, including re-exports.
  const named = /export\s*\{[^}]*\bloader\b[^}]*\}/.test(fileContent);
  // Same two shapes for `middleware`. Conservative in the same direction and
  // for a stronger reason: a false positive costs a static route its prebuilt
  // fast path, a false negative serves a gated page to anyone.
  const middlewareDeclared =
    /export\s+(?:async\s+)?(?:function|const|let|var)\s+middleware\b/.test(fileContent);
  const middlewareNamed = /export\s*\{[^}]*\bmiddleware\b[^}]*\}/.test(fileContent);
  return {
    hasLoader: declared || named,
    hasMiddleware: middlewareDeclared || middlewareNamed,
  };
}

function readRouteFacts(filePath: string): RouteFacts {
  try {
    return parseRouteFacts(fs.readFileSync(filePath, "utf-8"));
  } catch {
    // Unreadable source: assume the heavier path, and assume gated.
    return { hasLoader: true, hasMiddleware: true };
  }
}

function readRouteConfig(filePath: string): RouteConfig {
  try {
    const content = fs.readFileSync(filePath, "utf-8");
    return parseRouteConfig(content);
  } catch {
    return { mode: "static" };
  }
}

// Sentinel a literal-dot escape collapses to before the segment-splitting
// pass, so "[.]" survives as a "." in the final path instead of splitting
// the route (Remix's own flat-routes escape for a literal dot, e.g.
// "sitemap[.]xml.ts" -> "/sitemap.xml" rather than "/sitemap/xml"). Not a
// valid filename byte, so it can't collide with real input.
const DOT_ESCAPE = " ";

function fileToRoutePath(filename: string, parentPath: string): string {
  let name = filename;

  if (name === "index") {
    return parentPath || "/";
  }

  name = name.split("[.]").join(DOT_ESCAPE);

  // Split on "." for Remix-style flat routes (auth.login -> auth/login),
  // but preserve dots inside [...] dynamic segments.
  const segments: string[] = [];
  let buf = "";
  let depth = 0;
  for (const ch of name) {
    if (ch === "[") depth++;
    else if (ch === "]") depth--;
    if (ch === "." && depth === 0) {
      segments.push(buf);
      buf = "";
    } else {
      buf += ch;
    }
  }
  segments.push(buf);

  const pathSegments = segments.map((rawSegment) =>
    paramizeSegment(rawSegment.split(DOT_ESCAPE).join("."))
  );

  const path = pathSegments.join("/");
  return parentPath + "/" + path;
}

/**
 * Turn one filesystem segment into one route segment: `[id]` -> `:id`,
 * `[...slug]` -> `*slug`, anything else through unchanged.
 *
 * A dynamic or catch-all param may be followed by a literal suffix, e.g.
 * `[...slug].md` -> `*slug.md`, `[id].json` -> `:id.json`. Match the bracketed
 * param at the start and keep whatever trails the closing `]`.
 */
function paramizeSegment(segment: string): string {
  const catchAll = /^\[\.\.\.([^\]]+)\](.*)$/.exec(segment);
  if (catchAll) {
    // Catch-all: preserve the param name so consumers can read params.<name>.
    return "*" + catchAll[1] + catchAll[2];
  }
  const dynamic = /^\[([^\]]+)\](.*)$/.exec(segment);
  if (dynamic) {
    return ":" + dynamic[1] + dynamic[2];
  }
  return segment;
}

/**
 * Directory names carry params too — `api/runs/[id]/decide.tsx` is
 * `/api/runs/:id/decide`. A directory is already a path boundary, so unlike a
 * filename it is never split on ".", but it goes through the same param rule
 * and honours the same `[.]` literal-dot escape.
 */
function dirToRouteSegment(name: string): string {
  return paramizeSegment(name.split("[.]").join("."));
}

function pathToRegExp(routePath: string): { pattern: RegExp; params: string[] } {
  const params: string[] = [];
  let regexStr = "^";

  const segments = routePath.split("/").filter(Boolean);

  if (segments.length === 0) {
    return { pattern: /^\/$/, params: [] };
  }

  for (const segment of segments) {
    if (segment.startsWith("*")) {
      // Catch-all, optionally with a literal suffix (e.g. `*slug.md`): the
      // param name is up to the first dot, and the suffix must match literally.
      const rest = segment.slice(1);
      const dot = rest.indexOf(".");
      if (dot === -1) {
        params.push(rest || "*");
        regexStr += "/(.*)";
      } else {
        params.push(rest.slice(0, dot) || "*");
        regexStr += "/(.*?)" + escapeRegExp(rest.slice(dot));
      }
    } else if (segment.startsWith(":")) {
      const rest = segment.slice(1);
      const dot = rest.indexOf(".");
      if (dot === -1) {
        params.push(rest);
        regexStr += "/([^/]+)";
      } else {
        params.push(rest.slice(0, dot));
        regexStr += "/([^/]+?)" + escapeRegExp(rest.slice(dot));
      }
    } else {
      regexStr += "/" + escapeRegExp(segment);
    }
  }

  regexStr += "$";

  return { pattern: new RegExp(regexStr), params };
}

function escapeRegExp(str: string): string {
  return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function sortRoutes(routes: Route[]): void {
  routes.sort((a, b) => {
    const aSegments = a.path.split("/").filter(Boolean);
    const bSegments = b.path.split("/").filter(Boolean);

    if (aSegments.length !== bSegments.length) {
      return aSegments.length - bSegments.length;
    }

    for (let i = 0; i < aSegments.length; i++) {
      const aStatic = !aSegments[i].startsWith(":") && aSegments[i] !== "*";
      const bStatic = !bSegments[i].startsWith(":") && bSegments[i] !== "*";

      if (aStatic && !bStatic) return -1;
      if (!aStatic && bStatic) return 1;

      if (aSegments[i] === "*" && bSegments[i] !== "*") return 1;
      if (aSegments[i] !== "*" && bSegments[i] === "*") return -1;
    }

    return a.path.localeCompare(b.path);
  });
}

export function parseRouteConfig(fileContent: string): RouteConfig {
  let mode: RouteConfig["mode"] = "static";
  let cache: RouteConfig["cache"] | undefined;
  let hydrate: RouteConfig["hydrate"] | undefined;

  const configStr = extractConfigObjectLiteral(fileContent);
  if (configStr) {

    const modeMatch = configStr.match(/mode\s*:\s*["'](\w+)["']/);
    if (modeMatch && (modeMatch[1] === "static" || modeMatch[1] === "app")) {
      mode = modeMatch[1];
    }

    const hydrateMatch = configStr.match(/hydrate\s*:\s*(true|false)/);
    if (hydrateMatch) {
      hydrate = hydrateMatch[1] === "true";
    }

    const cacheMatch = configStr.match(/cache\s*:\s*\{([^}]*)\}/);
    if (cacheMatch) {
      const maxAgeMatch = cacheMatch[1].match(/maxAge\s*:\s*(\d+)/);
      const parsedMaxAge = Number.parseInt(maxAgeMatch?.[1] || "", 10);
      const loaderMaxAgeMatch = cacheMatch[1].match(/loaderMaxAge\s*:\s*(\d+)/);
      const parsedLoaderMaxAge = Number.parseInt(loaderMaxAgeMatch?.[1] || "", 10);

      const nextCache: RouteConfig["cache"] = {};
      if (Number.isFinite(parsedMaxAge) && parsedMaxAge > 0) {
        nextCache.maxAge = parsedMaxAge;
      }
      if (Number.isFinite(parsedLoaderMaxAge) && parsedLoaderMaxAge > 0) {
        nextCache.loaderMaxAge = parsedLoaderMaxAge;
      }
      if (nextCache.maxAge || nextCache.loaderMaxAge) {
        cache = nextCache;
      }
    }
  }

  const parsedConfig: RouteConfig = { mode };
  if (cache) {
    parsedConfig.cache = cache;
  }
  if (hydrate !== undefined) {
    parsedConfig.hydrate = hydrate;
  }
  return parsedConfig;
}

function extractConfigObjectLiteral(fileContent: string): string | null {
  const match = fileContent.match(/export\s+const\s+config\s*=\s*\{/);
  if (!match || match.index === undefined) {
    return null;
  }

  const start = match.index + match[0].lastIndexOf("{");
  let depth = 0;

  for (let i = start; i < fileContent.length; i++) {
    const ch = fileContent[i];
    if (ch === '"' || ch === "'" || ch === "`") {
      i = fileContent.indexOf(ch, i + 1);
      if (i === -1) return null;
      continue;
    }
    if (ch === "{") {
      depth += 1;
    } else if (ch === "}") {
      depth -= 1;
      if (depth === 0) {
        return fileContent.slice(start, i + 1);
      }
    }
  }

  return null;
}
