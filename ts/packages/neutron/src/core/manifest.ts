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
        : parentPath + "/" + entry.name;
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

function isRouteGroupDirectory(name: string): boolean {
  return name.startsWith("(") && name.endsWith(")");
}

function isLayoutFilename(fileName: string): boolean {
  const ext = path.extname(fileName);
  const baseName = path.basename(fileName, ext);
  return baseName === "_layout" && VALID_EXTENSIONS.includes(ext);
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

  if (isLayout) {
    const routePath = parentPath || "/";
    return {
      id: `route:${relativePath}`,
      path: routePath,
      file: filePath,
      pattern: new RegExp(`^${routePath === "/" ? "/" : routePath}$`),
      params: [],
      config,
      parentId,
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
    parentId,
  };
}

function readRouteConfig(filePath: string): RouteConfig {
  try {
    const content = fs.readFileSync(filePath, "utf-8");
    return parseRouteConfig(content);
  } catch {
    return { mode: "static" };
  }
}

function fileToRoutePath(filename: string, parentPath: string): string {
  let name = filename;

  if (name === "index") {
    return parentPath || "/";
  }

  const segments = name.split(".");
  const pathSegments: string[] = [];

  for (const segment of segments) {
    if (segment.startsWith("[...") && segment.endsWith("]")) {
      pathSegments.push("*");
    } else if (segment.startsWith("[") && segment.endsWith("]")) {
      pathSegments.push(":" + segment.slice(1, -1));
    } else {
      pathSegments.push(segment);
    }
  }

  const path = pathSegments.join("/");
  return parentPath + "/" + path;
}

function pathToRegExp(routePath: string): { pattern: RegExp; params: string[] } {
  const params: string[] = [];
  let regexStr = "^";

  const segments = routePath.split("/").filter(Boolean);

  if (segments.length === 0) {
    return { pattern: /^\/$/, params: [] };
  }

  for (const segment of segments) {
    if (segment === "*") {
      params.push("*");
      regexStr += "/(.*)";
    } else if (segment.startsWith(":")) {
      params.push(segment.slice(1));
      regexStr += "/([^/]+)";
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
