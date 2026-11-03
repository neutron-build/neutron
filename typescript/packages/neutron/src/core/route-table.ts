import { createHash } from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import type { Route } from "./types.js";

/**
 * The route table `neutron-ts build` discovered, written to the dist root so
 * the production server does not parse every route source at boot (which
 * also loads @babel/parser into the server). It is keyed on a fingerprint of
 * the routes directory: any added, removed or edited file there makes the
 * table stale, and the server discovers routes from source instead.
 */
export const ROUTE_TABLE_FILE = ".neutron-routes.json";
const ROUTE_TABLE_FORMAT = 1;

interface RouteTableFile {
  format: number;
  routesDir: string;
  fingerprint: string;
  routes: Route[];
}

/** Hash of every file under the routes directory: relative path and bytes. */
export function routesDirFingerprint(routesDir: string): string {
  const hash = createHash("sha1");
  const walk = (dir: string, prefix: string) => {
    const entries = fs.readdirSync(dir, { withFileTypes: true }).sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
    for (const entry of entries) {
      const relative = prefix + entry.name;
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        hash.update(`d:${relative}\0`);
        walk(full, `${relative}/`);
      } else if (entry.isFile()) {
        hash.update(`f:${relative}\0`);
        hash.update(fs.readFileSync(full));
        hash.update("\0");
      }
    }
  };
  if (fs.existsSync(routesDir)) walk(routesDir, "");
  return hash.digest("hex");
}

const toPosix = (value: string) => value.split(path.sep).join("/");

/**
 * Write the table, file paths relative to the app root so the output can
 * move (a container image built elsewhere). A table holding anything JSON
 * cannot carry (a RegExp or function in a route config) is not written; the
 * server then discovers as before.
 */
export function writeRouteTable(rootDir: string, routesDir: string, distDir: string, routes: Route[]): boolean {
  const relativeRoutes = routes.map((route) => ({ ...route, file: toPosix(path.relative(rootDir, route.file)) }));
  let lossless = true;
  const serialized = JSON.stringify(relativeRoutes, (_key, value: unknown) => {
    const kind = typeof value;
    if (kind === "function" || kind === "bigint" || kind === "symbol") lossless = false;
    else if (kind === "number" && !Number.isFinite(value)) lossless = false;
    else if (
      value !== null &&
      kind === "object" &&
      !Array.isArray(value) &&
      Object.getPrototypeOf(value) !== Object.prototype &&
      Object.getPrototypeOf(value) !== null
    ) {
      lossless = false;
    }
    return lossless ? value : undefined;
  });
  if (!lossless) return false;
  const table: RouteTableFile = {
    format: ROUTE_TABLE_FORMAT,
    routesDir: toPosix(path.relative(rootDir, routesDir)),
    fingerprint: routesDirFingerprint(routesDir),
    routes: JSON.parse(serialized) as Route[],
  };
  fs.mkdirSync(distDir, { recursive: true });
  fs.writeFileSync(path.join(distDir, ROUTE_TABLE_FILE), JSON.stringify(table));
  return true;
}

/** The build's route table when it still describes the routes directory, else null. */
export function loadRouteTable(rootDir: string, routesDir: string, distDir: string): Route[] | null {
  let table: RouteTableFile;
  try {
    table = JSON.parse(fs.readFileSync(path.join(distDir, ROUTE_TABLE_FILE), "utf-8")) as RouteTableFile;
  } catch {
    return null;
  }
  if (
    table?.format !== ROUTE_TABLE_FORMAT ||
    !Array.isArray(table.routes) ||
    table.routesDir !== toPosix(path.relative(rootDir, routesDir)) ||
    table.fingerprint !== routesDirFingerprint(routesDir)
  ) {
    return null;
  }
  return table.routes.map((route) => ({ ...route, file: path.resolve(rootDir, route.file) }));
}
