import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { discoverRoutes } from "./manifest.js";
import { loadRouteTable, writeRouteTable } from "./route-table.js";

let dir = "";
afterEach(() => {
  if (dir) fs.rmSync(dir, { recursive: true, force: true });
  dir = "";
});

function makeApp(root: string): void {
  const write = (file: string, content: string) => {
    fs.mkdirSync(path.dirname(path.join(root, file)), { recursive: true });
    fs.writeFileSync(path.join(root, file), content);
  };
  write("src/routes/_layout.tsx", "export default function L({ children }) { return children; }\n");
  write("src/routes/index.tsx", 'export const config = { mode: "app" };\nexport async function loader() { return {}; }\nexport default function P() { return null; }\n');
  write("src/routes/blog/[slug].tsx", "export function middleware(r, c, next) { return next(); }\nexport default function P() { return null; }\n");
  write("src/routes/api/ping.ts", "export function GET() { return new Response('pong'); }\n");
}

describe("route table", () => {
  it("round-trips the discovered table and goes stale when src/routes changes", () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), "neutron-route-table-"));
    makeApp(dir);
    const routesDir = path.join(dir, "src/routes");
    const distDir = path.join(dir, "dist");
    const routes = discoverRoutes({ routesDir });
    expect(writeRouteTable(dir, routesDir, distDir, routes)).toBe(true);
    expect(loadRouteTable(dir, routesDir, distDir)).toEqual(routes);

    fs.appendFileSync(path.join(routesDir, "index.tsx"), "export const action = async () => null;\n");
    expect(loadRouteTable(dir, routesDir, distDir)).toBeNull();

    writeRouteTable(dir, routesDir, distDir, discoverRoutes({ routesDir }));
    fs.writeFileSync(path.join(routesDir, "added.tsx"), "export default function P() { return null; }\n");
    expect(loadRouteTable(dir, routesDir, distDir)).toBeNull();
  });

  it("resolves files against the app root it is loaded from", () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), "neutron-route-table-"));
    const built = path.join(dir, "build-machine");
    makeApp(built);
    writeRouteTable(built, path.join(built, "src/routes"), path.join(built, "dist"), discoverRoutes({ routesDir: path.join(built, "src/routes") }));
    const moved = path.join(dir, "app");
    fs.renameSync(built, moved);
    const loaded = loadRouteTable(moved, path.join(moved, "src/routes"), path.join(moved, "dist"));
    expect(loaded).toEqual(discoverRoutes({ routesDir: path.join(moved, "src/routes") }));
  });

  it("is not written when a route config holds something JSON cannot carry", () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), "neutron-route-table-"));
    makeApp(dir);
    const routesDir = path.join(dir, "src/routes");
    const routes = discoverRoutes({ routesDir });
    (routes[0].config as unknown as Record<string, unknown>).pattern = /x/;
    expect(writeRouteTable(dir, routesDir, path.join(dir, "dist"), routes)).toBe(false);
    expect(loadRouteTable(dir, routesDir, path.join(dir, "dist"))).toBeNull();
  });
});
