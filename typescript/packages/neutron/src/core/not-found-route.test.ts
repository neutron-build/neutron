import { describe, it, expect, beforeAll, afterAll } from "vitest";
import * as fs from "node:fs";
import * as path from "node:path";
import { discoverRoutes, findNotFoundRoute } from "./manifest.js";
import { createRouter } from "./router.js";

const DIR = path.join(__dirname, "__nf_test_routes__");

/**
 * The `not-found.tsx` convention. `notFound()` can only return a standalone
 * document, so a 404 arrived with none of the app's chrome; this convention is
 * what lets it render through the layout chain like any other page.
 */
beforeAll(() => {
  fs.mkdirSync(path.join(DIR, "admin"), { recursive: true });

  fs.writeFileSync(path.join(DIR, "_layout.tsx"), "export default function L() {}");
  fs.writeFileSync(path.join(DIR, "index.tsx"), "export default function Home() {}");
  fs.writeFileSync(path.join(DIR, "not-found.tsx"), "export default function NF() {}");

  fs.writeFileSync(path.join(DIR, "admin", "_layout.tsx"), "export default function AL() {}");
  fs.writeFileSync(path.join(DIR, "admin", "index.tsx"), "export default function AI() {}");
  fs.writeFileSync(path.join(DIR, "admin", "not-found.tsx"), "export default function ANF() {}");
});

afterAll(() => {
  fs.rmSync(DIR, { recursive: true, force: true });
});

describe("not-found route convention", () => {
  it("marks not-found files and gives them their directory's path", () => {
    const routes = discoverRoutes({ routesDir: DIR });
    const roots = routes.filter((r) => r.isNotFound);
    expect(roots).toHaveLength(2);
    expect(roots.map((r) => r.path).sort()).toEqual(["/", "/admin"]);
  });

  // A page that says "not found" must not itself be reachable at /not-found,
  // and must not shadow its directory's index route.
  it("is not reachable by URL", () => {
    const routes = discoverRoutes({ routesDir: DIR });
    const router = createRouter();
    for (const r of routes) router.insert(r);

    expect(router.match("/not-found")).toBeNull();
    expect(router.match("/admin/not-found")).toBeNull();
  });

  it("does not shadow the directory's index route", () => {
    const routes = discoverRoutes({ routesDir: DIR });
    const router = createRouter();
    for (const r of routes) router.insert(r);

    const home = router.match("/");
    expect(home?.route.isNotFound).toBeFalsy();
    expect(home?.route.file).toContain("index");

    const admin = router.match("/admin");
    expect(admin?.route.isNotFound).toBeFalsy();
    expect(admin?.route.file).toContain("index");
  });

  // Deepest wins, so a miss under /admin looks like the admin app rather than
  // like the marketing site.
  it("picks the deepest not-found covering the path", () => {
    const routes = discoverRoutes({ routesDir: DIR });

    expect(findNotFoundRoute(routes, "/admin/nope")?.path).toBe("/admin");
    expect(findNotFoundRoute(routes, "/nope")?.path).toBe("/");
    expect(findNotFoundRoute(routes, "/administrative")?.path).toBe("/");
  });

  it("resolves through the router with its layout chain", () => {
    const routes = discoverRoutes({ routesDir: DIR });
    const router = createRouter();
    for (const r of routes) router.insert(r);

    const match = router.matchNotFound("/admin/missing");
    expect(match).not.toBeNull();
    expect(match!.route.path).toBe("/admin");
    // Root layout then admin layout — the chrome a bare notFound() cannot give.
    expect(match!.layouts.length).toBeGreaterThanOrEqual(2);
    expect(match!.layouts.every((l) => l.isLayout)).toBe(true);
  });

  it("returns null when the app has no not-found page", () => {
    const bare = path.join(__dirname, "__nf_bare__");
    fs.mkdirSync(bare, { recursive: true });
    fs.writeFileSync(path.join(bare, "index.tsx"), "export default function H() {}");
    try {
      const routes = discoverRoutes({ routesDir: bare });
      const router = createRouter();
      for (const r of routes) router.insert(r);
      expect(router.matchNotFound("/anything")).toBeNull();
    } finally {
      fs.rmSync(bare, { recursive: true, force: true });
    }
  });
});
