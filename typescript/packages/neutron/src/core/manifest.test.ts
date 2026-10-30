import { describe, it, expect, beforeAll } from "vitest";
import * as fs from "node:fs";
import * as path from "node:path";
import { discoverRoutes, parseRouteConfig, parseRouteFacts } from "../core/manifest.js";
import { isResourceRoute } from "./resource-routes.js";

const TEST_ROUTES_DIR = path.join(__dirname, "__test_routes__");

function setupTestRoutes() {
  if (!fs.existsSync(TEST_ROUTES_DIR)) {
    fs.mkdirSync(TEST_ROUTES_DIR, { recursive: true });
  }
  if (!fs.existsSync(path.join(TEST_ROUTES_DIR, "app"))) {
    fs.mkdirSync(path.join(TEST_ROUTES_DIR, "app"), { recursive: true });
  }
  if (!fs.existsSync(path.join(TEST_ROUTES_DIR, "(marketing)"))) {
    fs.mkdirSync(path.join(TEST_ROUTES_DIR, "(marketing)"), { recursive: true });
  }

  fs.writeFileSync(
    path.join(TEST_ROUTES_DIR, "index.tsx"),
    "export default function Home() {}"
  );
  fs.writeFileSync(
    path.join(TEST_ROUTES_DIR, "about.tsx"),
    "export default function About() {}"
  );
  fs.writeFileSync(
    path.join(TEST_ROUTES_DIR, "_layout.ts"),
    "export default function Layout() {}"
  );
  fs.writeFileSync(
    path.join(TEST_ROUTES_DIR, "app", "dashboard.tsx"),
    'export const config = { mode: "app" };\nexport default function Dashboard() {}'
  );
  fs.writeFileSync(
    path.join(TEST_ROUTES_DIR, "[slug].tsx"),
    "export default function Slug() {}"
  );
  fs.writeFileSync(
    path.join(TEST_ROUTES_DIR, "(marketing)", "pricing.tsx"),
    "export default function Pricing() {}"
  );
  fs.writeFileSync(
    path.join(TEST_ROUTES_DIR, "sitemap[.]xml.ts"),
    "export async function loader() { return new Response(''); }"
  );
  // A param as a mid-path DIRECTORY, not just a leaf filename.
  fs.mkdirSync(path.join(TEST_ROUTES_DIR, "api", "runs", "[id]"), {
    recursive: true,
  });
  fs.writeFileSync(
    path.join(TEST_ROUTES_DIR, "api", "runs", "[id]", "decide.tsx"),
    "export default function Decide() {}"
  );
  fs.writeFileSync(
    path.join(TEST_ROUTES_DIR, "api", "runs", "[id]", "index.tsx"),
    "export default function Run() {}"
  );
  fs.mkdirSync(path.join(TEST_ROUTES_DIR, "docs"), { recursive: true });
  fs.writeFileSync(
    path.join(TEST_ROUTES_DIR, "docs", "[...slug].tsx"),
    "export default function DocsSlug() {}"
  );
  // Catch-all with a literal-dot suffix: /docs/<slug>.md resource route.
  fs.writeFileSync(
    path.join(TEST_ROUTES_DIR, "docs", "[...slug][.]md.ts"),
    "export async function getStaticPaths() { return []; }\nexport async function loader() { return new Response(''); }"
  );
}

const SCRATCH_ROUTES_DIR = path.join(__dirname, "__scratch_routes__");

/** A throwaway routes dir, for tables that are meant to be rejected. */
function makeScratchRoutes(name: string): string {
  const dir = path.join(SCRATCH_ROUTES_DIR, name);
  fs.rmSync(dir, { recursive: true, force: true });
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

function cleanupTestRoutes() {
  fs.rmSync(SCRATCH_ROUTES_DIR, { recursive: true, force: true });
  if (fs.existsSync(TEST_ROUTES_DIR)) {
    fs.rmSync(TEST_ROUTES_DIR, { recursive: true, force: true });
  }
}

describe("manifest", () => {
  beforeAll(() => {
    cleanupTestRoutes();
    setupTestRoutes();
  });

  afterAll(() => {
    cleanupTestRoutes();
  });

  it("discovers all route files", () => {
    const routes = discoverRoutes({ routesDir: TEST_ROUTES_DIR });
    const paths = routes.map((r) => r.path).sort();

    expect(paths).toContain("/");
    expect(paths).toContain("/about");
    expect(paths).toContain("/app/dashboard");
    expect(paths).toContain("/:slug");
    expect(paths).toContain("/pricing");
  });

  it("detects layouts and sets parent ids", () => {
    const routes = discoverRoutes({ routesDir: TEST_ROUTES_DIR });
    const layout = routes.find((r) => r.file.endsWith("_layout.ts"));
    const dashboard = routes.find((r) => r.path === "/app/dashboard");

    expect(layout).toBeDefined();
    expect(dashboard?.parentId).toBe(layout?.id);
  });

  it("resolves a [.] escape to a literal dot instead of splitting the route", () => {
    const routes = discoverRoutes({ routesDir: TEST_ROUTES_DIR });
    const paths = routes.map((r) => r.path);

    expect(paths).toContain("/sitemap.xml");
    expect(paths).not.toContain("/sitemap/xml");
  });

  it("still splits catch-all dots normally alongside the [.] escape", () => {
    const routes = discoverRoutes({ routesDir: TEST_ROUTES_DIR });
    const catchAll = routes.find((r) => r.path === "/docs/*slug");

    expect(catchAll).toBeDefined();
  });

  it("supports a catch-all with a literal-dot suffix (/docs/<slug>.md)", () => {
    const routes = discoverRoutes({ routesDir: TEST_ROUTES_DIR });
    const mdRoute = routes.find((r) => r.path === "/docs/*slug.md");

    expect(mdRoute).toBeDefined();
    expect(mdRoute?.params).toContain("slug");
  });

  it("treats a [param] directory as dynamic, not as a literal segment", () => {
    const routes = discoverRoutes({ routesDir: TEST_ROUTES_DIR });
    const paths = routes.map((r) => r.path);

    expect(paths).toContain("/api/runs/:id/decide");
    expect(paths).toContain("/api/runs/:id");
    expect(paths).not.toContain("/api/runs/[id]/decide");
  });

  it("extracts params from a mid-path [param] directory", () => {
    const routes = discoverRoutes({ routesDir: TEST_ROUTES_DIR });
    const decide = routes.find((r) => r.path === "/api/runs/:id/decide");

    expect(decide?.params).toEqual(["id"]);
  });

  it("rejects a malformed dynamic segment instead of emitting it literally", () => {
    const dir = makeScratchRoutes("malformed");
    fs.mkdirSync(path.join(dir, "[id"), { recursive: true });
    fs.writeFileSync(
      path.join(dir, "[id", "show.tsx"),
      "export default function Show() {}"
    );

    expect(() => discoverRoutes({ routesDir: dir })).toThrow(
      /malformed dynamic segment/
    );
  });

  it("rejects a catch-all directory, which nothing below it could ever match", () => {
    const dir = makeScratchRoutes("catchall-dir");
    fs.mkdirSync(path.join(dir, "docs", "[...slug]"), { recursive: true });
    fs.writeFileSync(
      path.join(dir, "docs", "[...slug]", "raw.tsx"),
      "export default function Raw() {}"
    );

    expect(() => discoverRoutes({ routesDir: dir })).toThrow(
      /catch-all segment .* is not last/
    );
  });

  it("rejects two files that resolve to the same URL shape", () => {
    const dir = makeScratchRoutes("collision");
    fs.mkdirSync(path.join(dir, "users", "[id]"), { recursive: true });
    fs.writeFileSync(
      path.join(dir, "users", "[id]", "index.tsx"),
      "export default function ById() {}"
    );
    fs.writeFileSync(
      path.join(dir, "users", "[name].tsx"),
      "export default function ByName() {}"
    );

    expect(() => discoverRoutes({ routesDir: dir })).toThrow(
      /same URL shape/
    );
  });

  it("does not mistake a literal suffix for a duplicate route", () => {
    const dir = makeScratchRoutes("suffix");
    fs.mkdirSync(path.join(dir, "docs"), { recursive: true });
    fs.writeFileSync(
      path.join(dir, "docs", "[...slug].tsx"),
      "export default function Page() {}"
    );
    fs.writeFileSync(
      path.join(dir, "docs", "[...slug][.]md.ts"),
      "export async function loader() { return new Response(''); }"
    );

    const paths = discoverRoutes({ routesDir: dir }).map((r) => r.path).sort();
    expect(paths).toEqual(["/docs/*slug", "/docs/*slug.md"]);
  });

  it("extracts route params", () => {
    const routes = discoverRoutes({ routesDir: TEST_ROUTES_DIR });
    const slugRoute = routes.find((r) => r.path === "/:slug");

    expect(slugRoute?.params).toContain("slug");
  });

  it("parses route config from file content", () => {
    const staticConfig = parseRouteConfig("export default function() {}");
    expect(staticConfig.mode).toBe("static");
    expect(staticConfig.cache).toBeUndefined();

    const appConfig = parseRouteConfig(
      'export const config = { mode: "app" };'
    );
    expect(appConfig.mode).toBe("app");

    const cachedConfig = parseRouteConfig(
      'export const config = { mode: "app", cache: { maxAge: 30 } };'
    );
    expect(cachedConfig.mode).toBe("app");
    expect(cachedConfig.cache).toEqual({ maxAge: 30 });

    const multilineCachedConfig = parseRouteConfig(`
      export const config = {
        mode: "app",
        cache: { maxAge: 60, loaderMaxAge: 15 },
      };
    `);
    expect(multilineCachedConfig.mode).toBe("app");
    expect(multilineCachedConfig.cache).toEqual({ maxAge: 60, loaderMaxAge: 15 });

    const loaderCachedConfig = parseRouteConfig(
      'export const config = { mode: "app", cache: { loaderMaxAge: 20 } };'
    );
    expect(loaderCachedConfig.mode).toBe("app");
    expect(loaderCachedConfig.cache).toEqual({ loaderMaxAge: 20 });

    const invalidCacheConfig = parseRouteConfig(
      'export const config = { mode: "app", cache: { maxAge: 0, loaderMaxAge: 0 } };'
    );
    expect(invalidCacheConfig.mode).toBe("app");
    expect(invalidCacheConfig.cache).toBeUndefined();

    const noHydrateConfig = parseRouteConfig(
      'export const config = { mode: "app", hydrate: false };'
    );
    expect(noHydrateConfig.mode).toBe("app");
    expect(noHydrateConfig.hydrate).toBe(false);
  });

  it("parseRouteConfig handles braces inside string literals", () => {
    const config = parseRouteConfig(
      'export const config = { mode: "app", description: "use { and } in strings" };'
    );
    expect(config.mode).toBe("app");
  });

  it("ignores files starting with underscore (except _layout)", () => {
    fs.writeFileSync(
      path.join(TEST_ROUTES_DIR, "_utils.ts"),
      "export const util = {};"
    );

    const routes = discoverRoutes({ routesDir: TEST_ROUTES_DIR });
    expect(routes.find((r) => r.file.includes("_utils"))).toBeUndefined();

    fs.unlinkSync(path.join(TEST_ROUTES_DIR, "_utils.ts"));
  });

  it("ignores declaration and hidden route files", () => {
    fs.writeFileSync(
      path.join(TEST_ROUTES_DIR, ".neutron-routes.d.ts"),
      "export {};"
    );
    fs.writeFileSync(
      path.join(TEST_ROUTES_DIR, ".hidden.tsx"),
      "export default function Hidden() {}"
    );

    const routes = discoverRoutes({ routesDir: TEST_ROUTES_DIR });
    expect(
      routes.find((route) => route.file.endsWith(".neutron-routes.d.ts"))
    ).toBeUndefined();
    expect(
      routes.find((route) => route.file.endsWith(".hidden.tsx"))
    ).toBeUndefined();

    fs.unlinkSync(path.join(TEST_ROUTES_DIR, ".neutron-routes.d.ts"));
    fs.unlinkSync(path.join(TEST_ROUTES_DIR, ".hidden.tsx"));
  });

  it("derives resource-route facts: method handlers + no default component", () => {
    const dir = makeScratchRoutes("resource-facts");
    fs.mkdirSync(path.join(dir, "api"), { recursive: true });
    fs.writeFileSync(
      path.join(dir, "api", "time.ts"),
      "export async function GET() { return { now: 1 }; }\nexport async function POST() { return {}; }"
    );
    fs.writeFileSync(
      path.join(dir, "page.ts"),
      "export default function Page() {}"
    );

    const routes = discoverRoutes({ routesDir: dir });
    const time = routes.find((r) => r.path === "/api/time");
    expect(time?.hasMethodHandlers).toBe(true);
    expect(time?.hasDefaultExport).toBe(false);
    expect(isResourceRoute(time!)).toBe(true);
    // Which methods the module answers, in RESOURCE_ROUTE_METHODS order —
    // the fact OpenAPI generation documents one operation from.
    expect(time?.resourceMethods).toEqual(["GET", "POST"]);

    const page = routes.find((r) => r.path === "/page");
    expect(page?.hasMethodHandlers).toBe(false);
    expect(page?.hasDefaultExport).toBe(true);
    expect(isResourceRoute(page!)).toBe(false);

    // A file with both shapes is a discovery ERROR (see the test below), but
    // the facts themselves detect it — that is what the error is derived from.
    const both = parseRouteFacts(
      "export default function Page() {}\nexport async function GET() { return {}; }"
    );
    expect(both.hasMethodHandlers).toBe(true);
    expect(both.hasDefaultExport).toBe(true);
  });

  it("resourceMethods records every handler spelling and nothing else", () => {
    const everySpelling = parseRouteFacts(
      [
        "export async function GET() {}",
        "export const POST = async () => {};",
        "let put = async () => {};",
        "export { put as PUT };",
        "export async function OPTIONS() {}",
      ].join("\n")
    );
    expect(everySpelling.hasMethodHandlers).toBe(true);
    expect(everySpelling.resourceMethods).toEqual(["GET", "POST", "PUT", "OPTIONS"]);

    // The word boundary after the name keeps look-alikes from counting.
    const lookAlikes = parseRouteFacts(
      "export const GETX = () => {};\nconst GET_DATA = 1;\nexport default function Page() {}"
    );
    expect(lookAlikes.hasMethodHandlers).toBe(false);
    expect(lookAlikes.resourceMethods).toEqual([]);
  });

  it("rejects a route that exports both a component and method handlers", () => {
    const dir = makeScratchRoutes("both-exports");
    fs.writeFileSync(
      path.join(dir, "bad.ts"),
      "export default function Page() {}\nexport async function GET() { return {}; }"
    );

    expect(() => discoverRoutes({ routesDir: dir })).toThrow(
      /exports both a default component and HTTP method handlers[\s\S]*Split the file/
    );
  });
});

import { afterAll } from "vitest";
