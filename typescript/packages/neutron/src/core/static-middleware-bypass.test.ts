import { describe, it, expect, beforeAll, afterAll } from "vitest";
import * as fs from "node:fs";
import * as path from "node:path";

import { discoverRoutes, parseRouteFacts } from "./manifest.js";
import { renderStatic } from "./render-static.js";

/**
 * A-020. `mode: "static"` and `middleware` are contradictory, and it used to
 * resolve silently in favour of the wrong one: SSG prerendered the route and
 * the server answered from the prebuilt file, which returns before
 * renderAppRoute — the only place middleware runs. No error, no warning; the
 * page was simply public.
 */

const DIR = path.join(__dirname, "__a020_routes__");

const GATED_PAGE = `export const config = { mode: "static" };
export const middleware = async (request, context, next) => next();
export default function Secret() { return null; }
`;

const PUBLIC_PAGE = `export const config = { mode: "static" };
export default function Public() { return null; }
`;

const GATED_LAYOUT = `export const middleware = async (request, context, next) => next();
export default function Layout({ children }) { return children; }
`;

beforeAll(() => {
  fs.mkdirSync(path.join(DIR, "admin"), { recursive: true });
  fs.mkdirSync(path.join(DIR, "public"), { recursive: true });

  fs.writeFileSync(path.join(DIR, "index.tsx"), PUBLIC_PAGE);
  fs.writeFileSync(path.join(DIR, "secret.tsx"), GATED_PAGE);

  // A gate on a parent layout covers a static child: renderAppRoute collects
  // middleware from every route in [...layouts, route], so the child is just
  // as exposed as if it declared the gate itself.
  fs.writeFileSync(path.join(DIR, "admin", "_layout.tsx"), GATED_LAYOUT);
  fs.writeFileSync(path.join(DIR, "admin", "index.tsx"), PUBLIC_PAGE);

  fs.writeFileSync(path.join(DIR, "public", "index.tsx"), PUBLIC_PAGE);
});

afterAll(() => {
  fs.rmSync(DIR, { recursive: true, force: true });
});

describe("parseRouteFacts: middleware detection", () => {
  it("detects the declaration shapes the server-only stripper removes", () => {
    expect(parseRouteFacts("export const middleware = async () => {};").hasMiddleware).toBe(true);
    expect(parseRouteFacts("export async function middleware() {}").hasMiddleware).toBe(true);
    expect(parseRouteFacts("export function middleware() {}").hasMiddleware).toBe(true);
    expect(
      parseRouteFacts("const middleware = () => {};\nexport { middleware };").hasMiddleware
    ).toBe(true);
    expect(parseRouteFacts("export { requireAuth as middleware };").hasMiddleware).toBe(true);
  });

  it("does not fire on names that merely contain it", () => {
    expect(parseRouteFacts("export default function Page() { return null; }").hasMiddleware).toBe(
      false
    );
    expect(parseRouteFacts("export const middlewareOptions = {};").hasMiddleware).toBe(false);
    expect(parseRouteFacts("const middlewareStyles = {};\nexport default () => null;").hasMiddleware).toBe(
      false
    );
  });

  it("is independent of hasLoader", () => {
    const both = parseRouteFacts(
      "export const loader = async () => ({});\nexport const middleware = async () => {};"
    );
    expect(both.hasLoader).toBe(true);
    expect(both.hasMiddleware).toBe(true);
  });
});

describe("discoverRoutes carries hasMiddleware", () => {
  it("marks gated routes and leaves public ones unmarked", () => {
    const routes = discoverRoutes({ routesDir: DIR });
    const byPath = new Map(routes.map((r) => [r.file.replace(DIR, ""), r]));

    expect(byPath.get("/secret.tsx")?.hasMiddleware).toBe(true);
    expect(byPath.get("/admin/_layout.tsx")?.hasMiddleware).toBe(true);
    expect(byPath.get("/index.tsx")?.hasMiddleware).toBe(false);
    expect(byPath.get("/public/index.tsx")?.hasMiddleware).toBe(false);
  });
});

describe("renderStatic refuses to prerender a gated route", () => {
  // The regression proper. Before the fix this build succeeded and wrote
  // secret.html, which the server then served to anyone.
  it("fails the build rather than writing a public file for a gated page", async () => {
    const outDir = path.join(DIR, "..", "__a020_out__");
    fs.rmSync(outDir, { recursive: true, force: true });

    await expect(
      renderStatic({ routesDir: DIR, outputDir: outDir, appRoot: process.cwd() })
    ).rejects.toThrow(/gated by middleware/);

    expect(fs.existsSync(path.join(outDir, "secret.html"))).toBe(false);
  });

  it("names the offending file, and says so differently for a layout gate", async () => {
    const outDir = path.join(DIR, "..", "__a020_out2__");
    fs.rmSync(outDir, { recursive: true, force: true });

    let message = "";
    try {
      await renderStatic({ routesDir: DIR, outputDir: outDir, appRoot: process.cwd() });
    } catch (error) {
      message = (error as Error).message;
    }

    // Both gated routes are reported, each pointing at the file that gates it
    // — the page for a direct gate, the layout for an inherited one, because
    // "your page exports middleware" would be a lie for /admin.
    expect(message).toContain("secret.tsx exports `middleware`");
    expect(message).toContain("/admin — its layout");
    expect(message).toContain("_layout.tsx exports `middleware`");
    // The message has to be actionable, not just accurate.
    expect(message).toContain('mode: "static"');
  });
});
