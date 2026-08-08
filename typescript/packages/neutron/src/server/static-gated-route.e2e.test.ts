import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as net from "node:net";
import { afterAll, describe, expect, it } from "vitest";
import { createServer } from "./index.js";

/**
 * A-020, request side. The build-time guard (core/static-middleware-bypass
 * .test.ts) stops a gated route being prerendered in the first place; this
 * covers the case where the file exists anyway — a dist/ built before the
 * guard, a file committed by hand, a route whose gate was added after the
 * last build. Serving it returns before renderAppRoute, the only place
 * middleware runs, so the gate never fires and the page is public.
 */

const closers: Array<() => Promise<void>> = [];
const roots: string[] = [];

afterAll(async () => {
  await Promise.all(closers.map((close) => close()));
  for (const root of roots) {
    await fs.rm(root, { recursive: true, force: true });
  }
});

async function getFreePort(): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    const socket = net.createServer();
    socket.listen(0, "127.0.0.1", () => {
      const address = socket.address();
      if (!address || typeof address === "string") {
        reject(new Error("Failed to resolve test port"));
        return;
      }
      const { port } = address;
      socket.close((error) => (error ? reject(error) : resolve(port)));
    });
    socket.on("error", reject);
  });
}

const PREBUILT = '<!doctype html><html><body><div id="app">TOP SECRET</div></body></html>';

async function makeApp(options: {
  routeSource: string;
  routeFile: string;
  distFile: string;
  globalMiddleware?: string;
}): Promise<string> {
  // Inside the package dir, not os.tmpdir(): the Vite SSR runtime resolves
  // node_modules by walking up from the app root, and a system temp dir has
  // none — the server then silently falls back to static-only behaviour, which
  // is exactly the path under test.
  const root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-a020-"));
  roots.push(root);
  await fs.mkdir(path.join(root, "src", "routes"), { recursive: true });
  await fs.mkdir(path.dirname(path.join(root, "dist", options.distFile)), { recursive: true });

  await fs.writeFile(path.join(root, "src", "routes", options.routeFile), options.routeSource, "utf-8");

  // One app route, because the SSR runtime only starts when some route is
  // `mode: "app"` (`hasAppRoutes`, server/index.ts) — and route middleware and
  // src/middleware.ts are both loaded through it. A mixed app is also the only
  // shape where this bug is reachable at request time: a pure-static app has
  // no runtime to run a gate with, which is why the build-time guard is the
  // real protection there. See A-022.
  await fs.writeFile(
    path.join(root, "src", "routes", "app-route.tsx"),
    `
import { h } from "preact";
export const config = { mode: "app" };
export default function AppRoute() {
  return h("div", null, "app");
}
`,
    "utf-8"
  );
  // The prebuilt artefact the server must not hand out unguarded.
  await fs.writeFile(path.join(root, "dist", options.distFile), PREBUILT, "utf-8");
  await fs.writeFile(
    path.join(root, "dist", "index.html"),
    '<!doctype html><html><body><div id="app">home</div></body></html>',
    "utf-8"
  );

  if (options.globalMiddleware) {
    await fs.writeFile(path.join(root, "src", "middleware.ts"), options.globalMiddleware, "utf-8");
  }

  return root;
}

async function boot(root: string): Promise<string> {
  const port = await getFreePort();
  const running = await createServer({
    host: "127.0.0.1",
    port,
    rootDir: root,
    distDir: "dist",
    routesDir: "src/routes",
    compress: false,
  });
  closers.push(running.close);
  return `http://127.0.0.1:${port}`;
}

const GATED_STATIC_ROUTE = `
import { h } from "preact";
export const config = { mode: "static" };
export const middleware = async (request, context, next) => {
  return new Response("forbidden", { status: 403 });
};
export default function Secret() {
  return h("div", null, "secret");
}
`;

const PUBLIC_STATIC_ROUTE = `
import { h } from "preact";
export const config = { mode: "static" };
export default function Public() {
  return h("div", null, "public");
}
`;

describe("A-020: a gated route is never answered from a prebuilt file", () => {
  // The regression proper. Before the fix this returned 200 and the prebuilt
  // body, with the middleware never invoked.
  it("runs the route's middleware instead of serving dist/secret.html", async () => {
    const root = await makeApp({
      routeFile: "secret.tsx",
      routeSource: GATED_STATIC_ROUTE,
      distFile: "secret.html",
    });
    const base = await boot(root);

    const res = await fetch(`${base}/secret`);
    const body = await res.text();

    expect(body).not.toContain("TOP SECRET");
    expect(res.status).toBe(403);
  });

  it("still serves an ungated static route from dist — the fast path is intact", async () => {
    const root = await makeApp({
      routeFile: "open.tsx",
      routeSource: PUBLIC_STATIC_ROUTE,
      distFile: "open.html",
    });
    const base = await boot(root);

    const res = await fetch(`${base}/open`);
    expect(res.status).toBe(200);
    expect(await res.text()).toContain("TOP SECRET");
  });
});

describe("A-020: global middleware runs before a static hit", () => {
  // globalMiddleware is never registered as app-level middleware — it is only
  // passed into renderAppRoute — so a static hit used to bypass it entirely.
  // It can be run on this path, so it is, and its response wins.
  it("honours a redirect from src/middleware.ts on a prebuilt page", async () => {
    const root = await makeApp({
      routeFile: "open.tsx",
      routeSource: PUBLIC_STATIC_ROUTE,
      distFile: "open.html",
      globalMiddleware: `
export default async function middleware(request, context, next) {
  if (new URL(request.url).pathname === "/open") {
    return new Response(null, { status: 302, headers: { Location: "/login" } });
  }
  return next();
}
`,
    });
    const base = await boot(root);

    const res = await fetch(`${base}/open`, { redirect: "manual" });
    expect(res.status).toBe(302);
    expect(res.headers.get("location")).toBe("/login");
  });

  it("serves the prebuilt page when the global middleware calls next()", async () => {
    const root = await makeApp({
      routeFile: "open.tsx",
      routeSource: PUBLIC_STATIC_ROUTE,
      distFile: "open.html",
      globalMiddleware: `
export default async function middleware(request, context, next) {
  return next();
}
`,
    });
    const base = await boot(root);

    const res = await fetch(`${base}/open`);
    expect(res.status).toBe(200);
    expect(await res.text()).toContain("TOP SECRET");
  });
});
