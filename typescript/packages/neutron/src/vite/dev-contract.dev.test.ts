import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as http from "node:http";
import * as net from "node:net";
import { afterAll, describe, expect, it, vi } from "vitest";
import { createServer as createViteServer } from "vite";
import { neutronPlugin } from "./plugin.js";
import { resolvePreactSsr, vitePreactAliases } from "../core/preact-ssr.js";
import { discoverRoutes } from "../core/manifest.js";
import { serverOpenApiSpec, type NeutronOpenApiOptions } from "../server/openapi.js";
import type { NeutronRoutesConfig } from "../config.js";

/**
 * Dev/prod contract parity for the dev pipeline:
 * - GET /health (FRAMEWORK_CONTRACT.md §7) answers with the same body and
 *   override rule as the production server (it used to 404 in dev).
 * - GET /openapi.json + /docs (§4) are served when `server.openapi` is
 *   configured, with the production server's document (they fell through to
 *   Vite's index.html in dev).
 * - Responses returned from actions and loaders keep their status and body
 *   whether built with `Response.json()` or `new Response()`.
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

function responseRoute(make: string): string {
  return `
import { h } from "preact";
export const config = { mode: "app" };
const make = ${make};
export async function loader() {
  return make({ from: "loader" }, 202);
}
export async function action() {
  return make({ from: "action" }, 201);
}
export default function Page() {
  return h("div", null, "never rendered");
}
`;
}

const USER_HEALTH_ROUTE = `
export const config = { mode: "app" };
export async function loader() {
  return Response.json({ status: "degraded", nucleus: "disconnected", version: "9.9.9" }, { status: 503 });
}
export default function Page() {
  return null;
}
`;

async function makeApp(files: Record<string, string>): Promise<string> {
  // Inside the package dir so the Vite SSR runtime can resolve node_modules.
  const root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-dev-contract-"));
  roots.push(root);
  await fs.mkdir(path.join(root, "src", "routes"), { recursive: true });
  for (const [name, source] of Object.entries(files)) {
    await fs.mkdir(path.join(root, "src", "routes", path.dirname(name)), { recursive: true });
    await fs.writeFile(path.join(root, "src", "routes", name), source, "utf-8");
  }
  return root;
}

async function boot(
  root: string,
  version?: string,
  openapi?: NeutronOpenApiOptions,
  routeRules?: NeutronRoutesConfig
): Promise<string> {
  const port = await getFreePort();
  const preactSsr = resolvePreactSsr(root);
  const vite = await createViteServer({
    configFile: false,
    root,
    logLevel: "error",
    plugins: [
      neutronPlugin({
        routesDir: path.join(root, "src", "routes"),
        rootDir: root,
        version,
        openapi,
        routeRules,
      }),
    ],
    resolve: { alias: vitePreactAliases(preactSsr) },
    ssr: { noExternal: preactSsr.noExternal },
    server: { middlewareMode: true, hmr: false, ws: false },
  });

  const httpServer = http.createServer();
  httpServer.on("request", vite.middlewares);
  closers.push(async () => {
    await vite.close();
    await new Promise<void>((resolve) => httpServer.close(() => resolve()));
  });

  await new Promise<void>((resolve, reject) => {
    httpServer.listen(port, "127.0.0.1", () => resolve());
    httpServer.on("error", reject);
  });

  return `http://127.0.0.1:${port}`;
}

describe("dev server: GET /health", () => {
  it("serves the contract body", { timeout: 30_000 }, async () => {
    const base = await boot(await makeApp({ "index.tsx": responseRoute(`(b, s) => Response.json(b, { status: s })`) }));

    const res = await fetch(`${base}/health`);
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toContain("application/json");
    expect(res.headers.get("x-request-id")).toBeTruthy();
    expect(await res.json()).toEqual({ status: "ok", nucleus: "unconfigured", version: "0.1.0" });

    const echoed = await fetch(`${base}/health`, { headers: { "x-request-id": "probe-1" } });
    expect(echoed.headers.get("x-request-id")).toBe("probe-1");

    const head = await fetch(`${base}/health`, { method: "HEAD" });
    expect(head.status).toBe(200);
    expect(await head.text()).toBe("");
  });

  it("reports the configured version", { timeout: 30_000 }, async () => {
    const base = await boot(await makeApp({ "index.tsx": responseRoute(`(b, s) => Response.json(b, { status: s })`) }), "4.5.6");
    const res = await fetch(`${base}/health`);
    expect(await res.json()).toEqual({ status: "ok", nucleus: "unconfigured", version: "4.5.6" });
  });

  it("yields to an app-defined /health route", { timeout: 30_000 }, async () => {
    const base = await boot(await makeApp({ "health.tsx": USER_HEALTH_ROUTE }));
    const res = await fetch(`${base}/health`);
    expect(res.status).toBe(503);
    expect(await res.json()).toEqual({ status: "degraded", nucleus: "disconnected", version: "9.9.9" });
  });
});

const USER_DOCS_ROUTE = `
export const config = { mode: "app" };
export async function loader() {
  return new Response("user docs", { headers: { "content-type": "text/plain" } });
}
export default function Page() {
  return null;
}
`;

describe("dev server: GET /openapi.json and /docs", () => {
  const openapi: NeutronOpenApiOptions = { title: "Example web", version: "1.0.0" };

  it("serves the production server's spec when server.openapi is configured", { timeout: 30_000 }, async () => {
    const root = await makeApp({ "item.tsx": responseRoute(`(b, s) => Response.json(b, { status: s })`) });
    const base = await boot(root, undefined, openapi);

    const res = await fetch(`${base}/openapi.json`);
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toContain("application/json");
    expect(res.headers.get("x-request-id")).toBeTruthy();
    const spec = (await res.json()) as { openapi: string; info: { title: string; version: string }; paths: object };
    expect(spec.openapi.startsWith("3.1")).toBe(true);
    expect(spec.info).toEqual({ title: "Example web", version: "1.0.0" });
    expect(Object.keys(spec.paths)).toContain("/item");
    const routes = discoverRoutes({ routesDir: path.join(root, "src", "routes") });
    expect(spec).toEqual(serverOpenApiSpec(routes, openapi, "0.1.0"));

    const docs = await fetch(`${base}/docs`);
    expect(docs.status).toBe(200);
    expect(docs.headers.get("content-type")).toBe("text/html; charset=UTF-8");
    expect(await docs.text()).toContain("<title>Example web — API Docs</title>");

    const head = await fetch(`${base}/openapi.json`, { method: "HEAD" });
    expect(head.status).toBe(200);
    expect(await head.text()).toBe("");
  });

  it("defaults info.version to the server version, as start does", { timeout: 30_000 }, async () => {
    const base = await boot(await makeApp({ "index.tsx": responseRoute(`(b, s) => Response.json(b, { status: s })`) }), "4.5.6", { title: "No version" });
    const spec = (await (await fetch(`${base}/openapi.json`)).json()) as { info: { version: string } };
    expect(spec.info.version).toBe("4.5.6");
  });

  it("is not served when server.openapi is not configured", { timeout: 30_000 }, async () => {
    const base = await boot(await makeApp({ "index.tsx": responseRoute(`(b, s) => Response.json(b, { status: s })`) }));
    const res = await fetch(`${base}/openapi.json`);
    expect(res.headers.get("content-type") ?? "").not.toContain("application/json");
  });

  it("yields to an app-defined /docs route", { timeout: 30_000 }, async () => {
    const base = await boot(await makeApp({ "docs.tsx": USER_DOCS_ROUTE }), undefined, openapi);
    const res = await fetch(`${base}/docs`);
    expect(res.status).toBe(200);
    expect(await res.text()).toBe("user docs");
  });
});

describe("dev server: Responses from loaders and actions keep status and body", () => {
  for (const [label, make] of [
    ["Response.json", `(b, s) => Response.json(b, { status: s })`],
    ["new Response", `(b, s) => new Response(JSON.stringify(b), { status: s, headers: { "content-type": "application/json" } })`],
  ] as const) {
    it(`${label}: action 201, loader 202`, { timeout: 30_000 }, async () => {
      const base = await boot(await makeApp({ "item.tsx": responseRoute(make) }));

      const posted = await fetch(`${base}/item`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "{}",
      });
      expect(posted.status).toBe(201);
      expect(await posted.json()).toEqual({ from: "action" });

      const loaded = await fetch(`${base}/item`);
      expect(loaded.status).toBe(202);
      expect(await loaded.json()).toEqual({ from: "loader" });
    });
  }
});

const RESOURCE_ROUTE = `
export async function GET({ params }) {
  return { endpoint: "dev", id: params.id ?? null };
}
export async function POST({ request }) {
  return Response.json(await request.json(), { status: 201 });
}
`;

const GATED_RESOURCE_ROUTE = `
export async function middleware(request, context, next) {
  if (!request.headers.get("x-token")) {
    return new Response(JSON.stringify({ error: "denied" }), {
      status: 403,
      headers: { "content-type": "application/json" },
    });
  }
  return next();
}
export async function GET() {
  return { classified: true };
}
`;

describe("dev server: resource routes answer like the production server", () => {
  it("dispatches method handlers with params and 405s identically", { timeout: 30_000 }, async () => {
    const base = await boot(await makeApp({
      "index.tsx": responseRoute(`(b, s) => Response.json(b, { status: s })`),
      "api/time.ts": RESOURCE_ROUTE,
    }));

    const got = await fetch(`${base}/api/time`);
    expect(got.status).toBe(200);
    expect(await got.json()).toEqual({ endpoint: "dev", id: null });

    const posted = await fetch(`${base}/api/time`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ ok: 1 }),
    });
    expect(posted.status).toBe(201);
    expect(await posted.json()).toEqual({ ok: 1 });

    // Same 405 problem+json + Allow shape as start (TS-27/A-024 parity).
    const denied = await fetch(`${base}/api/time`, { method: "DELETE" });
    expect(denied.status).toBe(405);
    expect(denied.headers.get("content-type")).toContain("application/problem+json");
    expect(denied.headers.get("allow")).toBe("GET, POST, HEAD");
  });

  it("dispatches extension-bearing resource paths instead of bypassing to Vite", { timeout: 30_000 }, async () => {
    // Both spellings that put an extension in a URL segment:
    // - `rss[.]xml.ts` — the literal-dot filename escape → /rss.xml
    // - `feed.xml/index.ts` — a directory whose name carries the extension
    //   → /feed.xml
    // The dev static-asset bypass used to hand both to Vite (404) before the
    // router could dispatch them, while production answered 200 — dev must
    // route what production routes. `sitemap-xml.ts` (no dot survives in the
    // URL) is the control that always worked.
    const root = await makeApp({
      "index.tsx": responseRoute(`(b, s) => Response.json(b, { status: s })`),
      "rss[.]xml.ts": RESOURCE_ROUTE,
      "feed.xml/index.ts": RESOURCE_ROUTE,
      "sitemap-xml.ts": RESOURCE_ROUTE,
    });
    // A real static asset: the bypass must keep working for files, so the
    // fix cannot simply route every dotted path.
    await fs.mkdir(path.join(root, "public"), { recursive: true });
    await fs.writeFile(path.join(root, "public", "styles.css"), "body{margin:0}", "utf-8");
    const base = await boot(root);

    for (const path of ["/rss.xml", "/feed.xml", "/sitemap-xml"]) {
      const res = await fetch(`${base}${path}`);
      expect(res.status, `GET ${path}`).toBe(200);
      expect(await res.json()).toEqual({ endpoint: "dev", id: null });
    }

    // POST and 405 flow through the same dispatch, not the bypass.
    const posted = await fetch(`${base}/rss.xml`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ ok: 1 }),
    });
    expect(posted.status).toBe(201);
    expect(await posted.json()).toEqual({ ok: 1 });

    const denied = await fetch(`${base}/rss.xml`, { method: "DELETE" });
    expect(denied.status).toBe(405);
    expect(denied.headers.get("allow")).toBe("GET, POST, HEAD");

    // Real assets still bypass the router to Vite's static handler.
    const css = await fetch(`${base}/styles.css`);
    expect(css.status).toBe(200);
    expect(css.headers.get("content-type")).toContain("text/css");
    expect(await css.text()).toBe("body{margin:0}");
  });

  it("answers a handler's thrown error with the same generic 500 as start", { timeout: 30_000 }, async () => {
    const base = await boot(await makeApp({
      "index.tsx": responseRoute(`(b, s) => Response.json(b, { status: s })`),
      "api/boom.ts": `
export async function GET() {
  throw new Error("connect ECONNREFUSED postgres://admin:hunter2@db");
}
`,
    }));

    const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const res = await fetch(`${base}/api/boom`);
      expect(res.status).toBe(500);
      expect(res.headers.get("content-type")).toContain("application/problem+json");
      const text = await res.text();
      expect(text).not.toContain("hunter2");
      expect(JSON.parse(text)).toMatchObject({ status: 500, instance: "/api/boom" });
      // The detail goes to the developer's terminal instead.
      expect(consoleError.mock.calls.flat().map(String).join(" ")).toContain("hunter2");
    } finally {
      consoleError.mockRestore();
    }
  });

  it("adds route-rule headers to a Response.redirect() like start does", { timeout: 30_000 }, async () => {
    const base = await boot(
      await makeApp({
        "index.tsx": responseRoute(`(b, s) => Response.json(b, { status: s })`),
        "api/go.ts": `
export async function GET() {
  return Response.redirect("http://127.0.0.1/login", 307);
}
`,
      }),
      undefined,
      undefined,
      { headers: [{ source: "/api/go", headers: { "x-route-rule": "yes" } }] }
    );

    const res = await fetch(`${base}/api/go`, { redirect: "manual" });
    expect(res.status).toBe(307);
    expect(res.headers.get("location")).toBe("http://127.0.0.1/login");
    expect(res.headers.get("x-route-rule")).toBe("yes");
  });

  it("runs route middleware around dev dispatch", { timeout: 30_000 }, async () => {
    const base = await boot(await makeApp({
      "index.tsx": responseRoute(`(b, s) => Response.json(b, { status: s })`),
      "api/secret.ts": GATED_RESOURCE_ROUTE,
    }));

    const denied = await fetch(`${base}/api/secret`);
    expect(denied.status).toBe(403);
    expect(await denied.json()).toEqual({ error: "denied" });

    const allowed = await fetch(`${base}/api/secret`, { headers: { "x-token": "t" } });
    expect(allowed.status).toBe(200);
    expect(await allowed.json()).toEqual({ classified: true });
  });
});
