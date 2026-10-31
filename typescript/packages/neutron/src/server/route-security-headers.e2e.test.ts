import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as net from "node:net";
import * as http from "node:http";
import { afterAll, describe, expect, it } from "vitest";
import { createServer as createViteServer } from "vite";
import { Hono } from "hono";
import { createServer } from "./index.js";
import { neutronPlugin } from "../vite/plugin.js";
import { resolvePreactSsr, vitePreactAliases } from "../core/preact-ssr.js";

/**
 * F-P5-1: security headers scoped per route. Fylun needs COOP/COEP on
 * `/builder/**` only (WebContainer's cross-origin isolation) while every
 * other page must not get COEP. `routes.headers` rules are the per-route
 * mechanism: a rule's value overrides the `securityHeaders` default for the
 * paths it matches, `null` suppresses a default there, and `/x/:path*`
 * covers `/x` itself (Next's zero-or-more). It holds on app, static and
 * resource routes, on a mount, and in dev.
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

const FIXTURE: Record<string, string> = {
  "src/routes/builder/index.tsx": `
import { h } from "preact";
export const config = { mode: "app" };
export async function loader() { return { ok: true }; }
export default function Page() { return h("p", null, "builder"); }
`,
  "src/routes/builder/[id].tsx": `
import { h } from "preact";
export const config = { mode: "app" };
export async function loader() { return { ok: true }; }
export default function Page() { return h("p", null, "workspace"); }
`,
  "src/routes/builder/docs.tsx": `
import { h } from "preact";
export const config = { mode: "static" };
export default function Page() { return h("p", null, "docs"); }
`,
  "src/routes/builder/status.ts": `
export function GET() { return Response.json({ ok: true }); }
`,
  "src/routes/chat.tsx": `
import { h } from "preact";
export const config = { mode: "app" };
export async function loader() { return { ok: true }; }
export default function Page() { return h("p", null, "chat"); }
`,
  "src/routes/embed/[id].tsx": `
import { h } from "preact";
export const config = { mode: "app" };
export async function loader() { return { ok: true }; }
export default function Page() { return h("p", null, "embed"); }
`,
};

const ISOLATION = {
  "Cross-Origin-Opener-Policy": "same-origin",
  "Cross-Origin-Embedder-Policy": "credentialless",
};

const RULES = {
  headers: [
    { source: "/builder/:path*", headers: ISOLATION },
    { source: "/embed/:path*", headers: { "X-Frame-Options": null } },
  ],
};

async function writeFixture(prefix: string): Promise<string> {
  // Inside the package dir so the Vite SSR runtime can resolve node_modules.
  const root = await fs.mkdtemp(path.join(process.cwd(), prefix));
  roots.push(root);
  for (const [file, body] of Object.entries(FIXTURE)) {
    await fs.mkdir(path.dirname(path.join(root, file)), { recursive: true });
    await fs.writeFile(path.join(root, file), body, "utf-8");
  }
  return root;
}

async function bootProd(root: string): Promise<string> {
  // Production serves a static route from its prerendered HTML.
  await fs.mkdir(path.join(root, "dist", "builder", "docs"), { recursive: true });
  await fs.writeFile(
    path.join(root, "dist", "builder", "docs", "index.html"),
    "<!doctype html><html><body><p>docs</p></body></html>",
    "utf-8"
  );
  const port = await getFreePort();
  const api = new Hono();
  api.get("/ping", (c) => c.json({ pong: true }));
  const running = await createServer({
    host: "127.0.0.1",
    port,
    rootDir: root,
    compress: false,
    serverBundle: false,
    routes: RULES,
    mount: { "/builder/rpc": api },
  });
  closers.push(running.close);
  return `http://127.0.0.1:${port}`;
}

async function bootDev(root: string): Promise<string> {
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
        routeRules: RULES,
      }),
    ],
    resolve: { alias: vitePreactAliases(preactSsr) },
    ssr: { noExternal: preactSsr.noExternal },
    server: { middlewareMode: true },
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

async function headersOf(base: string, pathname: string): Promise<Headers> {
  const response = await fetch(`${base}${pathname}`, { headers: { Accept: "text/html" } });
  expect(response.status, pathname).toBe(200);
  await response.arrayBuffer();
  return response.headers;
}

async function expectScoped(base: string, isolatedPaths: string[]): Promise<void> {
  for (const pathname of isolatedPaths) {
    const headers = await headersOf(base, pathname);
    expect(headers.get("cross-origin-embedder-policy"), pathname).toBe("credentialless");
    expect(headers.get("cross-origin-opener-policy"), pathname).toBe("same-origin");
    // The other defaults still apply under the override.
    expect(headers.get("x-frame-options"), pathname).toBe("DENY");
  }

  const chat = await headersOf(base, "/chat");
  expect(chat.get("cross-origin-embedder-policy")).toBeNull();
  expect(chat.get("cross-origin-opener-policy")).toBe("same-origin-allow-popups");

  const embed = await headersOf(base, "/embed/card");
  expect(embed.get("x-frame-options")).toBeNull();
  expect(embed.get("x-content-type-options")).toBe("nosniff");
}

describe("security headers scoped per route through routes.headers", () => {
  it("production: app, static and resource routes and a mount", { timeout: 30_000 }, async () => {
    const base = await bootProd(await writeFixture(".tmp-neutron-route-security-"));
    await expectScoped(base, ["/builder", "/builder/p1", "/builder/docs", "/builder/status"]);
    // A mount under the rule's path gets it too.
    const mount = await headersOf(base, "/builder/rpc/ping");
    expect(mount.get("cross-origin-embedder-policy")).toBe("credentialless");
    expect(mount.get("cross-origin-opener-policy")).toBe("same-origin");
  });

  it("dev: the same headers as production", { timeout: 30_000 }, async () => {
    const base = await bootDev(await writeFixture(".tmp-neutron-dev-route-security-"));
    await expectScoped(base, ["/builder", "/builder/p1", "/builder/docs", "/builder/status"]);
  });
});
