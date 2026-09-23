import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as http from "node:http";
import * as net from "node:net";
import { afterAll, describe, expect, it } from "vitest";
import { createServer as createViteServer } from "vite";
import { neutronPlugin } from "./plugin.js";
import { resolvePreactSsr, vitePreactAliases } from "../core/preact-ssr.js";

/**
 * Dev/prod contract parity for the dev pipeline:
 * - GET /health (FRAMEWORK_CONTRACT.md §7) answers with the same body and
 *   override rule as the production server (it used to 404 in dev).
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
    await fs.writeFile(path.join(root, "src", "routes", name), source, "utf-8");
  }
  return root;
}

async function boot(root: string, version?: string): Promise<string> {
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
