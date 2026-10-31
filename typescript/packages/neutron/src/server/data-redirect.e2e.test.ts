import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as net from "node:net";
import * as http from "node:http";
import { afterAll, describe, expect, it } from "vitest";
import { createServer as createViteServer } from "vite";
import { createServer } from "./index.js";
import { neutronPlugin } from "../vite/plugin.js";
import { resolvePreactSsr, vitePreactAliases } from "../core/preact-ssr.js";

/**
 * F-P4-2: a redirect answering a client navigation's data request
 * (X-Neutron-Data) was sent as a plain 3xx, fetch followed it, and the router
 * rendered the target's data under the URL that redirected (the query on the
 * target was lost). It is now reported as 204 + X-Neutron-Redirect, from
 * every layer that can redirect: a loader, global middleware, a config rule.
 * Document requests are untouched. Production (createServer) and the dev
 * pipeline (the Vite plugin) answer identically.
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

const DATA_REDIRECT_FIXTURE: Record<string, string> = {
  "src/routes/open/[id].tsx": `
import { h } from "preact";
export const config = { mode: "app" };
export async function loader({ params }) {
  return new Response(null, {
    status: 307,
    headers: { location: "/workspace?open=" + params.id, "set-cookie": "seen=1; Path=/" },
  });
}
export default function Page() { return h("p", null, "never"); }
`,
  "src/routes/workspace.tsx": `
import { h } from "preact";
export const config = { mode: "app" };
export async function loader({ request }) {
  return { open: new URL(request.url).searchParams.get("open") };
}
export default function Page({ data }) { return h("p", null, "workspace " + data.open); }
`,
  "src/routes/mail.tsx": `
import { h } from "preact";
export const config = { mode: "app" };
export async function loader() { return { inbox: true }; }
export default function Page() { return h("p", null, "mail"); }
`,
  "src/middleware.ts": `
export default async function gate(request, context, next) {
  if (new URL(request.url).pathname === "/mail") {
    return new Response(null, { status: 302, headers: { location: "https://auth.example/login" } });
  }
  return next();
}
`,
};

const DATA_REDIRECT_RULES = {
  redirects: [{ source: "/old", destination: "/workspace?open=rule" }],
};

async function writeDataRedirectFixture(prefix: string): Promise<string> {
  // Inside the package dir so the Vite SSR runtime can resolve node_modules.
  const root = await fs.mkdtemp(path.join(process.cwd(), prefix));
  roots.push(root);
  for (const [file, body] of Object.entries(DATA_REDIRECT_FIXTURE)) {
    await fs.mkdir(path.dirname(path.join(root, file)), { recursive: true });
    await fs.writeFile(path.join(root, file), body, "utf-8");
  }
  return root;
}

const DATA = { "X-Neutron-Data": "true", Accept: "application/json" };

async function boot(root: string): Promise<string> {
  const port = await getFreePort();
  const running = await createServer({
    host: "127.0.0.1",
    port,
    rootDir: root,
    compress: false,
    serverBundle: false,
    routes: DATA_REDIRECT_RULES,
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
        routeRules: DATA_REDIRECT_RULES,
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

async function expectReported(base: string): Promise<void> {
  const loader = await fetch(`${base}/open/note-x`, { headers: DATA, redirect: "manual" });
  expect(loader.status).toBe(204);
  expect(loader.headers.get("x-neutron-redirect")).toBe("/workspace?open=note-x");
  expect(loader.headers.get("location")).toBeNull();
  expect(loader.headers.get("set-cookie")).toContain("seen=1");
  expect(loader.headers.get("cache-control")).toBe("no-store");

  const gate = await fetch(`${base}/mail`, { headers: DATA, redirect: "manual" });
  expect(gate.status).toBe(204);
  expect(gate.headers.get("x-neutron-redirect")).toBe("https://auth.example/login");

  const rule = await fetch(`${base}/old`, { headers: DATA, redirect: "manual" });
  expect(rule.status).toBe(204);
  expect(rule.headers.get("x-neutron-redirect")).toBe("/workspace?open=rule");

  const document = await fetch(`${base}/open/note-x`, { redirect: "manual" });
  expect(document.status).toBe(307);
  expect(document.headers.get("location")).toBe("/workspace?open=note-x");
  expect(document.headers.get("x-neutron-redirect")).toBeNull();
}

describe("createServer: redirects answering data requests are reported, not followed", () => {
  it("covers loader, global-middleware and config-rule redirects; documents keep the 3xx", { timeout: 30_000 }, async () => {
    await expectReported(await boot(await writeDataRedirectFixture(".tmp-neutron-data-redirect-")));
  });
});

describe("dev server: redirects answering data requests are reported, not followed", () => {
  it("answers exactly as production does", { timeout: 30_000 }, async () => {
    await expectReported(await bootDev(await writeDataRedirectFixture(".tmp-neutron-dev-data-redirect-")));
  });
});
