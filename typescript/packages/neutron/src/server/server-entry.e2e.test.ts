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
 * F-P4-1: a custom server built its mounts and WebSocket handler before
 * createServer, outside the module graph the routes run in, so a module both
 * imported was loaded twice (two Prisma clients, two auth instances, split
 * singletons). src/server.entry.ts declares them inside that graph. A counter
 * module imported by a mount, a route loader and the WebSocket handler must
 * be one instance: the Vite runtime createServer falls back to without a
 * bundle, and the dev server (the bundle is covered by the CLI's start test).
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
  "src/lib/counter.ts": `
let n = 0;
export const next = () => ++n;
`,
  "src/server.entry.ts": `
import { Hono } from "hono";
import { next } from "./lib/counter";
const api = new Hono();
api.get("/count", (c) => c.json({ n: next() }));
export default async () => ({
  mount: { "/api": api },
  websocket: {
    path: "/ws",
    attach(wss) {
      wss.on("connection", (socket) => socket.send(String(next())));
    },
  },
});
`,
  "src/routes/page.tsx": `
import { h } from "preact";
import { next } from "../lib/counter";
export const config = { mode: "app" };
export async function loader() { return { n: next() }; }
export default function Page({ data }) { return h("p", null, "n=" + data.n); }
`,
  // Under the mount's prefix but not one of its routes: falls through.
  "src/routes/api/ping.ts": `
export function GET() { return new Response("pong"); }
`,
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

async function bootViteFallback(root: string): Promise<string> {
  const port = await getFreePort();
  const running = await createServer({
    host: "127.0.0.1",
    port,
    rootDir: root,
    compress: false,
    serverBundle: false,
  });
  closers.push(running.close);
  expect(running.moduleSource).toBe("vite");
  expect(running.wss).toBeDefined();
  return `127.0.0.1:${port}`;
}

async function bootDev(root: string): Promise<string> {
  const port = await getFreePort();
  const preactSsr = resolvePreactSsr(root);
  const vite = await createViteServer({
    configFile: false,
    root,
    logLevel: "error",
    plugins: [neutronPlugin({ routesDir: path.join(root, "src", "routes"), rootDir: root })],
    resolve: { alias: vitePreactAliases(preactSsr) },
    ssr: { noExternal: preactSsr.noExternal },
    server: { host: "127.0.0.1", port, strictPort: true, hmr: false },
  });
  await vite.listen();
  closers.push(() => vite.close());
  return `127.0.0.1:${port}`;
}

async function wsMessage(host: string): Promise<string> {
  return await new Promise<string>((resolve, reject) => {
    const socket = new WebSocket(`ws://${host}/ws`);
    const timer = setTimeout(() => reject(new Error("no websocket message")), 10_000);
    socket.onmessage = (event) => {
      clearTimeout(timer);
      socket.close();
      resolve(String(event.data));
    };
    socket.onerror = () => {
      clearTimeout(timer);
      reject(new Error("websocket failed"));
    };
  });
}

async function expectOneInstance(host: string): Promise<void> {
  const base = `http://${host}`;
  const mount1 = await (await fetch(`${base}/api/count`)).json();
  const page = await (await fetch(`${base}/page`, { headers: { Accept: "text/html" } })).text();
  const mount2 = await (await fetch(`${base}/api/count`)).json();
  // Two copies of the counter would count 1, 1, 2.
  expect(mount1).toEqual({ n: 1 });
  expect(page).toContain("n=2");
  expect(mount2).toEqual({ n: 3 });
  expect(await wsMessage(host)).toBe("4");

  const fellThrough = await fetch(`${base}/api/ping`);
  expect(fellThrough.status).toBe(200);
  expect(await fellThrough.text()).toBe("pong");
}

describe("src/server.entry.ts shares the routes' module graph", () => {
  it("createServer on the Vite runtime: mount, route and websocket see one counter", { timeout: 30_000 }, async () => {
    await expectOneInstance(await bootViteFallback(await writeFixture(".tmp-neutron-server-entry-")));
  });

  it("dev server: mount, route and websocket see one counter", { timeout: 30_000 }, async () => {
    await expectOneInstance(await bootDev(await writeFixture(".tmp-neutron-dev-server-entry-")));
  });
});
