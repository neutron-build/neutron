import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as http from "node:http";
import * as net from "node:net";
import { afterAll, describe, expect, it } from "vitest";
import { createServer as createViteServer } from "vite";
import { neutronPlugin } from "./plugin.js";
import { resolvePreactSsr, vitePreactAliases } from "../core/preact-ssr.js";

/**
 * Dev/prod parity for thrown Responses. Production finalizes a Response
 * thrown by middleware as-is (server/index.ts catch-all); the dev pipeline
 * must answer with the same status and body, not hand the Response to
 * Vite's error middleware as a 500.
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

const APP_ROUTE_WITH_THROWING_MIDDLEWARE = `
import { h } from "preact";
export const config = { mode: "app" };
export const middleware = async (request, context, next) => {
  // Same shape as requireOrganization()/requirePermissions(): throw, not return.
  throw new Response("Organization context required", {
    status: 403,
    statusText: "Forbidden",
  });
};
export default function Page() {
  return h("div", null, "never rendered");
}
`;

async function makeApp(): Promise<string> {
  // Inside the package dir so the Vite SSR runtime can resolve node_modules.
  const root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-dev-thrown-"));
  roots.push(root);

  await fs.mkdir(path.join(root, "src", "routes"), { recursive: true });
  await fs.writeFile(
    path.join(root, "src", "routes", "org-only.tsx"),
    APP_ROUTE_WITH_THROWING_MIDDLEWARE,
    "utf-8"
  );

  return root;
}

async function boot(root: string): Promise<string> {
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

describe("dev server: middleware throwing a Response answers with that Response", () => {
  it("serves the thrown 403 body and status instead of a 500", { timeout: 30_000 }, async () => {
    const base = await boot(await makeApp());

    const res = await fetch(`${base}/org-only`);
    const body = await res.text();

    expect(res.status).toBe(403);
    expect(body).toContain("Organization context required");
  });
});
