import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as net from "node:net";
import { afterAll, describe, expect, it } from "vitest";
import { createServer } from "./index.js";

/**
 * A middleware that THROWS a Response is a documented short-circuit — and it
 * is exactly what the framework's own requireOrganization()/requirePermissions()
 * do on denial (enterprise-auth.ts). The SSR catch-all used to translate that
 * thrown Response into a bare 500 "Internal Server Error"; only loaders and
 * actions got `instanceof Response` treatment. It must answer with its own
 * status and body.
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
  const root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-thrown-resp-"));
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

describe("middleware throwing a Response answers with that Response", () => {
  it("serves the thrown 403 body and status instead of a 500", async () => {
    const base = await boot(await makeApp());

    const res = await fetch(`${base}/org-only`);
    const body = await res.text();

    expect(res.status).toBe(403);
    expect(body).toContain("Organization context required");
  });
});
