import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as path from "node:path";
import { Hono } from "hono";
import { afterEach, describe, expect, it } from "vitest";
import { createServer, type NeutronServer } from "./index.js";

/**
 * `Response.redirect()` (and `Response.error()`, fetched responses) carry an
 * IMMUTABLE header guard. The pipeline sets headers on the way out — request
 * id, security headers, CORS, Vary, route-rule headers — so such a response
 * used to surface as a bare 500. Every place user code can hand back a
 * Response must answer as that Response, with the pipeline's headers added.
 */

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

const REDIRECT = `Response.redirect("http://127.0.0.1/login", 307)`;

const FILES: Record<string, string> = {
  "src/middleware.ts": `
export default async function middleware(request, context, next) {
  const url = new URL(request.url);
  if (url.pathname === "/gated-global") return ${REDIRECT};
  if (url.pathname === "/unmatched-gated") return ${REDIRECT};
  return next();
}
`,
  "src/routes/gated-global.tsx": `
import { h } from "preact";
export const config = { mode: "app" };
export default function Page() { return h("p", null, "never"); }
`,
  "src/routes/gated-route.tsx": `
import { h } from "preact";
export const config = { mode: "app" };
export async function middleware() { return ${REDIRECT}; }
export default function Page() { return h("p", null, "never"); }
`,
  "src/routes/loader-returns.tsx": `
import { h } from "preact";
export const config = { mode: "app" };
export async function loader() { return ${REDIRECT}; }
export default function Page() { return h("p", null, "never"); }
`,
  "src/routes/loader-throws.tsx": `
import { h } from "preact";
export const config = { mode: "app" };
export async function loader() { throw ${REDIRECT}; }
export default function Page() { return h("p", null, "never"); }
`,
  "src/routes/action-returns.tsx": `
import { h } from "preact";
export const config = { mode: "app" };
export async function action() { return Response.redirect("http://127.0.0.1/login", 303); }
export default function Page() { return h("p", null, "form"); }
`,
  "src/routes/api/redirect.ts": `
export async function GET() { return ${REDIRECT}; }
export async function POST() { throw ${REDIRECT}; }
`,
};

describe("immutable-header responses answer as themselves", () => {
  let running: NeutronServer | null = null;
  let root = "";

  afterEach(async () => {
    if (running) {
      await running.close();
      running = null;
    }
    if (root) {
      await fs.rm(root, { recursive: true, force: true });
      root = "";
    }
  });

  async function boot(): Promise<string> {
    root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-immutable-"));
    for (const [name, source] of Object.entries(FILES)) {
      await fs.mkdir(path.join(root, path.dirname(name)), { recursive: true });
      await fs.writeFile(path.join(root, name), source, "utf-8");
    }
    const mounted = new Hono();
    mounted.get("/go", () => Response.redirect("http://127.0.0.1/login", 307));
    const port = await getFreePort();
    running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: root,
      // Default compression + security headers + request id: every
      // post-processing step that sets a header is in play.
      cors: { origin: "*" },
      routes: { headers: [{ source: "/api/redirect", headers: { "x-route-rule": "yes" } }] },
      mount: { "/ext": mounted },
    });
    return `http://127.0.0.1:${port}`;
  }

  function expectRedirect(res: Response, label: string, status = 307): void {
    expect.soft(res.status, label).toBe(status);
    expect.soft(res.headers.get("location"), label).toBe("http://127.0.0.1/login");
    // The pipeline's own headers still land on it.
    expect.soft(res.headers.get("x-request-id"), label).toBeTruthy();
    expect.soft(res.headers.get("x-content-type-options"), label).toBe("nosniff");
  }

  it("from global middleware, route middleware, loaders, actions, resource routes and mounts", { timeout: 30_000 }, async () => {
    const base = await boot();
    const get = (p: string, init?: RequestInit) =>
      fetch(`${base}${p}`, { redirect: "manual", headers: { Origin: "https://x.test" }, ...init });

    expectRedirect(await get("/gated-global"), "global middleware");
    expectRedirect(await get("/gated-route"), "route middleware");
    expectRedirect(await get("/loader-returns"), "loader return");
    expectRedirect(await get("/loader-throws"), "loader throw");
    expectRedirect(
      await get("/action-returns", { method: "POST", headers: { Origin: "https://x.test" } }),
      "action return",
      303
    );

    const resource = await get("/api/redirect");
    expectRedirect(resource, "resource route return");
    expect.soft(resource.headers.get("x-route-rule")).toBe("yes");
    expect.soft(resource.headers.get("access-control-allow-origin")).toBe("*");
    expectRedirect(
      await get("/api/redirect", { method: "POST", headers: { Origin: "https://x.test" } }),
      "resource route throw"
    );

    expectRedirect(await get("/ext/go"), "mounted sub-app");
  });
});
