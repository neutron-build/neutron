import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { createServer, type NeutronServer } from "./index.js";

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

/**
 * Fixture: one SSR app with a static page, an app-mode page, and a family of
 * resource routes under /api — the one-server shape this feature exists for.
 */
async function writeFixtureApp(rootDir: string): Promise<void> {
  await fs.mkdir(path.join(rootDir, "dist"), { recursive: true });
  await fs.mkdir(path.join(rootDir, "src", "routes", "api", "users", "[id]"), {
    recursive: true,
  });

  await fs.writeFile(
    path.join(rootDir, "dist", "about.html"),
    "<!doctype html><html><body><h1>Resource Fixture About</h1></body></html>",
    "utf-8"
  );

  await fs.writeFile(
    path.join(rootDir, "src", "routes", "index.tsx"),
    `
import { h } from "preact";
export const config = { mode: "app" };
export async function loader() {
  return { page: "home" };
}
export default function Home({ data }) {
  return h("main", null, "app page: " + data.page);
}
`,
    "utf-8"
  );

  // Global middleware: outermost. Marks the context so the order test below
  // can prove global → layout → route → handler execution.
  await fs.writeFile(
    path.join(rootDir, "src", "middleware.ts"),
    `
export default async function middleware(request, context, next) {
  context.order = ["global"];
  return next();
}
`,
    "utf-8"
  );

  // A layout over the API subtree: its middleware gates endpoints in it the
  // same way a layout gates pages (fail-closed: a gate on a parent must not
  // be bypassable by putting an endpoint under it).
  await fs.writeFile(
    path.join(rootDir, "src", "routes", "api", "_layout.ts"),
    `
export async function middleware(request, context, next) {
  context.order = context.order || [];
  context.order.push("layout");
  if (request.url.includes("/api/secret") && !request.headers.get("x-token")) {
    throw new Response(JSON.stringify({ error: "unauthorized" }), {
      status: 401,
      headers: { "Content-Type": "application/json" }
    });
  }
  return next();
}
export default function ApiLayout(props) {
  return props.children;
}
`,
    "utf-8"
  );

  await fs.writeFile(
    path.join(rootDir, "src", "routes", "api", "time.ts"),
    `
export async function GET() {
  return { now: "2026-10-30T00:00:00Z", dynamic: true };
}
`,
    "utf-8"
  );

  await fs.writeFile(
    path.join(rootDir, "src", "routes", "api", "users", "[id].ts"),
    `
export async function GET({ params }) {
  return { user: { id: params.id } };
}
export async function POST({ params, request }) {
  const body = await request.json();
  return { updated: params.id, body };
}
`,
    "utf-8"
  );

  await fs.writeFile(
    path.join(rootDir, "src", "routes", "api", "secret.ts"),
    `
export async function GET() {
  return { classified: true };
}
`,
    "utf-8"
  );

  // A loader on a resource route must never run — if it did, this endpoint
  // would answer with the throw instead of its GET handler.
  await fs.writeFile(
    path.join(rootDir, "src", "routes", "api", "watcher.ts"),
    `
export async function loader() {
  throw new Error("loaders must not run for resource routes");
}
export async function GET() {
  return { ok: true };
}
`,
    "utf-8"
  );

  await fs.writeFile(
    path.join(rootDir, "src", "routes", "api", "missing.ts"),
    `
import { notFoundError } from "@neutron-build/core";
export async function GET() {
  throw notFoundError("no such run");
}
`,
    "utf-8"
  );
}

describe("resource routes (Option A — file-convention endpoints)", () => {
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

  it("serves endpoints alongside pages in one server", async () => {
    root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-resource-"));
    await writeFixtureApp(root);
    const port = await getFreePort();
    running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
    });
    const base = `http://127.0.0.1:${port}`;

    // Resource route: JSON auto-serialization from a plain return.
    const time = await fetch(`${base}/api/time`);
    expect(time.status).toBe(200);
    expect(time.headers.get("content-type")).toBe("application/json");
    expect(await time.json()).toEqual({ now: "2026-10-30T00:00:00Z", dynamic: true });

    // Path params reach the handlers.
    const user = await fetch(`${base}/api/users/42`);
    expect(await user.json()).toEqual({ user: { id: "42" } });

    // Mutation methods with bodies work.
    const updated = await fetch(`${base}/api/users/42`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "Fylun" }),
    });
    expect(updated.status).toBe(200);
    expect(await updated.json()).toEqual({ updated: "42", body: { name: "Fylun" } });

    // The static page still serves from dist.
    const home = await fetch(`${base}/about`);
    expect(home.status).toBe(200);
    expect(await home.text()).toContain("Resource Fixture About");

    // The app-mode page still renders per request.
    const appPage = await fetch(`${base}/`, {
      headers: { "X-Neutron-Data": "true" },
    });
    expect(appPage.status).toBe(200);
    const data = JSON.parse(await appPage.text());
    expect(JSON.stringify(data)).toContain("home");
  });

  it("405 is RFC 7807 problem+json with an Allow header", async () => {
    root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-resource-"));
    await writeFixtureApp(root);
    const port = await getFreePort();
    running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
    });
    const base = `http://127.0.0.1:${port}`;

    const res = await fetch(`${base}/api/time`, { method: "DELETE" });
    expect(res.status).toBe(405);
    expect(res.headers.get("content-type")).toContain("application/problem+json");
    expect(res.headers.get("allow")).toBe("GET, HEAD");
    const body = (await res.json()) as Record<string, unknown>;
    expect(body.type).toBe("https://neutron.dev/errors/method-not-allowed");
    expect(body.status).toBe(405);
    expect(body.instance).toBe("/api/time");
  });

  it("a handler's thrown notFoundError answers the RFC 7807 404 shape", async () => {
    root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-resource-"));
    await writeFixtureApp(root);
    const port = await getFreePort();
    running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
    });

    const res = await fetch(`http://127.0.0.1:${port}/api/missing`);
    expect(res.status).toBe(404);
    expect(res.headers.get("content-type")).toContain("application/problem+json");
    const body = (await res.json()) as Record<string, unknown>;
    expect(body.type).toBe("https://neutron.dev/errors/not-found");
    expect(body.detail).toBe("no such run");
    expect(body.instance).toBe("/api/missing");
  });

  it("HEAD falls back to the GET handler (same headers, no body)", async () => {
    root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-resource-"));
    await writeFixtureApp(root);
    const port = await getFreePort();
    running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
    });

    const head = await fetch(`http://127.0.0.1:${port}/api/time`, { method: "HEAD" });
    expect(head.status).toBe(200);
    expect(head.headers.get("content-type")).toBe("application/json");
    expect(await head.text()).toBe("");
  });

  it("runs global + layout middleware around the handler, in FRAMEWORK_CONTRACT order", async () => {
    root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-resource-"));
    // Order-proving fixture: global marks the context, layout appends, the
    // route middleware appends, the handler reports what it saw.
    await fs.mkdir(path.join(root, "src", "routes", "api"), { recursive: true });
    await fs.writeFile(
      path.join(root, "src", "middleware.ts"),
      `
export default async function middleware(request, context, next) {
  context.order = ["global"];
  return next();
}
`,
      "utf-8"
    );
    await fs.writeFile(
      path.join(root, "src", "routes", "api", "_layout.ts"),
      `
export async function middleware(request, context, next) {
  context.order.push("layout");
  return next();
}
export default function Layout(p) { return p.children; }
`,
      "utf-8"
    );
    await fs.writeFile(
      path.join(root, "src", "routes", "api", "order.ts"),
      `
export async function middleware(request, context, next) {
  context.order.push("route");
  return next();
}
export async function GET({ context }) {
  context.order.push("handler");
  return { order: context.order };
}
`,
      "utf-8"
    );

    const port = await getFreePort();
    running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
    });

    const res = await fetch(`http://127.0.0.1:${port}/api/order`);
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ order: ["global", "layout", "route", "handler"] });
  });

  it("a layout middleware gate protects resource routes in its subtree (A-020 fail-closed)", async () => {
    root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-resource-"));
    await writeFixtureApp(root);
    const port = await getFreePort();
    running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
    });
    const base = `http://127.0.0.1:${port}`;

    // No token: the layout middleware's thrown Response answers.
    const denied = await fetch(`${base}/api/secret`);
    expect(denied.status).toBe(401);
    expect(await denied.json()).toEqual({ error: "unauthorized" });

    // With the token: the endpoint answers.
    const allowed = await fetch(`${base}/api/secret`, { headers: { "x-token": "abc" } });
    expect(allowed.status).toBe(200);
    expect(await allowed.json()).toEqual({ classified: true });
  });

  it("loaders never run for resource routes", async () => {
    root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-resource-"));
    await writeFixtureApp(root);
    const port = await getFreePort();
    running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
    });

    // watcher.ts exports a loader that THROWS; if any loader machinery ran for
    // this request the response would be an error, not the GET handler's body.
    const res = await fetch(`http://127.0.0.1:${port}/api/watcher`);
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ ok: true });
  });

  it("reports resource routes through the onRequestEnd hook with their route id", async () => {
    root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-resource-"));
    await writeFixtureApp(root);
    const port = await getFreePort();
    const seen: string[] = [];
    running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
      hooks: {
        onRequestEnd: (event) => {
          if (event.pathname.startsWith("/api/")) {
            seen.push(`${event.routeId}:${event.status}`);
          }
        },
      },
    });

    await fetch(`http://127.0.0.1:${port}/api/time`);
    await fetch(`http://127.0.0.1:${port}/api/users/7`);
    expect(seen).toEqual(["route:api/time.ts:200", "route:api/users/[id].ts:200"]);
  });

  it("extension-bearing resource paths dispatch as endpoints (dev parity)", async () => {
    // /rss.xml and /feed.xml come from route modules (`rss[.]xml.ts`,
    // `feed.xml/index.ts`). Production dispatches resource routes ahead of
    // any static file path; this is the parity target for the dev server's
    // static-asset bypass (see dev-contract.dev.test.ts).
    root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-resource-"));
    await fs.mkdir(path.join(root, "src", "routes", "feed.xml"), { recursive: true });
    await fs.writeFile(
      path.join(root, "src", "routes", "rss[.]xml.ts"),
      `
export async function GET() {
  return { feed: "rss" };
}
`,
      "utf-8"
    );
    await fs.writeFile(
      path.join(root, "src", "routes", "feed.xml", "index.ts"),
      `
export async function GET() {
  return { feed: "atom" };
}
`,
      "utf-8"
    );

    const port = await getFreePort();
    running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
    });
    const base = `http://127.0.0.1:${port}`;

    const rss = await fetch(`${base}/rss.xml`);
    expect(rss.status).toBe(200);
    expect(rss.headers.get("content-type")).toBe("application/json");
    expect(await rss.json()).toEqual({ feed: "rss" });

    const atom = await fetch(`${base}/feed.xml`);
    expect(atom.status).toBe(200);
    expect(await atom.json()).toEqual({ feed: "atom" });

    const denied = await fetch(`${base}/rss.xml`, { method: "DELETE" });
    expect(denied.status).toBe(405);
    expect(denied.headers.get("allow")).toBe("GET, HEAD");
  });

  it("refuses to boot when a route exports both a component and method handlers", async () => {
    root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-resource-"));
    await fs.mkdir(path.join(root, "src", "routes"), { recursive: true });
    await fs.writeFile(
      path.join(root, "src", "routes", "bad.ts"),
      `
export default function Page() {
  return null;
}
export async function GET() {
  return { nope: true };
}
`,
      "utf-8"
    );
    const port = await getFreePort();

    await expect(
      createServer({
        host: "127.0.0.1",
        port,
        rootDir: root,
        compress: false,
      })
    ).rejects.toThrow(
      /exports both a default component and HTTP method handlers[\s\S]*Split the file/
    );
  });
});
