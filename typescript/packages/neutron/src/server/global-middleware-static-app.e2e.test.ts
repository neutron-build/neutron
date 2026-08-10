import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as net from "node:net";
import { afterAll, describe, expect, it } from "vitest";
import { createServer } from "./index.js";

/**
 * A-022. `src/middleware.ts` is loaded THROUGH the SSR runtime, and the runtime
 * used to start only when some route was `mode: "app"`. So an app whose routes
 * are all static never loaded its global middleware: not imported, never run,
 * no warning — and adding a single app route made it start working, which is a
 * confusing thing to discover.
 *
 * Found while writing the A-020 request-side tests, which failed for exactly
 * this reason until their fixtures included an app route.
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

const STATIC_ONLY_ROUTE = `
import { h } from "preact";
export const config = { mode: "static" };
export default function Page() {
  return h("div", null, "page");
}
`;

const GATE = `
export default async function middleware(request, context, next) {
  if (new URL(request.url).pathname === "/private") {
    return new Response("forbidden", { status: 403 });
  }
  return next();
}
`;

/** A pure-static app: every route is `mode: "static"`, no app route anywhere. */
async function makeStaticApp(withMiddleware: boolean): Promise<string> {
  // Inside the package dir so the Vite SSR runtime can resolve node_modules.
  const root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-a022-"));
  roots.push(root);

  await fs.mkdir(path.join(root, "src", "routes"), { recursive: true });
  await fs.mkdir(path.join(root, "dist"), { recursive: true });

  await fs.writeFile(path.join(root, "src", "routes", "private.tsx"), STATIC_ONLY_ROUTE, "utf-8");
  await fs.writeFile(path.join(root, "src", "routes", "open.tsx"), STATIC_ONLY_ROUTE, "utf-8");

  await fs.writeFile(
    path.join(root, "dist", "private.html"),
    '<!doctype html><html><body><div id="app">PRIVATE</div></body></html>',
    "utf-8"
  );
  await fs.writeFile(
    path.join(root, "dist", "open.html"),
    '<!doctype html><html><body><div id="app">OPEN</div></body></html>',
    "utf-8"
  );
  await fs.writeFile(
    path.join(root, "dist", "index.html"),
    '<!doctype html><html><body><div id="app">home</div></body></html>',
    "utf-8"
  );

  if (withMiddleware) {
    await fs.writeFile(path.join(root, "src", "middleware.ts"), GATE, "utf-8");
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

describe("A-022: global middleware in an app with no app routes", () => {
  // The regression proper. Before the fix this returned 200 and "PRIVATE":
  // no runtime was started, so src/middleware.ts was never even imported.
  it("runs src/middleware.ts even though every route is mode: \"static\"", async () => {
    const base = await boot(await makeStaticApp(true));

    const res = await fetch(`${base}/private`);
    const body = await res.text();

    expect(body).not.toContain("PRIVATE");
    expect(res.status).toBe(403);
  });

  it("still serves the pages the middleware lets through", async () => {
    const base = await boot(await makeStaticApp(true));

    const res = await fetch(`${base}/open`);
    expect(res.status).toBe(200);
    expect(await res.text()).toContain("OPEN");
  });

  // A static-only app with no middleware file must not pay for a runtime it
  // does not need — the presence of the file is what changes the decision.
  it("serves a static-only app with no middleware file unchanged", async () => {
    const base = await boot(await makeStaticApp(false));

    const priv = await fetch(`${base}/private`);
    expect(priv.status).toBe(200);
    expect(await priv.text()).toContain("PRIVATE");

    const open = await fetch(`${base}/open`);
    expect(open.status).toBe(200);
    expect(await open.text()).toContain("OPEN");
  });
});
