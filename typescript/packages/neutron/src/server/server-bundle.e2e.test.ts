import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

/**
 * A dist that carries a server bundle (dist/.neutron-server-bundle.json, as
 * `neutron-ts build` writes it) is served from that bundle: route modules,
 * layouts, not-found, resource routes and global middleware all load
 * natively and no Vite server is constructed (F-P2-1). The Vite runtime is
 * only the fallback for a dist without a current bundle.
 */

const viteCreateServer = vi.hoisted(() => ({ calls: 0 }));
vi.mock("vite", async (importOriginal) => {
  const real = await importOriginal<typeof import("vite")>();
  return {
    ...real,
    createServer: (...args: Parameters<typeof real.createServer>) => {
      viteCreateServer.calls++;
      return real.createServer(...args);
    },
  };
});

const { createServer, generateServerBundleEntry, SERVER_BUNDLE_MANIFEST } = await import("./index.js");
type NeutronServer = Awaited<ReturnType<typeof createServer>>;

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

// Plain ESM, so the hand-assembled bundle needs no compiler: the entry the
// build generates, importing these files directly, is a valid bundle.
const FILES: Record<string, string> = {
  "src/middleware.js": `
export default async function middleware(request, context, next) {
  if (new URL(request.url).pathname.startsWith("/private")) {
    return new Response("denied", { status: 401 });
  }
  const response = await next();
  response.headers.set("x-global", "ran");
  return response;
}
`,
  "src/routes/_layout.js": `
import { h } from "preact";
import { useLocation } from "@neutron-build/core";
export default function Layout({ children }) {
  return h("main", null, h("span", null, "layout-path=" + useLocation().pathname), children);
}
`,
  "src/routes/index.js": `
import { h } from "preact";
import { useState } from "preact/hooks";
export const config = { mode: "app" };
export async function loader() { return { n: 41 }; }
export default function Home({ data }) {
  const [count] = useState(data.n + 1);
  return h("p", null, "hooks-count=" + count);
}
`,
  "src/routes/not-found.js": `
import { h } from "preact";
export const config = { mode: "app" };
export default function NotFound() { return h("p", null, "custom-missing"); }
`,
  "src/routes/island.js": `
import { h } from "preact";
import { useState } from "preact/hooks";
import { ServerIsland } from "@neutron-build/core";
export const config = { mode: "app" };
function Deferred() {
  const [text] = useState("island-content");
  return h("p", null, text);
}
export default function Page() {
  return h(ServerIsland, { id: "deferred", fallback: h("p", null, "island-loading") }, h(Deferred));
}
`,
  "src/routes/api/ping.js": `
export function GET() { return Response.json({ pong: true }); }
`,
};

describe("server bundle", () => {
  let running: NeutronServer | null = null;
  let root = "";

  beforeEach(() => {
    viteCreateServer.calls = 0;
  });

  afterEach(async () => {
    if (running) {
      const closing = running;
      running = null;
      await closing.close();
    }
    if (root) {
      await fs.rm(root, { recursive: true, force: true });
      root = "";
    }
    vi.restoreAllMocks();
  });

  async function writeApp(bundled: string[]): Promise<void> {
    // Outside the package, under a space-free path: vitest's module runner
    // cannot load a percent-encoded file URL (plain Node can). The app links
    // core and its deps the way an installed app sees them.
    root = await fs.mkdtemp(path.join(os.tmpdir(), "neutron-server-bundle-"));
    const ownModules = path.join(process.cwd(), "node_modules");
    await fs.mkdir(path.join(root, "node_modules", "@neutron-build"), { recursive: true });
    for (const entry of await fs.readdir(ownModules)) {
      if (entry.startsWith(".") || entry === "@neutron-build") continue;
      await fs.symlink(path.join(ownModules, entry), path.join(root, "node_modules", entry), "dir");
    }
    await fs.symlink(process.cwd(), path.join(root, "node_modules", "@neutron-build", "core"), "dir");
    for (const [name, source] of Object.entries(FILES)) {
      await fs.mkdir(path.dirname(path.join(root, name)), { recursive: true });
      await fs.writeFile(path.join(root, name), source, "utf-8");
    }
    const entryPath = path.join(root, "dist", "server", "start", "entry.js");
    await fs.mkdir(path.dirname(entryPath), { recursive: true });
    await fs.writeFile(
      entryPath,
      generateServerBundleEntry({
        entryPath,
        rootDir: root,
        files: bundled.map((name) => path.join(root, name)),
        runtime: "preact",
      }),
      "utf-8"
    );
    await fs.writeFile(
      path.join(root, "dist", SERVER_BUNDLE_MANIFEST),
      JSON.stringify({ format: 1, entry: "server/start/entry.js", runtime: "preact" }),
      "utf-8"
    );
  }

  async function boot(): Promise<string> {
    const port = await getFreePort();
    running = await createServer({ host: "127.0.0.1", port, rootDir: root, compress: false });
    return `http://127.0.0.1:${port}`;
  }

  it("serves every route kind from the bundle without constructing Vite", { timeout: 30_000 }, async () => {
    await writeApp(Object.keys(FILES));
    const base = await boot();

    expect(running!.moduleSource).toBe("server-bundle");
    expect(viteCreateServer.calls).toBe(0);

    const home = await fetch(base);
    expect(home.status).toBe(200);
    expect(home.headers.get("x-global")).toBe("ran");
    const html = await home.text();
    expect(html).toContain("layout-path=/<");
    expect(html).toContain("hooks-count=42");

    const ping = await fetch(`${base}/api/ping`);
    expect(await ping.json()).toEqual({ pong: true });
    expect(ping.headers.get("x-global")).toBe("ran");

    const missing = await fetch(`${base}/nope`, { headers: { accept: "text/html" } });
    expect(missing.status).toBe(404);
    expect(await missing.text()).toContain("custom-missing");

    expect((await fetch(`${base}/private/x`)).status).toBe(401);
    expect(viteCreateServer.calls).toBe(0);
  });

  it("answers server islands from the routes' module graph", { timeout: 30_000 }, async () => {
    await writeApp(Object.keys(FILES));
    const base = await boot();

    const page = await (await fetch(`${base}/island`)).text();
    expect(page).toContain("island-loading");
    const endpoint = page.match(/\/__neutron_island\/deferred\?t=[0-9a-f]+/)?.[0];
    expect(endpoint).toBeTruthy();
    // <ServerIsland> came from the bundle's core, so its registry (and the
    // Preact its children's hooks run on) is that instance's, not the
    // server's own.
    const island = await fetch(`${base}${endpoint}`);
    expect(island.status).toBe(200);
    expect(await island.text()).toBe("<p>island-content</p>");
  });

  it("falls back to the Vite runtime, with a warning, when the bundle lacks a route", { timeout: 60_000 }, async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    await writeApp(Object.keys(FILES).filter((name) => name !== "src/routes/api/ping.js"));
    const base = await boot();

    expect(running!.moduleSource).toBe("vite");
    expect(viteCreateServer.calls).toBe(1);
    expect(warn.mock.calls.flat().join(" ")).toMatch(
      /Not using the server bundle \(it does not contain src\/routes\/api\/ping\.js\)/
    );
    expect(await (await fetch(`${base}/api/ping`)).json()).toEqual({ pong: true });
  });

  it("transforms from source when serverBundle is false", { timeout: 60_000 }, async () => {
    await writeApp(Object.keys(FILES));
    const port = await getFreePort();
    running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
      serverBundle: false,
    });

    expect(running.moduleSource).toBe("vite");
    expect(viteCreateServer.calls).toBe(1);
    expect(await (await fetch(`http://127.0.0.1:${port}/api/ping`)).json()).toEqual({ pong: true });
  });

  it("falls back when the bundle was built for another runtime", { timeout: 60_000 }, async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    await writeApp(Object.keys(FILES));
    await fs.writeFile(
      path.join(root, "dist", SERVER_BUNDLE_MANIFEST),
      JSON.stringify({ format: 1, entry: "server/start/entry.js", runtime: "react-compat" }),
      "utf-8"
    );
    const base = await boot();

    expect(running!.moduleSource).toBe("vite");
    expect(warn.mock.calls.flat().join(" ")).toMatch(/built for runtime "react-compat"/);
    expect((await fetch(`${base}/api/ping`)).status).toBe(200);
  });

  it("refuses to start when a current bundle fails to import", { timeout: 30_000 }, async () => {
    await writeApp(Object.keys(FILES));
    await fs.writeFile(
      path.join(root, "dist", "server", "start", "entry.js"),
      'import "./missing-chunk.js";\n',
      "utf-8"
    );
    await expect(boot()).rejects.toThrow(/Failed to load the server bundle dist\/server\/start\/entry\.js/);
    expect(viteCreateServer.calls).toBe(0);
  });
});
