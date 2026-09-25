import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as path from "node:path";
import { afterAll, describe, expect, it } from "vitest";
import { createServer, type NeutronServer } from "./index.js";

/**
 * A production server that needs the SSR runtime (app routes) used to start
 * Vite with an HMR WebSocket server on a second, random port bound to all
 * interfaces. Production must listen on the configured port only.
 */

const APP_ROUTE = `
import { h } from "preact";
export const config = { mode: "app" };
export async function loader() {
  return { ok: true };
}
export default function Page({ data }) {
  return h("div", null, "ok=" + String(data.ok));
}
`;

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

let root = "";
let running: NeutronServer | null = null;

afterAll(async () => {
  await running?.close();
  if (root) await fs.rm(root, { recursive: true, force: true });
});

describe("production server listeners", () => {
  it("opens exactly one listener (the configured port) with the SSR runtime up", async () => {
    root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-single-listener-"));
    await fs.mkdir(path.join(root, "src", "routes"), { recursive: true });
    await fs.writeFile(path.join(root, "src", "routes", "page.tsx"), APP_ROUTE, "utf-8");
    const port = await getFreePort();

    const listened: unknown[] = [];
    const originalListen = net.Server.prototype.listen;
    net.Server.prototype.listen = function (this: net.Server, ...args: unknown[]) {
      listened.push(args[0]);
      return (originalListen as (...a: unknown[]) => net.Server).apply(this, args);
    } as typeof net.Server.prototype.listen;
    try {
      running = await createServer({
        host: "127.0.0.1",
        port,
        rootDir: root,
        distDir: "dist",
        routesDir: "src/routes",
        compress: false,
      });
      // Force the SSR runtime to load a route module, in case Vite defers
      // any listener until first use.
      const res = await fetch(`http://127.0.0.1:${port}/page`);
      expect(res.status).toBe(200);
      expect(await res.text()).toContain("ok=true");
    } finally {
      net.Server.prototype.listen = originalListen;
    }

    expect(listened).toHaveLength(1);
    const first = listened[0];
    const listenedPort =
      typeof first === "object" && first !== null ? (first as { port?: number }).port : first;
    expect(listenedPort).toBe(port);
  }, 30_000);
});
