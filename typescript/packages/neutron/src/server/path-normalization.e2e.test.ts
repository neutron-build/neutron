import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as net from "node:net";
import { afterAll, describe, expect, it } from "vitest";
import { createServer, normalizePathname } from "./index.js";

/**
 * normalizePathname used to reject any decoded path containing ".."
 * anywhere — a substring test, not a segment test. Legal paths like
 * `/v1.2..3` or `/a..b` were answered 400 Bad Request. Traversal must be
 * judged per path segment (`/a/../b`), matching the runtime handler's own
 * guard.
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

describe("normalizePathname traversal check", () => {
  it("still rejects a whole '..' segment", () => {
    expect(normalizePathname("/a/../b")).toBeNull();
    expect(normalizePathname("/../etc/passwd")).toBeNull();
  });

  it("accepts '..' inside a segment", () => {
    expect(normalizePathname("/a..b")).toBe("/a..b");
    expect(normalizePathname("/v1.2..3")).toBe("/v1.2..3");
  });
});

describe("path normalization traversal check", () => {
  it("accepts paths where '..' is inside a segment (not a segment)", async () => {
    const root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-path-norm-"));
    roots.push(root);
    await fs.mkdir(path.join(root, "src", "routes"), { recursive: true });

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
    const base = `http://127.0.0.1:${port}`;

    // No such route: the answer must be 404, not a 400 from the path guard.
    // (Traversal rejection itself is covered by the unit tests above —
    // fetch() and Hono normalize %2e%2e / ".." segments away before the
    // guard ever sees them over HTTP.)
    const legal = await fetch(`${base}/v1.2..3`);
    expect(legal.status).toBe(404);
  });
});
