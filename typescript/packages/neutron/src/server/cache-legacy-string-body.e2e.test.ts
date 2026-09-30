import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createServer } from "./index.js";
import type { NeutronAppCacheStore, NeutronAppResponseCacheEntry } from "./cache-store.js";

/**
 * Core <= 0.2.2 stored `body` as a string. An entry like that outlives an
 * upgrade in an external store (Redis), and must be served, not turned into
 * an empty 200.
 */

let fixtureRoot = "";
let closeServer: (() => Promise<void>) | null = null;
let baseUrl = "";

async function getFreePort(): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    const socket = net.createServer();
    socket.listen(0, "127.0.0.1", () => {
      const address = socket.address();
      if (!address || typeof address === "string") {
        reject(new Error("Failed to resolve test port"));
        return;
      }
      socket.close((error) => (error ? reject(error) : resolve(address.port)));
    });
    socket.on("error", reject);
  });
}

const ROUTE = `
import { h } from "preact";
export const config = { mode: "app", cache: { maxAge: 30 } };
export default function Page() {
  return h("div", null, "fresh render");
}
`;

const legacyStore: NeutronAppCacheStore = {
  async getGeneration() { return '0'; },
  async setIfGeneration() { return false; },
  async get() {
    const legacy = {
      status: 200,
      statusText: "OK",
      headers: [["content-type", "text/html; charset=utf-8"]],
      body: "<p>legacy café</p>",
      expiresAt: Date.now() + 60_000,
    };
    return legacy as unknown as NeutronAppResponseCacheEntry;
  },
  async set() {},
  async deleteByPath() {},
  async clear() {},
};

beforeAll(async () => {
  fixtureRoot = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-legacy-cache-"));
  await fs.mkdir(path.join(fixtureRoot, "src", "routes"), { recursive: true });
  await fs.writeFile(path.join(fixtureRoot, "src", "routes", "page.ts"), ROUTE, "utf-8");
  const port = await getFreePort();
  const running = await createServer({
    host: "127.0.0.1",
    port,
    rootDir: fixtureRoot,
    distDir: "dist",
    routesDir: "src/routes",
    compress: false,
    cache: { app: legacyStore },
  });
  closeServer = running.close;
  baseUrl = `http://127.0.0.1:${port}`;
});

afterAll(async () => {
  if (closeServer) await closeServer();
  if (fixtureRoot) await fs.rm(fixtureRoot, { recursive: true, force: true });
});

describe("a cached entry written with a string body", () => {
  it("is served with its bytes", { timeout: 20_000 }, async () => {
    const res = await fetch(`${baseUrl}/page`);
    expect(res.status).toBe(200);
    expect(await res.text()).toBe("<p>legacy café</p>");
  });
});
