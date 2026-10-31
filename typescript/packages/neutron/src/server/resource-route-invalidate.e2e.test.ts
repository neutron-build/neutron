import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { decodeSerializedPayload } from "../core/serialization.js";
import { createServer, type NeutronServer } from "./index.js";

/**
 * A mutating resource-route handler can name other paths to evict with
 * `x-neutron-invalidate`, exactly as a page action can: both the app-response
 * cache and the loader-data cache drop the named page.
 */

let fixtureRoot = "";
let server: NeutronServer | null = null;
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
      const { port } = address;
      socket.close((error) => (error ? reject(error) : resolve(port)));
    });
    socket.on("error", reject);
  });
}

const PAGE = `
import { h } from "preact";
export const config = { mode: "app", cache: { maxAge: 120, loaderMaxAge: 120 } };
let loadCount = 0;
export async function loader() {
  loadCount += 1;
  return { loadCount };
}
export default function Page({ data }) {
  return h("div", null, "load=" + data.loadCount);
}
`;

const ENDPOINT = `
export async function POST() {
  return Response.json({ ok: true }, { headers: { "x-neutron-invalidate": "/feed" } });
}
export async function PUT() {
  return Response.json({ ok: true });
}
`;

async function htmlLoadCount(): Promise<string> {
  const res = await fetch(`${baseUrl}/feed`);
  expect(res.status).toBe(200);
  return (await res.text()).match(/load=(\d+)/)?.[1] ?? "none";
}

async function dataLoadCount(): Promise<number> {
  const res = await fetch(`${baseUrl}/feed`, { headers: { Accept: "application/json" } });
  expect(res.status).toBe(200);
  const payload = decodeSerializedPayload<Record<string, unknown>>(await res.json());
  return (Object.values(payload)[0] as { loadCount: number }).loadCount;
}

beforeAll(async () => {
  fixtureRoot = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-resource-invalidate-"));
  await fs.mkdir(path.join(fixtureRoot, "src", "routes", "api"), { recursive: true });
  await fs.writeFile(path.join(fixtureRoot, "src", "routes", "feed.ts"), PAGE, "utf-8");
  await fs.writeFile(path.join(fixtureRoot, "src", "routes", "api", "publish.ts"), ENDPOINT, "utf-8");
  const port = await getFreePort();
  server = await createServer({ host: "127.0.0.1", port, rootDir: fixtureRoot, compress: false });
  baseUrl = `http://127.0.0.1:${port}`;
});

afterAll(async () => {
  if (server) {
    await server.close().catch(() => {});
    server = null;
  }
  if (fixtureRoot) {
    await fs.rm(fixtureRoot, { recursive: true, force: true });
  }
});

describe("resource-route mutations honour x-neutron-invalidate", () => {
  it("evicts the named page from the response and loader-data caches", { timeout: 30_000 }, async () => {
    const html = await htmlLoadCount();
    expect(await htmlLoadCount()).toBe(html);
    const data = await dataLoadCount();
    expect(await dataLoadCount()).toBe(data);

    // A mutation that names nothing leaves the page cached.
    expect((await fetch(`${baseUrl}/api/publish`, { method: "PUT" })).status).toBe(200);
    expect(await htmlLoadCount()).toBe(html);
    expect(await dataLoadCount()).toBe(data);

    // One that names /feed evicts it from both caches.
    expect((await fetch(`${baseUrl}/api/publish`, { method: "POST" })).status).toBe(200);
    const htmlAfter = await htmlLoadCount();
    expect(htmlAfter).not.toBe(html);
    expect(await dataLoadCount()).toBeGreaterThan(data);
  });
});
