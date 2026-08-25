import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { decodeSerializedPayload } from "../core/serialization.js";
import { createServer } from "./index.js";

/**
 * Loader-cache keys are built from the RAW request path (percent-encoded,
 * trailing slash intact) while `deleteByPath` invalidation normalizes/decodes.
 * A mutation to `/a%20b` or `/foo/` therefore failed to evict the entry stored
 * under the raw spelling — stale loader data served until TTL.
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
      const { port } = address;
      socket.close((error) => (error ? reject(error) : resolve(port)));
    });
    socket.on("error", reject);
  });
}

const PARAM_ROUTE = `
import { h } from "preact";
let loadCount = 0;
export const config = { mode: "app", cache: { loaderMaxAge: 120 } };
export async function loader() {
  loadCount += 1;
  return { loadCount };
}
export async function action() {
  return { ok: true };
}
export default function Page({ data }) {
  return h("div", null, "load=" + data.loadCount);
}
`;

async function writeFixtureApp(rootDir: string): Promise<void> {
  await fs.mkdir(path.join(rootDir, "src", "routes", "docs"), { recursive: true });
  await fs.mkdir(path.join(rootDir, "src", "routes", "notes"), { recursive: true });
  // Same route body for both: a loader whose call count is observable.
  const body = PARAM_ROUTE;
  await fs.writeFile(path.join(rootDir, "src", "routes", "docs", "[slug].ts"), body, "utf-8");
  await fs.writeFile(path.join(rootDir, "src", "routes", "notes", "index.ts"), body, "utf-8");
}

async function getLoadCount(url: string): Promise<number> {
  const res = await fetch(url, { headers: { Accept: "application/json" } });
  expect(res.status).toBe(200);
  const payload = decodeSerializedPayload<Record<string, unknown>>(await res.json());
  const first = Object.values(payload)[0] as { loadCount: number } | undefined;
  if (!first || typeof first.loadCount !== "number") {
    throw new Error(`unexpected payload for ${url}: ${JSON.stringify(payload)}`);
  }
  return first.loadCount;
}

beforeAll(async () => {
  fixtureRoot = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-loader-inv-"));
  await writeFixtureApp(fixtureRoot);

  const port = await getFreePort();
  const running = await createServer({
    host: "127.0.0.1",
    port,
    rootDir: fixtureRoot,
    distDir: "dist",
    routesDir: "src/routes",
    compress: false,
  });

  closeServer = running.close;
  baseUrl = `http://127.0.0.1:${port}`;
});

afterAll(async () => {
  if (closeServer) {
    await closeServer();
  }
  if (fixtureRoot) {
    await fs.rm(fixtureRoot, { recursive: true, force: true });
  }
});

describe("loader-cache invalidation matches how keys are written", () => {
  it("a mutation to a percent-encoded path evicts the cached loader data", { timeout: 30_000 }, async () => {
    const url = `${baseUrl}/docs/a%20b`;

    expect(await getLoadCount(url)).toBe(1);
    expect(await getLoadCount(url)).toBe(1); // served from cache

    const mutation = await fetch(url, {
      method: "POST",
      headers: { Accept: "application/json" },
    });
    expect(mutation.status).toBe(200);

    expect(await getLoadCount(url)).toBe(2); // must re-run, not serve stale
  });

  it("a mutation to the bare path evicts loader data written via a trailing-slash request", { timeout: 30_000 }, async () => {
    expect(await getLoadCount(`${baseUrl}/notes/`)).toBe(1);
    expect(await getLoadCount(`${baseUrl}/notes/`)).toBe(1); // served from cache

    const mutation = await fetch(`${baseUrl}/notes`, {
      method: "POST",
      headers: { Accept: "application/json" },
    });
    expect(mutation.status).toBe(200);

    expect(await getLoadCount(`${baseUrl}/notes/`)).toBe(2); // must re-run
  });
});
