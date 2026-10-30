import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { decodeSerializedPayload } from "../core/serialization.js";
import { createServer, type NeutronServer } from "./index.js";

/**
 * A mutating resource-route request fences loader fills at COMPLETION (the
 * TS-07 round-2 rule the app-route mutation path follows): a cacheable GET
 * that STARTED while the endpoint was running must not publish its
 * (pre-mutation) loader output after the endpoint finished.
 */

let fixtureRoot = "";
let server: NeutronServer | null = null;
let baseUrl = "";
let fenceDir = "";

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

const WAIT_FOR = `
async function waitFor(file) {
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    try { await fs.stat(file); return; } catch { await new Promise((r) => setTimeout(r, 25)); }
  }
}
`;

const PAGE = `
import { h } from "preact";
import * as fs from "node:fs/promises";
export const config = { mode: "app", cache: { loaderMaxAge: 120 } };
${WAIT_FOR}
let loadCount = 0;
export async function loader() {
  loadCount += 1;
  const n = loadCount;
  if (n === 1) {
    const dir = process.env.RESOURCE_FENCE_DIR;
    await fs.writeFile(dir + "/loader-started", "1", "utf-8");
    await waitFor(dir + "/loader-gate");
  }
  return { loadCount: n };
}
export default function Page({ data }) {
  return h("div", null, "load=" + data.loadCount);
}
`;

const ENDPOINT = `
import * as fs from "node:fs/promises";
${WAIT_FOR}
export async function POST() {
  const dir = process.env.RESOURCE_FENCE_DIR;
  await fs.writeFile(dir + "/post-entered", "1", "utf-8");
  await waitFor(dir + "/post-gate");
  return { ok: true };
}
`;

async function waitForFile(file: string): Promise<void> {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    try {
      await fs.stat(file);
      return;
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
  }
  throw new Error(`timed out waiting for ${file}`);
}

async function getLoadCount(): Promise<number> {
  const res = await fetch(`${baseUrl}/docs`, { headers: { Accept: "application/json" } });
  expect(res.status).toBe(200);
  const payload = decodeSerializedPayload<Record<string, unknown>>(await res.json());
  const first = Object.values(payload)[0] as { loadCount: number } | undefined;
  if (!first || typeof first.loadCount !== "number") {
    throw new Error(`unexpected payload: ${JSON.stringify(payload)}`);
  }
  return first.loadCount;
}

beforeAll(async () => {
  fixtureRoot = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-resource-fence-"));
  await fs.mkdir(path.join(fixtureRoot, "src", "routes", "docs"), { recursive: true });
  await fs.mkdir(path.join(fixtureRoot, "src", "routes", "api"), { recursive: true });
  await fs.writeFile(path.join(fixtureRoot, "src", "routes", "docs", "index.ts"), PAGE, "utf-8");
  await fs.writeFile(path.join(fixtureRoot, "src", "routes", "api", "todos.ts"), ENDPOINT, "utf-8");
  fenceDir = path.join(fixtureRoot, "fence");
  await fs.mkdir(fenceDir, { recursive: true });
  process.env.RESOURCE_FENCE_DIR = fenceDir;

  const port = await getFreePort();
  server = await createServer({ host: "127.0.0.1", port, rootDir: fixtureRoot, compress: false });
  baseUrl = `http://127.0.0.1:${port}`;
});

afterAll(async () => {
  if (server) {
    await server.close().catch(() => {});
    server = null;
  }
  delete process.env.RESOURCE_FENCE_DIR;
  if (fixtureRoot) {
    await fs.rm(fixtureRoot, { recursive: true, force: true });
  }
});

describe("resource-route mutations fence loader fills at completion", () => {
  it(
    "a GET that started during the endpoint does not publish after it",
    { timeout: 30_000 },
    async () => {
      // The endpoint starts and holds; anything it does before running has
      // already happened when the GET begins.
      const mutation = fetch(`${baseUrl}/api/todos`, { method: "POST" });
      await waitForFile(path.join(fenceDir, "post-entered"));

      // GET 1 starts during the mutation; its loader holds too.
      const inFlight = getLoadCount();
      await waitForFile(path.join(fenceDir, "loader-started"));

      // The endpoint completes while GET 1's loader is still running.
      await fs.writeFile(path.join(fenceDir, "post-gate"), "1", "utf-8");
      expect((await mutation).status).toBe(200);

      await fs.writeFile(path.join(fenceDir, "loader-gate"), "1", "utf-8");
      expect(await inFlight).toBe(1);

      // GET 1's fill was fenced, so GET 2 runs the loader again. A published
      // fill would make this a cache hit serving the stale loadCount=1.
      expect(await getLoadCount()).toBe(2);
      expect(await getLoadCount()).toBe(2);
    }
  );
});
