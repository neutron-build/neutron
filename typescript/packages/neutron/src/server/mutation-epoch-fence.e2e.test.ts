import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { decodeSerializedPayload } from "../core/serialization.js";
import { createServer, type NeutronServer } from "./index.js";

/**
 * TS-07 (round 2): the invalidation epoch advances not only BEFORE a
 * mutation but again at mutation COMPLETION, and loader fills re-check it
 * before publishing. A GET that starts while a mutation is mid-flight must
 * not publish its (pre-mutation) loader output after the mutation's final
 * invalidation.
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

const ROUTE = `
import { h } from "preact";
import * as fs from "node:fs/promises";
export const config = { mode: "app", cache: { loaderMaxAge: 120 } };
let loadCount = 0;
export async function loader() {
  loadCount += 1;
  const n = loadCount;
  // The FIRST loader run blocks on a gate file the test writes only after
  // the mutation has completed, so the interleaving is deterministic under
  // scheduler load instead of relying on a fixed sleep.
  if (n === 1 && process.env.EPOCH_FENCE_DIR) {
    const dir = process.env.EPOCH_FENCE_DIR;
    await fs.mkdir(dir, { recursive: true });
    await fs.writeFile(dir + "/started", String(n), "utf-8");
    const deadline = Date.now() + 60_000;
    while (Date.now() < deadline) {
      try {
        await fs.stat(dir + "/gate");
        break;
      } catch {
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
    }
  }
  return { loadCount: n };
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
  await fs.writeFile(path.join(rootDir, "src", "routes", "docs", "index.ts"), ROUTE, "utf-8");
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
  fixtureRoot = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-epoch-"));
  await writeFixtureApp(fixtureRoot);
  process.env.EPOCH_FENCE_DIR = path.join(fixtureRoot, "fence");

  const port = await getFreePort();
  server = await createServer({
    host: "127.0.0.1",
    port,
    rootDir: fixtureRoot,
    distDir: "dist",
    routesDir: "src/routes",
    compress: false,
  });
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

describe("TS-07: completion invalidation fences in-flight loader fills", () => {
  it(
    "a GET that started before a mutation's completion does not republish stale loader data",
    { timeout: 30_000 },
    async () => {
      // GET 1 starts; its first loader run blocks on the gate file.
      const inFlight = getLoadCount();
      // Wait until GET 1's loader has observably started, then complete a
      // mutation on the same path (pre-invalidation + action + completion
      // invalidation all land while GET 1's loader is still gated).
      const fenceDir = process.env.EPOCH_FENCE_DIR!;
      const startedDeadline = Date.now() + 10_000;
      while (Date.now() < startedDeadline) {
        try {
          await fs.stat(fenceDir + "/started");
          break;
        } catch {
          await new Promise((resolve) => setTimeout(resolve, 25));
        }
      }
      const mutation = await fetch(`${baseUrl}/docs`, {
        method: "POST",
        headers: { Accept: "application/json" },
      });
      expect(mutation.status).toBe(200);
      // Release GET 1's loader only now, after the mutation completed.
      await fs.writeFile(fenceDir + "/gate", "1", "utf-8");

      // GET 1 finishes AFTER the mutation completed. Its fill was fenced,
      // so nothing stale was published.
      expect(await inFlight).toBe(1);

      // GET 2 must re-run the loader: the mutation request itself runs the
      // route loader (count 2) before GET 2 (count 3). If GET 1's fenced
      // fill had been published after the completion invalidation, GET 2
      // would instead be a cache HIT serving the stale loadCount=1.
      expect(await getLoadCount()).toBe(3);
      // GET 3 is a normal post-mutation cache hit.
      expect(await getLoadCount()).toBe(3);
    }
  );
});
