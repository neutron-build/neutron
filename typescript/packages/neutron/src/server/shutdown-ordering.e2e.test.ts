import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createServer, type NeutronServer } from "./index.js";

/**
 * TS-29 (round 2): the teardown stages were constructed in the right order
 * but executed via `Promise.allSettled(teardown.map(stage => stage()))` —
 * every stage STARTED at once, so the SSR runtime (Vite) closed while
 * in-flight renders were still draining. The contract under test: a render
 * that is already in flight when close() is called must still complete
 * successfully, because the HTTP drain finishes before the SSR teardown
 * begins.
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

const SLOW_ROUTE = `
import { h } from "preact";
export const config = { mode: "app" };
let started = 0;
export async function loader() {
  started += 1;
  await new Promise((resolve) => setTimeout(resolve, 1500));
  return { started };
}
export default function Page({ data }) {
  return h("div", null, "started=" + data.started);
}
`;

async function writeFixtureApp(rootDir: string): Promise<void> {
  await fs.mkdir(path.join(rootDir, "src", "routes", "slow"), { recursive: true });
  await fs.writeFile(path.join(rootDir, "src", "routes", "slow", "index.ts"), SLOW_ROUTE, "utf-8");
}

beforeAll(async () => {
  fixtureRoot = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-shutdown-"));
  await writeFixtureApp(fixtureRoot);

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

describe("TS-29: shutdown stages run sequentially", () => {
  it(
    "an in-flight render completes even though close() was called",
    { timeout: 30_000 },
    async () => {
      // Start a render whose loader takes 1.5s, then immediately begin
      // shutdown. Under the old concurrent teardown the SSR runtime closed
      // at once and this render failed; sequentially, the HTTP drain stage
      // holds the SSR teardown until the response is out.
      const inFlight = fetch(`${baseUrl}/slow`, { headers: { Accept: "application/json" } });
      // Give the request a moment to reach the loader, then close.
      await new Promise((resolve) => setTimeout(resolve, 250));
      const closing = server!.close();

      const response = await inFlight;
      expect(response.status).toBe(200);
      // The render completed with its loader data — under the old
      // concurrent teardown the SSR runtime died mid-render instead.
      const body = await response.text();
      expect(body).toContain("started");

      await closing;
    }
  );

  it(
    "close() is idempotent — repeated calls join the same teardown",
    { timeout: 15_000 },
    async () => {
      await Promise.all([server!.close(), server!.close(), server!.close()]);
    }
  );
});
