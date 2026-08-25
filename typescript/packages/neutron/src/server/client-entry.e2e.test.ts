import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as net from "node:net";
import { afterAll, describe, expect, it } from "vitest";
import { createServer } from "./index.js";

/**
 * `.neutron-client-entry.json` is the authoritative record of which bundled
 * `assets/index-*.js` is the hydration entry. The scan-and-sort fallback
 * compares filenames lexicographically, so `index-9.js` beats `index-10.js`
 * and the wrong entry script gets served once a build produces ten or more
 * chunks. The metadata must win over the heuristic, not the other way round.
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

const APP_ROUTE = `
import { h } from "preact";
export const config = { mode: "app" };
export default function Page() {
  return h("div", null, "hello");
}
`;

async function makeApp(): Promise<string> {
  // Inside the package dir so the Vite SSR runtime can resolve node_modules.
  const root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-entry-"));
  roots.push(root);

  await fs.mkdir(path.join(root, "src", "routes"), { recursive: true });
  await fs.writeFile(path.join(root, "src", "routes", "index.tsx"), APP_ROUTE, "utf-8");

  // A dist with two entry-looking chunks: the metadata names index-10, the
  // lexicographic scan would pick index-9.
  await fs.mkdir(path.join(root, "dist", "assets"), { recursive: true });
  await fs.writeFile(path.join(root, "dist", "assets", "index-9.js"), "// chunk 9", "utf-8");
  await fs.writeFile(path.join(root, "dist", "assets", "index-10.js"), "// chunk 10", "utf-8");
  await fs.writeFile(
    path.join(root, "dist", ".neutron-client-entry.json"),
    JSON.stringify({ src: "/assets/index-10.js" }),
    "utf-8"
  );

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

describe("client entry resolution trusts the build metadata over a filename scan", () => {
  it("serves the entry named by .neutron-client-entry.json, not the lexically-largest index-*.js", { timeout: 30_000 }, async () => {
    const base = await boot(await makeApp());

    const res = await fetch(`${base}/`);
    const html = await res.text();

    expect(res.status).toBe(200);
    expect(html).toContain('/assets/index-10.js');
    expect(html).not.toContain('/assets/index-9.js');
  });
});
