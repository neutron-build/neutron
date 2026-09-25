import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createServer, type NeutronServer } from "./index.js";

/**
 * Regression: `@hono/node-server` replaces globalThis.Response, and the
 * inherited static factories (`Response.json()`) return native instances that
 * fail `instanceof` against the replacement. An action returning
 * `Response.json({...}, { status: 201 })` — the create-neutron app template's
 * own shape — was treated as plain action data and served as 200 `{}`.
 * Both construction styles must answer as themselves, from actions and
 * loaders alike.
 */

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

function routeSource(make: string): string {
  return `
import { h } from "preact";
export const config = { mode: "app" };
const make = ${make};
export async function loader() {
  return make({ from: "loader" }, 202);
}
export async function action() {
  return make({ from: "action" }, 201);
}
export default function Page() {
  return h("div", null, "never rendered");
}
`;
}

const FACTORY = `(body, status) => Response.json(body, { status, headers: { "x-made-by": "factory" } })`;
const CTOR = `(body, status) => new Response(JSON.stringify(body), {
  status,
  headers: { "content-type": "application/json", "x-made-by": "ctor" },
})`;

let root = "";
let running: NeutronServer | null = null;
let base = "";

beforeAll(async () => {
  root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-response-factories-"));
  await fs.mkdir(path.join(root, "src", "routes"), { recursive: true });
  await fs.writeFile(path.join(root, "src", "routes", "factory.tsx"), routeSource(FACTORY), "utf-8");
  await fs.writeFile(path.join(root, "src", "routes", "ctor.tsx"), routeSource(CTOR), "utf-8");

  const port = await getFreePort();
  running = await createServer({
    host: "127.0.0.1",
    port,
    rootDir: root,
    distDir: "dist",
    routesDir: "src/routes",
    compress: false,
  });
  base = `http://127.0.0.1:${port}`;
}, 30_000);

afterAll(async () => {
  await running?.close();
  if (root) await fs.rm(root, { recursive: true, force: true });
});

describe("Responses returned from loaders and actions keep their status and body", () => {
  for (const [route, madeBy] of [
    ["factory", "factory"],
    ["ctor", "ctor"],
  ] as const) {
    it(`${route}: action → 201 with the JSON body`, async () => {
      const res = await fetch(`${base}/${route}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "{}",
      });
      expect(res.status).toBe(201);
      expect(res.headers.get("x-made-by")).toBe(madeBy);
      expect(await res.json()).toEqual({ from: "action" });
    });

    it(`${route}: loader → 202 with the JSON body`, async () => {
      const res = await fetch(`${base}/${route}`);
      expect(res.status).toBe(202);
      expect(res.headers.get("x-made-by")).toBe(madeBy);
      expect(await res.json()).toEqual({ from: "loader" });
    });
  }
});
