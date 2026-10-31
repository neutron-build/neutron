import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { createServer, type NeutronServer } from "./index.js";

/**
 * Global middleware (src/middleware.ts) runs for every request the SSR
 * catch-all answers, FRAMEWORK_CONTRACT §5: authentication precedes the route
 * handler, and "no route matched" is the handler's answer, not a reason to skip
 * the stack. It used to run for an unmatched path only when the app had a
 * not-found.tsx (whose render runs the chain), so an auth gate leaked the
 * framework's 404 to unauthenticated clients when it did not.
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

const GATE = `
export default async function middleware(request, context, next) {
  if (request.headers.get("x-token") !== "ok") {
    return new Response("login required", { status: 401, headers: { "x-gate": "global" } });
  }
  const response = await next();
  response.headers.set("x-gate", "passed");
  return response;
}
`;

const PAGE = `
import { h } from "preact";
export const config = { mode: "app" };
export default function Page() { return h("p", null, "home"); }
`;

const NOT_FOUND = `
import { h } from "preact";
export const config = { mode: "app" };
export default function NotFound() { return h("p", null, "custom missing"); }
`;

describe("global middleware on unmatched paths", () => {
  let running: NeutronServer | null = null;
  let root = "";

  afterEach(async () => {
    if (running) {
      await running.close();
      running = null;
    }
    if (root) {
      await fs.rm(root, { recursive: true, force: true });
      root = "";
    }
  });

  async function boot(withNotFound: boolean, routes: Record<string, string> = {}): Promise<string> {
    root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-global-mw-"));
    await fs.mkdir(path.join(root, "src", "routes"), { recursive: true });
    await fs.writeFile(path.join(root, "src", "middleware.ts"), GATE, "utf-8");
    await fs.writeFile(path.join(root, "src", "routes", "index.tsx"), PAGE, "utf-8");
    if (withNotFound) {
      await fs.writeFile(path.join(root, "src", "routes", "not-found.tsx"), NOT_FOUND, "utf-8");
    }
    for (const [name, source] of Object.entries(routes)) {
      await fs.writeFile(path.join(root, "src", "routes", name), source, "utf-8");
    }
    const port = await getFreePort();
    running = await createServer({ host: "127.0.0.1", port, rootDir: root, compress: false });
    return `http://127.0.0.1:${port}`;
  }

  it("gates a static route whose page is missing from dist", { timeout: 30_000 }, async () => {
    const base = await boot(false, { "about.tsx": PAGE.replace('mode: "app"', 'mode: "static"') });

    expect((await fetch(`${base}/about`)).status).toBe(401);
    const missing = await fetch(`${base}/about`, { headers: { "x-token": "ok" } });
    expect(missing.status).toBe(404);
    expect(missing.headers.get("x-gate")).toBe("passed");
  });

  for (const withNotFound of [false, true]) {
    const label = withNotFound ? "with not-found.tsx" : "without not-found.tsx";

    it(`gates an unmatched path ${label}`, { timeout: 30_000 }, async () => {
      const base = await boot(withNotFound);

      const denied = await fetch(`${base}/no/such/page`);
      expect(denied.status).toBe(401);
      expect(denied.headers.get("x-gate")).toBe("global");
      expect(await denied.text()).toBe("login required");

      // Past the gate, the 404 is the usual one and the middleware saw it.
      const missing = await fetch(`${base}/no/such/page`, {
        headers: { "x-token": "ok", Accept: "application/json" },
      });
      expect(missing.status).toBe(404);
      expect(missing.headers.get("x-gate")).toBe("passed");
      if (!withNotFound) {
        expect(missing.headers.get("content-type")).toContain("application/problem+json");
      }
    });
  }
});
