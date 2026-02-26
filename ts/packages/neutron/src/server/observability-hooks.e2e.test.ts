import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as net from "node:net";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createServer } from "./index.js";

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
      socket.close((error) => {
        if (error) {
          reject(error);
          return;
        }
        resolve(port);
      });
    });
    socket.on("error", reject);
  });
}

async function writeFixtureApp(rootDir: string): Promise<void> {
  await fs.mkdir(path.join(rootDir, "dist"), { recursive: true });
  await fs.mkdir(path.join(rootDir, "src", "routes"), { recursive: true });

  await fs.writeFile(
    path.join(rootDir, "dist", "index.html"),
    "<!doctype html><html><body><h1>Observability fixture</h1></body></html>",
    "utf-8"
  );

  await fs.writeFile(
    path.join(rootDir, "src", "routes", "hooked.ts"),
    `
import { h } from "preact";
export const config = { mode: "app" };
export async function loader() {
  return { ok: true };
}
export async function action() {
  return { ok: true, mutated: true };
}
export default function Hooked({ data }) {
  return h("div", null, "hooked:" + String(data.ok));
}
`,
    "utf-8"
  );

  await fs.writeFile(
    path.join(rootDir, "src", "routes", "error.ts"),
    `
import { h } from "preact";
export const config = { mode: "app" };
export async function loader() {
  throw new Error("hooked boom");
}
export function ErrorBoundary({ error }) {
  return h("div", null, "boundary:" + error.message);
}
export default function ErrorPage() {
  return h("div", null, "unreachable");
}
`,
    "utf-8"
  );
}

describe("server observability hooks e2e", () => {
  let fixtureRoot = "";
  let closeServer: (() => Promise<void>) | null = null;
  let baseUrl = "";

  const requestStart: Array<{ pathname: string; requestId: string }> = [];
  const requestEnd: Array<{ pathname: string; requestId: string; status: number }> = [];
  const loaderStart: Array<{ routeId: string }> = [];
  const loaderEnd: Array<{ routeId: string; outcome: string }> = [];
  const actionStart: Array<{ routeId: string }> = [];
  const actionEnd: Array<{ routeId: string; outcome: string }> = [];
  const errors: Array<{ source: string; routeId?: string; message: string }> = [];

  beforeAll(async () => {
    fixtureRoot = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-hooks-e2e-"));
    await writeFixtureApp(fixtureRoot);

    const port = await getFreePort();
    const running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: fixtureRoot,
      distDir: "dist",
      routesDir: "src/routes",
      compress: false,
      hooks: {
        onRequestStart: (event) => {
          requestStart.push({ pathname: event.pathname, requestId: event.requestId });
        },
        onRequestEnd: (event) => {
          requestEnd.push({
            pathname: event.pathname,
            requestId: event.requestId,
            status: event.status,
          });
        },
        onLoaderStart: (event) => {
          loaderStart.push({ routeId: event.routeId });
        },
        onLoaderEnd: (event) => {
          loaderEnd.push({ routeId: event.routeId, outcome: event.outcome });
        },
        onActionStart: (event) => {
          actionStart.push({ routeId: event.routeId });
        },
        onActionEnd: (event) => {
          actionEnd.push({ routeId: event.routeId, outcome: event.outcome });
        },
        onError: (event) => {
          errors.push({
            source: event.source,
            routeId: event.routeId,
            message: event.error.message,
          });
        },
      },
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

  it("emits request/loader/action/error hooks", async () => {
    const getHooked = await fetch(`${baseUrl}/hooked`, {
      headers: { Accept: "application/json" },
    });
    expect(getHooked.status).toBe(200);

    const postHooked = await fetch(`${baseUrl}/hooked`, {
      method: "POST",
      headers: { Accept: "application/json" },
    });
    expect(postHooked.status).toBe(200);

    const getError = await fetch(`${baseUrl}/error`);
    expect(getError.status).toBe(500);

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(requestStart.length).toBeGreaterThanOrEqual(3);
    expect(requestEnd.length).toBeGreaterThanOrEqual(3);
    expect(requestEnd.some((event) => event.status === 500)).toBe(true);
    expect(requestEnd.some((event) => event.status === 200)).toBe(true);

    expect(loaderStart.some((event) => event.routeId === "route:hooked.ts")).toBe(true);
    expect(loaderStart.some((event) => event.routeId === "route:error.ts")).toBe(true);
    expect(
      loaderEnd.some(
        (event) => event.routeId === "route:hooked.ts" && event.outcome === "success"
      )
    ).toBe(true);
    expect(
      loaderEnd.some(
        (event) => event.routeId === "route:error.ts" && event.outcome === "error"
      )
    ).toBe(true);

    expect(actionStart.length).toBe(1);
    expect(actionEnd.length).toBe(1);
    expect(actionEnd[0].routeId).toBe("route:hooked.ts");
    expect(actionEnd[0].outcome).toBe("success");

    expect(
      errors.some(
        (event) =>
          event.source === "loader" &&
          event.routeId === "route:error.ts" &&
          event.message.includes("hooked boom")
      )
    ).toBe(true);
  }, 30000);
});
