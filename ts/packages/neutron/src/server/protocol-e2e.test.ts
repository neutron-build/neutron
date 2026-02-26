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
  await fs.mkdir(path.join(rootDir, "src", "routes", "users"), { recursive: true });

  await fs.writeFile(
    path.join(rootDir, "dist", "index.html"),
    "<!doctype html><html><body><h1>Static fixture</h1></body></html>",
    "utf-8"
  );

  await fs.writeFile(
    path.join(rootDir, "src", "routes", "users", "[id].ts"),
    `
import { h } from "preact";

export const config = { mode: "app", cache: { maxAge: 30 } };

export function headers() {
  return {
    "Cache-Control": "public, max-age=30",
    Vary: "Accept",
  };
}

export async function loader({ params }) {
  return {
    user: {
      id: params.id || "0",
      name: "User " + (params.id || "0"),
    },
  };
}

export default function UserPage({ data }) {
  return h("main", null, "User: " + data.user.name);
}
`,
    "utf-8"
  );

  await fs.writeFile(
    path.join(rootDir, "src", "routes", "users", "no-hydrate.ts"),
    `
import { h } from "preact";

export const config = { mode: "app", hydrate: false };

export async function loader() {
  return {
    message: "No hydrate payload",
  };
}

export default function NoHydratePage({ data }) {
  return h("main", null, "NoHydrate: " + data.message);
}
`,
    "utf-8"
  );
}

describe("protocol e2e", () => {
  let fixtureRoot = "";
  let closeServer: (() => Promise<void>) | null = null;
  let baseUrl = "";

  beforeAll(async () => {
    fixtureRoot = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-protocol-e2e-"));
    await writeFixtureApp(fixtureRoot);

    const port = await getFreePort();
    const running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: fixtureRoot,
      distDir: "dist",
      routesDir: "src/routes",
      compress: false,
      routes: {
        redirects: [{ source: "/legacy/:id", destination: "/users/:id" }],
        rewrites: [{ source: "/profile/:id", destination: "/users/:id" }],
        headers: [{ source: "/users/:id", headers: { "X-Test-Header": "enabled" } }],
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

  it("serves static and app responses with correct etag/head/304 semantics", async () => {
    const staticGet = await fetch(`${baseUrl}/`);
    expect(staticGet.status).toBe(200);
    expect(staticGet.headers.get("etag")).toBeTruthy();
    expect(staticGet.headers.get("cache-control")).toContain("must-revalidate");
    expect(await staticGet.text()).toContain("Static fixture");

    const staticEtag = staticGet.headers.get("etag");
    expect(staticEtag).toBeTruthy();

    const staticHead = await fetch(`${baseUrl}/`, { method: "HEAD" });
    expect(staticHead.status).toBe(200);
    expect(await staticHead.text()).toBe("");

    const static304 = await fetch(`${baseUrl}/`, {
      headers: { "If-None-Match": staticEtag! },
    });
    expect(static304.status).toBe(304);
    expect(static304.headers.get("x-neutron-cache")).toBe("REVALIDATED");
    expect(static304.headers.get("content-length")).toBeNull();
    expect(await static304.text()).toBe("");

    const appMiss = await fetch(`${baseUrl}/users/1`, {
      headers: { Accept: "application/json" },
    });
    expect(appMiss.status).toBe(200);
    expect(appMiss.headers.get("x-neutron-cache")).toBeNull();
    const missPayload = await appMiss.json();
    expect(typeof missPayload.__neutron_serialized__).toBe("string");

    const appHit = await fetch(`${baseUrl}/users/1`, {
      headers: { Accept: "application/json" },
    });
    expect(appHit.status).toBe(200);
    expect(appHit.headers.get("x-neutron-cache")).toBe("HIT");
    const appHitEtag = appHit.headers.get("etag");
    expect(appHitEtag).toBeTruthy();

    const appHead = await fetch(`${baseUrl}/users/1`, {
      method: "HEAD",
      headers: { Accept: "application/json" },
    });
    expect(appHead.status).toBe(200);
    expect(appHead.headers.get("x-neutron-cache")).toBe("HIT");
    expect(await appHead.text()).toBe("");

    const app304 = await fetch(`${baseUrl}/users/1`, {
      headers: {
        Accept: "application/json",
        "If-None-Match": appHitEtag!,
      },
    });
    expect(app304.status).toBe(304);
    expect(app304.headers.get("x-neutron-cache")).toBe("HIT");
    expect(app304.headers.get("content-length")).toBeNull();
    expect(await app304.text()).toBe("");

    const hydratedHtml = await fetch(`${baseUrl}/users/1`);
    expect(hydratedHtml.status).toBe(200);
    const hydratedBody = await hydratedHtml.text();
    expect(hydratedBody).toContain("__NEUTRON_DATA_SERIALIZED__");

    const noHydrateHtml = await fetch(`${baseUrl}/users/no-hydrate`);
    expect(noHydrateHtml.status).toBe(200);
    const noHydrateBody = await noHydrateHtml.text();
    expect(noHydrateBody).toContain("NoHydrate: No hydrate payload");
    expect(noHydrateBody).not.toContain("__NEUTRON_DATA_SERIALIZED__");
  }, 30000);

  it("applies redirects, rewrites, and route headers from config", async () => {
    const redirectResponse = await fetch(`${baseUrl}/legacy/9`, {
      redirect: "manual",
    });
    expect(redirectResponse.status).toBe(307);
    expect(redirectResponse.headers.get("location")).toBe("/users/9");

    const rewrittenResponse = await fetch(`${baseUrl}/profile/7`, {
      headers: { Accept: "application/json" },
    });
    expect(rewrittenResponse.status).toBe(200);
    const payload = await rewrittenResponse.json() as { __neutron_serialized__?: string };
    expect(typeof payload.__neutron_serialized__).toBe("string");

    const headerResponse = await fetch(`${baseUrl}/users/7`);
    expect(headerResponse.headers.get("x-test-header")).toBe("enabled");
  });
});
