import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as net from "node:net";
import { afterAll, describe, expect, it } from "vitest";
import { createServer } from "./index.js";

/**
 * The SSR catch-all's DEFAULT 404 (no user `not-found.tsx`) is content
 * negotiated: HTML-preferring clients get the minimal HTML page (bare
 * text/plain "Not Found" is not a page a browser can render), everything
 * else gets the RFC 7807 problem document in exactly the shape api mode
 * serves (FRAMEWORK_CONTRACT.md §2). A user `not-found.tsx` wins for every
 * client. api mode stays pure problem+json — its tests pin it; one
 * negotiation request proves HTML preference does not leak into it.
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

const INDEX_ROUTE = `
import { h } from "preact";
export const config = { mode: "app" };
export default function Home() {
  return h("div", null, "home");
}
`;

const NOT_FOUND_ROUTE = `
import { h } from "preact";
export default function NotFound() {
  return h("div", { "data-custom-not-found": "yes" }, "these are not the droids");
}
`;

/** An SSR app root with one route; `withNotFound` adds a not-found.tsx. */
async function makeApp(withNotFound: boolean): Promise<string> {
  // Inside the package dir so the Vite SSR runtime can resolve node_modules.
  const root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-ssr-404-"));
  roots.push(root);

  await fs.mkdir(path.join(root, "src", "routes"), { recursive: true });
  await fs.writeFile(path.join(root, "src", "routes", "index.tsx"), INDEX_ROUTE, "utf-8");
  if (withNotFound) {
    await fs.writeFile(
      path.join(root, "src", "routes", "not-found.tsx"),
      NOT_FOUND_ROUTE,
      "utf-8"
    );
  }
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

// A browser-ish Accept: HTML first, wildcards strictly lower.
const BROWSER_ACCEPT =
  "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";

describe("SSR-mode default 404 (no user not-found.tsx)", () => {
  it("serves the HTML 404 page to text/html clients", async () => {
    const base = await boot(await makeApp(false));

    const res = await fetch(`${base}/definitely-missing`, {
      headers: { Accept: BROWSER_ACCEPT },
    });
    expect(res.status).toBe(404);
    expect(res.headers.get("content-type")).toContain("text/html");
    const body = await res.text();
    expect(body.startsWith("<!doctype html>")).toBe(true);
    // The notFound() shell: a real page a browser can render.
    expect(body).toContain("<h1>404</h1>");
    expect(body).toContain("This page could not be found.");
  });

  it("serves RFC 7807 problem+json to application/json clients", async () => {
    const base = await boot(await makeApp(false));

    const res = await fetch(`${base}/definitely-missing`, {
      headers: { Accept: "application/json" },
    });
    expect(res.status).toBe(404);
    expect(res.headers.get("content-type")).toContain("application/problem+json");
    // Exactly api mode's shape (server-modes.test.ts pins the api half).
    expect(await res.json()).toEqual({
      type: "https://neutron.dev/errors/not-found",
      title: "Not Found",
      status: 404,
      detail: "No route matches /definitely-missing",
      instance: "/definitely-missing",
    });
  });

  it("serves problem+json when no Accept preference is expressed", async () => {
    const base = await boot(await makeApp(false));

    // Node's fetch sends no Accept header by default — an unopinionated
    // client gets the machine-readable default, like api mode.
    const res = await fetch(`${base}/definitely-missing`);
    expect(res.status).toBe(404);
    expect(res.headers.get("content-type")).toContain("application/problem+json");
    const problem = (await res.json()) as { detail: string };
    expect(problem.detail).toBe("No route matches /definitely-missing");
  });

  it("serves problem+json to X-Neutron-Data requests even alongside Accept: text/html", async () => {
    const base = await boot(await makeApp(false));

    // The client router's data requests must never receive HTML to parse.
    const res = await fetch(`${base}/definitely-missing`, {
      headers: { "X-Neutron-Data": "true", Accept: BROWSER_ACCEPT },
    });
    expect(res.status).toBe(404);
    expect(res.headers.get("content-type")).toContain("application/problem+json");
  });
});

describe("user not-found.tsx wins over the default for every client", () => {
  it("renders the app's own page (through its layouts) as the 404", async () => {
    const base = await boot(await makeApp(true));

    const res = await fetch(`${base}/definitely-missing`, {
      headers: { Accept: BROWSER_ACCEPT },
    });
    expect(res.status).toBe(404);
    expect(res.headers.get("content-type")).toContain("text/html");
    const body = await res.text();
    expect(body).toContain("these are not the droids");
    expect(body).not.toContain("This page could not be found.");
  });

  it("answers JSON clients with the page's JSON envelope, not the default problem", async () => {
    const base = await boot(await makeApp(true));

    const res = await fetch(`${base}/definitely-missing`, {
      headers: { Accept: "application/json" },
    });
    expect(res.status).toBe(404);
    // The not-found route renders through the app pipeline like any route:
    // JSON requests get its loader envelope. problem+json here would mean
    // the default had preempted the app's page.
    expect(res.headers.get("content-type")).toContain("application/json");
    expect(res.headers.get("content-type")).not.toContain("problem");
  });
});

describe("api mode: no content negotiation", () => {
  it("answers problem+json even to text/html clients", async () => {
    const root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-api-404-"));
    roots.push(root);
    const port = await getFreePort();
    const running = await createServer({
      mode: "api",
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
    });
    closers.push(running.close);

    const res = await fetch(`http://127.0.0.1:${port}/missing`, {
      headers: { Accept: BROWSER_ACCEPT },
    });
    expect(res.status).toBe(404);
    expect(res.headers.get("content-type")).toContain("application/problem+json");
  });
});
