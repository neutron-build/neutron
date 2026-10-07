import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createServer } from "./index.js";

/**
 * The data protocol serves a second representation of the SAME URL: the
 * client router asks for `{"__neutron_serialized__": ...}` with
 * `Accept: application/json` + `X-Neutron-Data: true`, while a navigation gets
 * hydrated HTML. The server-side app-response cache separates the variants in
 * its key (TS-04), but the WIRE headers never declared that variation: a JSON
 * HIT left the server advertising the framework-synthesized
 * `Cache-Control: public, max-age=N` with no `Vary`, so the browser HTTP cache
 * stored the data payload under the bare URL and handed it to the next
 * document navigation — a reload painted the raw serialized payload as page
 * content. Both variants must declare every representation dimension the
 * server cache keys on (Accept, Accept-Language, X-Neutron-Data,
 * X-Neutron-Routes) so no shared cache can cross-serve them.
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

function page(body: string): string {
  return `
import { h } from "preact";
export default function Page() {
  return h("main", null, ${JSON.stringify(body)});
}
`;
}

async function writeFixtureApp(rootDir: string): Promise<void> {
  await fs.mkdir(path.join(rootDir, "dist"), { recursive: true });
  await fs.mkdir(path.join(rootDir, "src", "routes"), { recursive: true });

  await fs.writeFile(path.join(rootDir, "dist", "index.html"), "<!doctype html><html><body>static</body></html>", "utf-8");

  // Cacheable route WITHOUT its own headers() export — the exact shape whose
  // synthesized `Cache-Control: public, max-age=N` used to ship with no Vary.
  await fs.writeFile(
    path.join(rootDir, "src", "routes", "cached.ts"),
    `
import { h } from "preact";
export const config = { mode: "app", cache: { maxAge: 30 } };
export async function loader() {
  return { ok: true };
}
export default function Page({ data }) {
  return h("main", null, "cached " + data.ok);
}
`,
    "utf-8"
  );

  // Plain uncached route — the MISS-only path (no synthesized Cache-Control).
  await fs.writeFile(
    path.join(rootDir, "src", "routes", "plain.ts"),
    `
import { h } from "preact";
export const config = { mode: "app" };
export async function loader() {
  return { ok: true };
}
export default function Page({ data }) {
  return h("main", null, "plain " + data.ok);
}
`,
    "utf-8"
  );

  // Route that declares part of the variation itself — the framework must
  // MERGE the remaining dimensions, not replace the route's token.
  await fs.writeFile(
    path.join(rootDir, "src", "routes", "own-vary.ts"),
    `
import { h } from "preact";
export const config = { mode: "app", cache: { maxAge: 30 } };
export function headers() {
  return { Vary: "Accept-Language" };
}
export async function loader() {
  return { ok: true };
}
export default function Page({ data }) {
  return h("main", null, "own-vary " + data.ok);
}
`,
    "utf-8"
  );
}

/** Data-protocol request, exactly as the client router issues it. */
function dataInit(): RequestInit {
  return { headers: { Accept: "application/json", "X-Neutron-Data": "true" } };
}

/** Document-navigation request, as a reload issues it. */
function documentInit(): RequestInit {
  return { headers: { Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8" } };
}

function varyTokens(value: string | null): Set<string> {
  return new Set(
    (value ?? "")
      .split(",")
      .map((token) => token.trim().toLowerCase())
      .filter(Boolean)
  );
}

const REPRESENTATION_DIMENSIONS = ["accept", "accept-language", "x-neutron-data", "x-neutron-routes"];

beforeAll(async () => {
  fixtureRoot = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-data-protocol-vary-"));
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

describe("data-protocol responses cannot poison shared caches", () => {
  it("a JSON data response declares every representation dimension in Vary", { timeout: 30_000 }, async () => {
    const res = await fetch(`${baseUrl}/cached`, dataInit());
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toContain("application/json");

    const tokens = varyTokens(res.headers.get("vary"));
    for (const dimension of REPRESENTATION_DIMENSIONS) {
      expect(tokens, `Vary must include ${dimension} (got: ${[...tokens].join(", ")})`).toContain(dimension);
    }
  });

  it("a cache HIT advertising synthesized Cache-Control still carries the Vary set", { timeout: 30_000 }, async () => {
    // First fill: MISS (stores the entry).
    await fetch(`${baseUrl}/cached`, dataInit());
    // Second read: served from the app-response cache with the synthesized
    // `Cache-Control: public, max-age=N`. This is the response a browser used
    // to store for the bare URL and then paint on reload.
    const hit = await fetch(`${baseUrl}/cached`, dataInit());
    expect(hit.headers.get("x-neutron-cache")).toBe("HIT");
    expect(hit.headers.get("cache-control")).toContain("public");

    const tokens = varyTokens(hit.headers.get("vary"));
    for (const dimension of REPRESENTATION_DIMENSIONS) {
      expect(tokens, `HIT Vary must include ${dimension} (got: ${[...tokens].join(", ")})`).toContain(dimension);
    }
  });

  it("a document request after data requests receives HTML, never the serialized payload", { timeout: 30_000 }, async () => {
    // Populate both the server cache and (in a real browser) the HTTP cache
    // with the JSON variant first — the exact reload-poisoning sequence.
    await fetch(`${baseUrl}/cached`, dataInit());
    await fetch(`${baseUrl}/cached`, dataInit());

    const document = await fetch(`${baseUrl}/cached`, documentInit());
    expect(document.status).toBe(200);
    expect(document.headers.get("content-type")).toContain("text/html");
    const body = await document.text();
    expect(body.startsWith('{"__neutron_serialized__"')).toBe(false);
    expect(body).toContain("cached true");
  });

  it("route-declared Vary tokens are merged, not replaced", { timeout: 30_000 }, async () => {
    const res = await fetch(`${baseUrl}/own-vary`, dataInit());
    expect(res.status).toBe(200);

    const tokens = varyTokens(res.headers.get("vary"));
    expect(tokens).toContain("accept-language");
    for (const dimension of REPRESENTATION_DIMENSIONS) {
      expect(tokens).toContain(dimension);
    }
    // No duplicated tokens from the merge.
    expect([...tokens].length).toBe(new Set([...tokens]).size);
  });

  it("HTML document responses declare the same dimensions (cached HTML cannot answer a data fetch)", { timeout: 30_000 }, async () => {
    const document = await fetch(`${baseUrl}/plain`, documentInit());
    expect(document.status).toBe(200);
    expect(document.headers.get("content-type")).toContain("text/html");

    const tokens = varyTokens(document.headers.get("vary"));
    for (const dimension of REPRESENTATION_DIMENSIONS) {
      expect(tokens, `document Vary must include ${dimension} (got: ${[...tokens].join(", ")})`).toContain(dimension);
    }

    // The reverse direction: a data request after the document was cached
    // must still get the JSON representation from the server.
    const data = await fetch(`${baseUrl}/plain`, dataInit());
    expect(data.headers.get("content-type")).toContain("application/json");
    const payload = (await data.json()) as { __neutron_serialized__?: string };
    expect(typeof payload.__neutron_serialized__).toBe("string");
  });
});
