import * as fs from "node:fs/promises";
import * as http from "node:http";
import * as net from "node:net";
import * as path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { createServer as createViteServer } from "vite";
import { createServer, type NeutronServer } from "./index.js";
import { neutronPlugin } from "../vite/plugin.js";
import { resolvePreactSsr, vitePreactAliases } from "../core/preact-ssr.js";

/**
 * F-P6-2: the app's not-found.tsx answers every 404 a page produces, not
 * only an unmatched path — a static route asked for a param its
 * getStaticPaths did not generate, and a page loader's notFound() /
 * notFoundError() — with its layouts and status 404, in `start` (here the
 * Vite-runtime fallback; the server bundle is covered by the CLI's build +
 * start test) and in dev.
 *
 * Route files import "@neutron-build/core" (Node's self-reference: the
 * built dist), so the server and the routes hold different copies of core,
 * as under the Vite SSR runtime. Build before test, as CI does.
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
      socket.close((error) => (error ? reject(error) : resolve(address.port)));
    });
    socket.on("error", reject);
  });
}

const FILES: Record<string, string> = {
  "src/routes/_layout.tsx": `
import { h } from "preact";
export default function Layout({ children }) { return h("main", null, h("nav", null, "site-chrome"), children); }
`,
  "src/routes/not-found.tsx": `
import { h } from "preact";
export const config = { mode: "app" };
export function head() { return { title: "Missing page" }; }
export default function NotFound() { return h("p", null, "custom-missing"); }
`,
  "src/routes/index.tsx": `
import { h } from "preact";
export default function Home() { return h("p", null, "home"); }
`,
  "src/routes/models/[id].tsx": `
import { h } from "preact";
export async function getStaticPaths() { return { paths: [{ params: { id: "known" } }] }; }
export async function loader({ params }) { return { id: params.id }; }
export default function Model({ data }) { return h("p", null, "model " + data.id); }
`,
  "src/routes/items/[id].tsx": `
import { h } from "preact";
import { notFound, notFoundError } from "@neutron-build/core";
export const config = { mode: "app" };
export async function loader({ params }) {
  if (params.id === "thrown") {
    const response = notFound();
    response.headers.append("set-cookie", "seen=1; Path=/");
    throw response;
  }
  if (params.id === "returned") return notFound();
  if (params.id === "problem") throw notFoundError("No such item");
  if (params.id === "custom") return new Response("custom 404 body", { status: 404 });
  return { id: params.id };
}
export default function Item({ data }) { return h("p", null, "item " + data.id); }
`,
  "src/routes/shops/[shop]/_layout.tsx": `
import { h } from "preact";
import { notFound } from "@neutron-build/core";
export const config = { mode: "app" };
export async function loader({ params }) {
  if (params.shop === "closed") throw notFound();
  return { shop: params.shop };
}
export default function Shop({ data, children }) { return h("section", null, "shop " + data.shop, children); }
`,
  "src/routes/shops/[shop]/index.tsx": `
import { h } from "preact";
export const config = { mode: "app" };
export default function ShopHome() { return h("p", null, "shop home"); }
`,
  // A client entry, so documents carry the entry script: src/main.tsx in
  // dev, the build's entry metadata in start.
  "src/main.tsx": "export {};\n",
  "dist/.neutron-client-entry.json": JSON.stringify({ src: "/assets/entry.js" }),
  // What `neutron-ts build` writes for the static route: only the listed id.
  "dist/models/known/index.html": "<!doctype html><html><body><p>model known (prebuilt)</p></body></html>",
};

let root = "";
const closers: Array<() => Promise<void>> = [];

afterEach(async () => {
  while (closers.length > 0) await closers.pop()!();
  if (root) {
    await fs.rm(root, { recursive: true, force: true });
    root = "";
  }
});

async function writeApp(): Promise<void> {
  root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-not-found-page-"));
  for (const [file, source] of Object.entries(FILES)) {
    await fs.mkdir(path.dirname(path.join(root, file)), { recursive: true });
    await fs.writeFile(path.join(root, file), source, "utf-8");
  }
}

async function bootStart(): Promise<string> {
  await writeApp();
  const port = await getFreePort();
  const running: NeutronServer = await createServer({ host: "127.0.0.1", port, rootDir: root, compress: false });
  closers.push(() => running.close());
  return `http://127.0.0.1:${port}`;
}

async function bootDev(): Promise<string> {
  await writeApp();
  await fs.rm(path.join(root, "dist"), { recursive: true, force: true });
  const port = await getFreePort();
  const preactSsr = resolvePreactSsr(root);
  const vite = await createViteServer({
    configFile: false,
    root,
    logLevel: "error",
    plugins: [neutronPlugin({ routesDir: path.join(root, "src", "routes"), rootDir: root })],
    resolve: { alias: vitePreactAliases(preactSsr) },
    ssr: { noExternal: preactSsr.noExternal },
    server: { middlewareMode: true },
  });
  const httpServer = http.createServer();
  httpServer.on("request", vite.middlewares);
  closers.push(async () => {
    await vite.close();
    await new Promise<void>((resolve) => httpServer.close(() => resolve()));
  });
  await new Promise<void>((resolve, reject) => {
    httpServer.listen(port, "127.0.0.1", () => resolve());
    httpServer.on("error", reject);
  });
  return `http://127.0.0.1:${port}`;
}

const html = { accept: "text/html" };

async function expectNotFoundPage(base: string, pathname: string): Promise<Response> {
  const response = await fetch(`${base}${pathname}`, { headers: html });
  const body = await response.text();
  expect(response.status, `${pathname}: ${body.slice(0, 300)}`).toBe(404);
  expect(response.headers.get("content-type")).toContain("text/html");
  expect(body, pathname).toContain("custom-missing");
  expect(body, pathname).toContain("site-chrome");
  expect(body, pathname).toContain("<title>Missing page</title>");
  // F-P6-5: the document names the not-found route it rendered, on the
  // client entry, so the router hydrates that route and not the page the
  // URL matches.
  expect(body, pathname).toMatch(/<script type="module"[^>]* data-neutron-not-found="route:not-found\.tsx"[^>]*><\/script>/);
  return response;
}

for (const [label, boot] of [["start (Vite runtime)", bootStart], ["dev", bootDev]] as const) {
  describe(`app not-found page for page 404s — ${label}`, () => {
    it("answers a static route's ungenerated param", { timeout: 60_000 }, async () => {
      const base = await boot();
      await expectNotFoundPage(base, "/models/unknown");
      const known = await fetch(`${base}/models/known`, { headers: html });
      expect(known.status).toBe(200);
      expect(await known.text()).toContain("model known");
    });

    it("answers a loader's notFound(), returned or thrown, and notFoundError()", { timeout: 60_000 }, async () => {
      const base = await boot();
      const thrown = await expectNotFoundPage(base, "/items/thrown");
      // Headers the loader put on its 404 are kept.
      expect(thrown.headers.get("set-cookie")).toContain("seen=1");
      await expectNotFoundPage(base, "/items/returned");
      await expectNotFoundPage(base, "/items/problem");
      // A layout loader's notFound() answers the same way.
      await expectNotFoundPage(base, "/shops/closed");

      // A 404 Response of the loader's own making is sent as written.
      const custom = await fetch(`${base}/items/custom`, { headers: html });
      expect(custom.status).toBe(404);
      expect(await custom.text()).toBe("custom 404 body");

      const ok = await fetch(`${base}/items/present`, { headers: html });
      expect(ok.status).toBe(200);
      const okBody = await ok.text();
      expect(okBody).toContain("item present");
      // Only a not-found render is marked.
      expect(okBody).not.toContain("data-neutron-not-found");
      const shop = await fetch(`${base}/shops/open`, { headers: html });
      expect(shop.status).toBe(200);
      const shopBody = await shop.text();
      expect(shopBody).toContain("shop home");
      expect(shopBody).not.toContain("data-neutron-not-found");
    });
  });
}

describe("app not-found page for an unmatched path — start", () => {
  it("names the not-found route too", { timeout: 60_000 }, async () => {
    const base = await bootStart();
    await expectNotFoundPage(base, "/nowhere/at/all");
  });
});

describe("app not-found page for page 404s — start, data requests", () => {
  it("keeps the loader's 404 for a client-navigation data request", { timeout: 60_000 }, async () => {
    const base = await bootStart();
    const data = await fetch(`${base}/items/thrown`, { headers: { "x-neutron-data": "true", accept: "application/json" } });
    expect(data.status).toBe(404);
    expect(await data.text()).not.toContain("custom-missing");
  });
});
