import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { decodeSerializedPayload } from "../core/serialization.js";
import { collectChunkCss, ROUTE_CSS_MANIFEST, routeStylesheetHrefs } from "../core/route-css.js";
import { createServer } from "./index.js";

async function getFreePort(): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    const socket = net.createServer();
    socket.listen(0, "127.0.0.1", () => {
      const address = socket.address();
      if (!address || typeof address === "string") return reject(new Error("no port"));
      const { port } = address;
      socket.close(() => resolve(port));
    });
    socket.on("error", reject);
  });
}

describe("collectChunkCss", () => {
  it("lists a chunk's static imports' CSS before its own, once each, without following dynamic imports", () => {
    const manifest = {
      "index.html": { file: "assets/index.js", isEntry: true, css: ["assets/entry.css"], dynamicImports: ["page"] },
      _shared: { file: "assets/shared.js", css: ["assets/shared.css"] },
      _deep: { file: "assets/deep.js", imports: ["_shared"], css: ["assets/deep.css"] },
      page: { file: "assets/page.js", imports: ["index.html", "_deep", "_shared"], css: ["assets/page.css"] },
    };
    expect(collectChunkCss(manifest, "page")).toEqual([
      "/assets/entry.css",
      "/assets/shared.css",
      "/assets/deep.css",
      "/assets/page.css",
    ]);
    expect(collectChunkCss(manifest, "missing")).toEqual([]);
  });

  it("falls back to every stylesheet for a route the table does not know", () => {
    const table = { format: 1 as const, routes: { a: ["/assets/a.css"] } };
    expect(routeStylesheetHrefs(table, "a", ["/assets/a.css", "/assets/b.css"])).toEqual(["/assets/a.css"]);
    expect(routeStylesheetHrefs(table, "b", ["/assets/a.css", "/assets/b.css"])).toEqual([
      "/assets/a.css",
      "/assets/b.css",
    ]);
    expect(routeStylesheetHrefs(null, "a", ["/assets/x.css"])).toEqual(["/assets/x.css"]);
  });
});

describe("SSR links only the route's own stylesheets", () => {
  let root = "";
  let close: (() => Promise<void>) | null = null;
  let baseUrl = "";

  beforeAll(async () => {
    root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-route-css-"));
    await fs.mkdir(path.join(root, "dist", "assets"), { recursive: true });
    await fs.mkdir(path.join(root, "src", "routes"), { recursive: true });
    for (const name of ["layout", "shared", "shop", "docs"]) {
      await fs.writeFile(path.join(root, "dist", "assets", `${name}.css`), `:root { --${name}: 1 }`);
    }
    await fs.writeFile(
      path.join(root, "dist", ROUTE_CSS_MANIFEST),
      JSON.stringify({
        format: 1,
        routes: {
          "route:shop.ts": ["/assets/layout.css", "/assets/shared.css", "/assets/shop.css"],
          "route:docs.ts": ["/assets/layout.css", "/assets/shared.css", "/assets/docs.css"],
        },
      })
    );
    for (const name of ["shop", "docs", "fresh"]) {
      await fs.writeFile(
        path.join(root, "src", "routes", `${name}.ts`),
        `import { h } from "preact";
export const config = { mode: "app" };
export default function Page() { return h("p", null, "${name}-page"); }
`
      );
    }
    const port = await getFreePort();
    const running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: root,
      distDir: "dist",
      routesDir: "src/routes",
      compress: false,
    });
    close = running.close;
    baseUrl = `http://127.0.0.1:${port}`;
  });

  afterAll(async () => {
    await close?.();
    if (root) await fs.rm(root, { recursive: true, force: true });
  });

  const links = (html: string) =>
    [...html.matchAll(/<link rel="stylesheet" href="([^"]+)">/g)].map((match) => match[1]);

  it("in the document and in the navigation payload", async () => {
    const shop = await (await fetch(`${baseUrl}/shop`, { headers: { accept: "text/html" } })).text();
    expect(shop).toContain("shop-page");
    expect(links(shop)).toEqual(["/assets/layout.css", "/assets/shared.css", "/assets/shop.css"]);

    const docs = await fetch(`${baseUrl}/docs`, { headers: { accept: "application/json" } });
    const payload = decodeSerializedPayload<Record<string, unknown>>(await docs.json());
    expect(payload.__css__).toEqual(["/assets/layout.css", "/assets/shared.css", "/assets/docs.css"]);

    // A route the table does not know (added after the build) keeps every sheet.
    const fresh = await (await fetch(`${baseUrl}/fresh`, { headers: { accept: "text/html" } })).text();
    expect(links(fresh)).toEqual([
      "/assets/docs.css",
      "/assets/layout.css",
      "/assets/shared.css",
      "/assets/shop.css",
    ]);
  }, 60_000);
});
