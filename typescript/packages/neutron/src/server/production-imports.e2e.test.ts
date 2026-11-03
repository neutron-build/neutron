import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { afterAll, describe, expect, it } from "vitest";
import { discoverRoutes } from "../core/manifest.js";
import { writeRouteTable } from "../core/route-table.js";
import { generateServerBundleEntry, SERVER_BUNDLE_MANIFEST } from "./server-bundle.js";

/**
 * A prebuilt app (server bundle + route table, as `neutron-ts build` writes
 * them) boots and serves without loading build-time code: no Vite, no MDX
 * or frontmatter toolchain, no source parsers. Run in a fresh Node process
 * whose load hook records every module, against the built dist (as an
 * installed app sees core). Build before test, as CI does.
 */

const FORBIDDEN = ["vite", "@mdx-js/mdx", "gray-matter", "@babel/parser", "acorn", "esprima", "micromark", "magic-string"];

const FILES: Record<string, string> = {
  "src/routes/_layout.js": `
import { h } from "preact";
export default function Layout({ children }) { return h("main", null, children); }
`,
  "src/routes/index.js": `
import { h } from "preact";
import { useLoaderData } from "@neutron-build/core";
export const config = { mode: "app" };
export async function loader() { return { greeting: "hello-from-loader" }; }
export default function Home() { return h("p", null, useLoaderData().greeting); }
`,
  "src/routes/about.js": `
import { h } from "preact";
export default function About() { return h("p", null, "about"); }
`,
  "dist/about/index.html": "<!doctype html><html><body><p>about (prebuilt)</p></body></html>",
};

const CHILD = `
import { registerHooks } from "node:module";
const urls = new Set();
registerHooks({ load(url, context, next) { urls.add(url); return next(url, context); } });
const root = process.argv[2];
const { createServer } = await import("@neutron-build/core/server");
const server = await createServer({ rootDir: root, host: "127.0.0.1", port: 0 });
const home = await fetch(server.url + "/");
const about = await fetch(server.url + "/about");
const result = {
  home: { status: home.status, body: await home.text() },
  about: { status: about.status, body: await about.text() },
  moduleSource: server.moduleSource,
  urls: [...urls],
};
await server.close();
process.stdout.write(JSON.stringify(result));
`;

let root = "";
afterAll(async () => {
  if (root) await fs.rm(root, { recursive: true, force: true });
});

function packageOf(url: string): string | null {
  const match = /.*\/node_modules\/((?:@[^/]+\/)?[^/]+)\//.exec(url);
  return match ? match[1] : null;
}

describe("production server module graph", () => {
  it("serves a prebuilt app without loading build-time code", { timeout: 60_000 }, async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "neutron-production-imports-"));
    const ownModules = path.join(process.cwd(), "node_modules");
    await fs.mkdir(path.join(root, "node_modules", "@neutron-build"), { recursive: true });
    for (const entry of await fs.readdir(ownModules)) {
      if (entry.startsWith(".") || entry === "@neutron-build") continue;
      await fs.symlink(path.join(ownModules, entry), path.join(root, "node_modules", entry), "dir");
    }
    await fs.symlink(process.cwd(), path.join(root, "node_modules", "@neutron-build", "core"), "dir");
    for (const [name, source] of Object.entries(FILES)) {
      await fs.mkdir(path.dirname(path.join(root, name)), { recursive: true });
      await fs.writeFile(path.join(root, name), source, "utf-8");
    }
    const routesDir = path.join(root, "src", "routes");
    const distDir = path.join(root, "dist");
    const routes = discoverRoutes({ routesDir });
    const entryPath = path.join(distDir, "server", "start", "entry.js");
    await fs.mkdir(path.dirname(entryPath), { recursive: true });
    await fs.writeFile(
      entryPath,
      generateServerBundleEntry({ entryPath, rootDir: root, files: routes.map((route) => route.file), runtime: "preact" }),
      "utf-8"
    );
    await fs.writeFile(
      path.join(distDir, SERVER_BUNDLE_MANIFEST),
      JSON.stringify({ format: 1, entry: "server/start/entry.js", runtime: "preact" }),
      "utf-8"
    );
    expect(writeRouteTable(root, routesDir, distDir, routes)).toBe(true);
    const childPath = path.join(root, "child.mjs");
    await fs.writeFile(childPath, CHILD, "utf-8");

    const { stdout } = await promisify(execFile)(process.execPath, [childPath, root], { cwd: root });
    const result = JSON.parse(stdout) as {
      home: { status: number; body: string };
      about: { status: number; body: string };
      moduleSource: string | null;
      urls: string[];
    };

    expect(result.home.status).toBe(200);
    expect(result.home.body).toContain("hello-from-loader");
    expect(result.about.status).toBe(200);
    expect(result.about.body).toContain("about (prebuilt)");
    expect(result.moduleSource).toBe("server-bundle");

    const packages = new Set(result.urls.map(packageOf).filter((name): name is string => name !== null));
    expect([...packages].filter((name) => FORBIDDEN.includes(name) || name.startsWith("micromark"))).toEqual([]);
    const coreDist = result.urls.filter((url) => url.includes("/packages/neutron/dist/"));
    expect(coreDist.length).toBeGreaterThan(0);
    // The dev server's plugin graph. (core's entry still links the light
    // build helpers it exports, such as discoverRoutes, without their parsers.)
    expect(
      coreDist.filter((url) => /\/dist\/vite\/(?:plugin|public-env|server-only|server-actions|ssr-cjs)\.js$/.test(url))
    ).toEqual([]);
  });
});
