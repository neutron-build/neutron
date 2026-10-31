import * as fs from "node:fs/promises";
import * as http from "node:http";
import * as net from "node:net";
import * as path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { createServer as createViteServer } from "vite";
import { createServer, type NeutronServer } from "./index.js";
import { neutronPlugin } from "../vite/plugin.js";
import { resolvePreactSsr, vitePreactAliases } from "../core/preact-ssr.js";
import { DOCUMENT_HEAD_FILE, extractDocumentHead, serializeDocumentHead } from "../core/document-head.js";
import { decodeSerializedPayload } from "../core/serialization.js";

/**
 * F-P6-1 over HTTP: server-rendered documents carry the app's index.html
 * head under the route chain's head() — `start` (streamed render, reading
 * the head `neutron-ts build` records in dist) and dev (reading index.html)
 * — and the client-navigation payload's `__head__` is the same head.
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

const INDEX_HTML = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1, interactive-widget=resizes-content" />
    <meta name="theme-color" media="(prefers-color-scheme: light)" content="white" />
    <meta name="theme-color" media="(prefers-color-scheme: dark)" content="#0a0a0a" />
    <meta property="og:site_name" content="Fylun" />
    <title>Fylun</title>
  </head>
  <body><div id="app"></div></body>
</html>`;

const FILES: Record<string, string> = {
  "index.html": INDEX_HTML,
  "src/routes/_layout.tsx": `
import { h } from "preact";
export function head() {
  return { titleTemplate: "%s | Fylun", verification: { google: "g-token" }, robots: "index, follow" };
}
export default function Layout({ children }) { return h("main", null, children); }
`,
  "src/routes/index.tsx": `
import { h } from "preact";
export const config = { mode: "app" };
export default function Home() { return h("p", null, "home"); }
`,
  "src/routes/settings.tsx": `
import { h } from "preact";
export const config = { mode: "app" };
export function head() {
  return {
    title: "Settings",
    noindex: true,
    themeColor: "#111111",
    meta: [{ name: "format-detection", content: "telephone=no" }],
  };
}
export default function Settings() { return h("p", null, "settings"); }
`,
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
  root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-document-head-"));
  for (const [file, source] of Object.entries(FILES)) {
    await fs.mkdir(path.dirname(path.join(root, file)), { recursive: true });
    await fs.writeFile(path.join(root, file), source, "utf-8");
  }
}

async function bootStart(): Promise<string> {
  await writeApp();
  // What `neutron-ts build` records; start has no index.html to read.
  await fs.mkdir(path.join(root, "dist"), { recursive: true });
  await fs.writeFile(path.join(root, "dist", DOCUMENT_HEAD_FILE), serializeDocumentHead(extractDocumentHead(INDEX_HTML)));
  await fs.rm(path.join(root, "index.html"));
  const port = await getFreePort();
  const running: NeutronServer = await createServer({ host: "127.0.0.1", port, rootDir: root, compress: false });
  closers.push(() => running.close());
  return `http://127.0.0.1:${port}`;
}

async function bootDev(): Promise<string> {
  await writeApp();
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

const headOf = (html: string) => /<head>([\s\S]*?)<\/head>/.exec(html)?.[1] ?? "";
const count = (html: string, pattern: RegExp) => html.match(pattern)?.length ?? 0;

for (const [label, boot] of [["start", bootStart], ["dev", bootDev]] as const) {
  describe(`index.html head in server-rendered documents — ${label}`, () => {
    it("carries index.html's tags under the route chain's", { timeout: 60_000 }, async () => {
      const base = await boot();

      const home = headOf(await (await fetch(`${base}/`, { headers: { accept: "text/html" } })).text());
      expect(count(home, /name="viewport"/g)).toBe(1);
      expect(home).toContain("interactive-widget=resizes-content");
      expect(count(home, /charset=/gi)).toBe(1);
      expect(count(home, /name="theme-color"/g)).toBe(2);
      expect(home).toContain('media="(prefers-color-scheme: dark)" content="#0a0a0a"');
      expect(home).toContain('<meta property="og:site_name" content="Fylun" />');
      expect(home).toContain('<meta name="google-site-verification" content="g-token">');
      expect(home).toContain('<meta name="robots" content="index, follow">');
      expect(count(home, /<title>/g)).toBe(1);
      expect(home).toContain("<title>Fylun</title>");

      const settingsHtml = await (await fetch(`${base}/settings`, { headers: { accept: "text/html" } })).text();
      const settings = headOf(settingsHtml);
      expect(count(settings, /<title>/g)).toBe(1);
      expect(settings).toContain("<title>Settings | Fylun</title>");
      expect(count(settings, /name="theme-color"/g)).toBe(1);
      expect(settings).toContain('<meta name="theme-color" content="#111111">');
      // The page's robots replaces the layout's; noindex is what was asked.
      expect(count(settings, /name="robots"/g)).toBe(1);
      expect(settings).toContain('<meta name="robots" content="noindex">');
      expect(settings).toContain('<meta name="format-detection" content="telephone=no">');

      // Client navigation gets the same head.
      const data = await fetch(`${base}/settings`, { headers: { accept: "application/json", "x-neutron-data": "true" } });
      const payload = decodeSerializedPayload<Record<string, unknown>>(await data.json());
      const navHead = String(payload.__head__);
      expect(navHead).toContain("interactive-widget=resizes-content");
      expect(navHead).toContain('<meta name="theme-color" content="#111111">');
      expect(navHead).toContain('<meta name="robots" content="noindex">');
      expect(navHead).toContain('<meta property="og:site_name" content="Fylun" />');
    });
  });
}
