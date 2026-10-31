import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as net from "node:net";
import { afterAll, describe, expect, it } from "vitest";
import { createServer as createViteServer, type Plugin } from "vite";
import { neutronPlugin } from "./plugin.js";
import { resolvePreactSsr, vitePreactAliases } from "../core/preact-ssr.js";
import { decodeSerializedPayload } from "../core/serialization.js";

/**
 * Dev linked a previous build's minified stylesheet (dist/assets/*.css) on a
 * page (the Fylun port: /pricing, /studio). Tailwind v4 registers every
 * source file it scans with `addWatchFile`, and where dist/ is not gitignored
 * it scans the build output; Vite records each such file as a file-only
 * "asset" entry under the stylesheet that registered it, and dev walked those
 * entries as if they were CSS imports. The plugin below does what Tailwind's
 * does, for one file.
 */

const closers: Array<() => Promise<void>> = [];
const roots: string[] = [];

afterAll(async () => {
  await Promise.all(closers.map((close) => close()));
  for (const root of roots) await fs.rm(root, { recursive: true, force: true });
});

async function getFreePort(): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    const socket = net.createServer();
    socket.listen(0, "127.0.0.1", () => {
      const address = socket.address();
      if (!address || typeof address === "string") return reject(new Error("no port"));
      const { port } = address;
      socket.close((error) => (error ? reject(error) : resolve(port)));
    });
    socket.on("error", reject);
  });
}

function watchesBuildOutput(staleFile: string): Plugin {
  return {
    name: "test:tailwind-like-source-scan",
    transform(_code, id) {
      if (id.split("?")[0].endsWith("/src/styles/studio.css")) {
        this.addWatchFile(staleFile);
      }
      return null;
    },
  };
}

describe("dev stylesheets", () => {
  it("never links build output, even when a CSS plugin watches it", { timeout: 60_000 }, async () => {
    const root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-dev-css-"));
    roots.push(root);
    const files: Record<string, string> = {
      "src/styles/studio.css": ".studio{color:#ff0000}\n",
      "src/routes/studio.tsx": `
import { h } from "preact";
import "../styles/studio.css";
export const config = { mode: "app" };
export default function Studio() { return h("p", { class: "studio" }, "studio"); }
`,
      // A previous build's output.
      "dist/assets/studio-OLDHASH.css": ".studio{color:red}",
    };
    for (const [file, body] of Object.entries(files)) {
      await fs.mkdir(path.dirname(path.join(root, file)), { recursive: true });
      await fs.writeFile(path.join(root, file), body, "utf-8");
    }

    const port = await getFreePort();
    const preactSsr = resolvePreactSsr(root);
    const vite = await createViteServer({
      configFile: false,
      root,
      logLevel: "silent",
      plugins: [
        neutronPlugin({ routesDir: path.join(root, "src", "routes"), rootDir: root }),
        watchesBuildOutput(path.join(root, "dist", "assets", "studio-OLDHASH.css")),
      ],
      resolve: { alias: vitePreactAliases(preactSsr) },
      ssr: { noExternal: preactSsr.noExternal },
      server: { host: "127.0.0.1", port, strictPort: true, hmr: false },
    });
    await vite.listen();
    closers.push(() => vite.close());
    const base = `http://127.0.0.1:${port}`;

    const html = await (await fetch(`${base}/studio`, { headers: { Accept: "text/html" } })).text();
    const links = [...html.matchAll(/<link rel="stylesheet" href="([^"]+)"/g)].map((match) => match[1]);
    expect(links).toEqual(["/src/styles/studio.css?direct"]);

    // The client navigation's list (what the router loads before rendering).
    const data = await (
      await fetch(`${base}/studio`, { headers: { Accept: "application/json", "X-Neutron-Data": "true" } })
    ).json();
    expect((decodeSerializedPayload(data) as { __css__: string[] }).__css__).toEqual(["/src/styles/studio.css"]);
    expect(html + JSON.stringify(data)).not.toContain("OLDHASH");
  });
});
