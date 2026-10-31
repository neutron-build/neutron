import { execFileSync } from "node:child_process";
import * as fs from "node:fs";
import { createRequire } from "node:module";
import * as net from "node:net";
import * as path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { compileMounts } from "../core/mount-dispatch.js";
import { createServer } from "./index.js";

/**
 * F-P6-4: an app whose `hono` is not core's copy (a linked framework, or an
 * app pinned to another 4.x) must mount its sub-apps without a cast, and
 * they must work.
 *
 * The fixture is a second copy of hono on disk — what an app's own install
 * is to TypeScript and to Node: a different declaration of `class Hono`
 * (whose `#private` members make two copies nominally incompatible) and a
 * different module instance. The type half compiles against core's built
 * dist declarations, so it needs `pnpm build` first, like the other
 * dist-resolving type tests.
 */
const require = createRequire(import.meta.url);
const coreHonoRoot = (() => {
  // hono's exports map hides package.json: walk up from its entry instead.
  let dir = path.dirname(fs.realpathSync(require.resolve("hono")));
  while (!fs.existsSync(path.join(dir, "package.json")) || JSON.parse(fs.readFileSync(path.join(dir, "package.json"), "utf-8")).name !== "hono") {
    dir = path.dirname(dir);
  }
  return dir;
})();
let fixtureRoot = "";
let foreignHonoRoot = "";

beforeAll(() => {
  fixtureRoot = fs.mkdtempSync(path.join(process.cwd(), ".tmp-foreign-hono-"));
  foreignHonoRoot = path.join(fixtureRoot, "node_modules", "hono");
  fs.mkdirSync(foreignHonoRoot, { recursive: true });
  // Another version, as an app's own install usually is (Fylun: 4.13 against
  // core's 4.12). TypeScript folds two copies of one name@version into one,
  // so an identical version would not reproduce the split.
  const pkg = JSON.parse(fs.readFileSync(path.join(coreHonoRoot, "package.json"), "utf-8"));
  fs.writeFileSync(path.join(foreignHonoRoot, "package.json"), JSON.stringify({ ...pkg, version: "4.999.0-foreign" }), "utf-8");
  // ESM build + declarations; the CommonJS build is not needed.
  fs.cpSync(path.join(coreHonoRoot, "dist"), path.join(foreignHonoRoot, "dist"), {
    recursive: true,
    filter: (source) => !source.startsWith(path.join(coreHonoRoot, "dist", "cjs")),
  });
});

afterAll(() => {
  if (fixtureRoot) fs.rmSync(fixtureRoot, { recursive: true, force: true });
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
      socket.close((error) => (error ? reject(error) : resolve(address.port)));
    });
    socket.on("error", reject);
  });
}

describe("mount sub-apps built with another copy of hono (F-P6-4)", () => {
  it("type-checks without a cast to NeutronMountApp", () => {
    const distServer = path.resolve(__dirname, "../../dist/server/index.d.ts");
    fs.writeFileSync(
      path.join(fixtureRoot, "usage.ts"),
      `import { Hono } from "hono";
import { createServer, defineServerEntry, type NeutronMount, type NeutronMountApp } from "@neutron-build/core/server";

const api = new Hono().get("/ping", (c) => c.text("pong"));
const typed = new Hono<{ Variables: { user: string } }>().basePath("/v1");

const app: NeutronMountApp = api;
const entry: NeutronMount = { app: typed, globalMiddleware: false };
export const server = createServer({ mode: "api", mount: { "/api": api, "/typed": entry, "/app": app } });
export default defineServerEntry({ mount: { "/api": api, "/typed": { app: typed } } });

// @ts-expect-error a bare fetch handler is not a mount: Neutron routes the sub-app.
export const handlerOnly: NeutronMountApp = { fetch: () => new Response("x") };
`,
      "utf-8"
    );
    fs.writeFileSync(
      path.join(fixtureRoot, "tsconfig.json"),
      JSON.stringify({
        compilerOptions: {
          target: "ES2022",
          module: "ESNext",
          moduleResolution: "bundler",
          lib: ["ES2022", "DOM", "DOM.Iterable"],
          strict: true,
          skipLibCheck: true,
          noEmit: true,
          types: [],
          paths: { "@neutron-build/core/server": [distServer] },
        },
        files: ["usage.ts"],
      }),
      "utf-8"
    );
    const tsc = require.resolve("typescript/lib/tsc.js");
    try {
      execFileSync(process.execPath, [tsc, "--project", path.join(fixtureRoot, "tsconfig.json"), "--pretty", "false"], {
        cwd: fixtureRoot,
        stdio: "pipe",
        encoding: "utf8",
      });
    } catch (error) {
      const err = error as { stdout?: string; stderr?: string; message?: string };
      expect.fail(`foreign-hono mount fixture failed to compile:\n${err.stdout ?? ""}${err.stderr ?? ""}${err.message ?? ""}`);
    }
  }, 60_000);

  it("serves a foreign-hono sub-app from createServer and the adapter runtime", async () => {
    const foreign = (await import(/* @vite-ignore */ path.join(foreignHonoRoot, "dist", "index.js"))) as typeof import("hono");
    const { Hono: CoreHono } = await import("hono");
    expect(foreign.Hono).not.toBe(CoreHono);

    const subApp = new foreign.Hono();
    subApp.get("/ping", (c) => c.json({ pong: true }));
    subApp.onError((error, c) => c.json({ caught: error.message }, 418));
    subApp.get("/boom", () => {
      throw new Error("from the sub-app");
    });

    const port = await getFreePort();
    const server = await createServer({ host: "127.0.0.1", port, mode: "api", compress: false, mount: { "/api": subApp } });
    try {
      const ping = await fetch(`http://127.0.0.1:${port}/api/ping`);
      expect(ping.status).toBe(200);
      expect(await ping.json()).toEqual({ pong: true });
      // The sub-app's own error handler still answers its errors.
      const boom = await fetch(`http://127.0.0.1:${port}/api/boom`);
      expect(boom.status).toBe(418);
      expect(await boom.json()).toEqual({ caught: "from the sub-app" });
    } finally {
      await server.close();
    }

    const [mount] = compileMounts({ "/api": subApp });
    const answer = await mount.dispatch(new Request("http://x/api/ping"));
    expect(answer?.status).toBe(200);
    expect(await mount.dispatch(new Request("http://x/api/missing"))).toBeNull();
  });
});
