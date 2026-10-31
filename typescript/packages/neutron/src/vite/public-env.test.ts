import * as fs from "node:fs/promises";
import * as path from "node:path";
import { afterAll, afterEach, beforeEach, describe, expect, it } from "vitest";
import { build, createServer, type Rollup } from "vite";
import { neutronPublicEnvPlugin, resolvePublicEnvPrefixes } from "./public-env.js";

/**
 * Next apps read public configuration as `process.env.NEXT_PUBLIC_*`. Vite
 * exposes only `import.meta.env.VITE_*` to client code and turns the rest of
 * `process.env` into `{}` in client builds, so every such read was empty in a
 * Neutron client build (and threw in dev). `env.publicPrefixes` inlines
 * prefixed variables, both spellings, in client and server code; nothing
 * without a prefix may reach the output.
 */

const roots: string[] = [];
const saved = { ...process.env };

beforeEach(() => {
  process.env.NEXT_PUBLIC_APP_URL = "https://public.example";
  process.env.PUBLIC_FLAG = "public-flag";
  process.env.FYLUN_SECRET_KEY = "secret-value-must-not-ship";
  delete process.env.NEXT_PUBLIC_UNSET;
});

afterEach(() => {
  for (const key of ["NEXT_PUBLIC_APP_URL", "PUBLIC_FLAG", "FYLUN_SECRET_KEY"]) {
    if (saved[key] === undefined) delete process.env[key];
    else process.env[key] = saved[key];
  }
});

afterAll(async () => {
  for (const root of roots) await fs.rm(root, { recursive: true, force: true });
});

const ENTRY = `
export const appUrl = process.env.NEXT_PUBLIC_APP_URL;
export const metaUrl = import.meta.env.NEXT_PUBLIC_APP_URL;
export const flag = process.env.PUBLIC_FLAG;
export const unset = process.env.NEXT_PUBLIC_UNSET ?? "fallback";
export const label = "process.env.NEXT_PUBLIC_APP_URL";
export function secret() {
  return typeof process === "undefined" ? "no-process" : process.env.FYLUN_SECRET_KEY;
}
export const mode = process.env.NODE_ENV;
export const whole = process.env;
`;

async function makeRoot(): Promise<string> {
  const root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-public-env-"));
  roots.push(root);
  await fs.writeFile(path.join(root, "entry.ts"), ENTRY, "utf-8");
  await fs.writeFile(path.join(root, ".env"), "NEXT_PUBLIC_FROM_FILE=from-file\nFILE_SECRET=file-secret\n", "utf-8");
  return root;
}

const PREFIXES = { publicPrefixes: ["PUBLIC_", "NEXT_PUBLIC_"] };

async function buildChunk(root: string, ssr: boolean): Promise<string> {
  const output = (await build({
    configFile: false,
    root,
    logLevel: "silent",
    plugins: [neutronPublicEnvPlugin(PREFIXES)],
    build: {
      write: false,
      minify: false,
      ...(ssr
        ? { ssr: path.join(root, "entry.ts") }
        : { lib: false, rollupOptions: { input: path.join(root, "entry.ts"), preserveEntrySignatures: "strict" } }),
    },
  })) as Rollup.RollupOutput | Rollup.RollupOutput[];
  const outputs = Array.isArray(output) ? output : [output];
  return outputs
    .flatMap((o) => o.output)
    .map((chunk) => (chunk.type === "chunk" ? chunk.code : String(chunk.source)))
    .join("\n");
}

describe("env.publicPrefixes", () => {
  it("rejects an empty prefix and defaults to PUBLIC_", () => {
    expect(resolvePublicEnvPrefixes()).toEqual(["PUBLIC_"]);
    expect(() => resolvePublicEnvPrefixes({ publicPrefixes: [""] })).toThrow(/non-empty/);
  });

  it("client build inlines prefixed variables and leaves secrets out", { timeout: 60_000 }, async () => {
    const code = await buildChunk(await makeRoot(), false);
    expect(code).toContain('"https://public.example"');
    expect(code).toContain('"public-flag"');
    expect(code).not.toContain("process.env.NEXT_PUBLIC_APP_URL;");
    // The string literal is not an expression and stays as written.
    expect(code).toContain('"process.env.NEXT_PUBLIC_APP_URL"');
    expect(code).not.toContain("secret-value-must-not-ship");
    expect(code).not.toContain("file-secret");
  });

  it("server build inlines prefixed variables and keeps secrets as runtime reads", { timeout: 60_000 }, async () => {
    const code = await buildChunk(await makeRoot(), true);
    expect(code).toContain('"https://public.example"');
    expect(code).toContain("process.env.FYLUN_SECRET_KEY");
    expect(code).not.toContain("secret-value-must-not-ship");
  });

  it("dev: client code reads what the build reads; server code keeps runtime reads", { timeout: 60_000 }, async () => {
    const root = await makeRoot();
    const server = await createServer({
      configFile: false,
      root,
      logLevel: "silent",
      plugins: [neutronPublicEnvPlugin(PREFIXES)],
      server: { middlewareMode: true, hmr: false, ws: false },
    });
    try {
      const client = await server.environments.client.transformRequest("/entry.ts");
      const code = client!.code;
      expect(code).toContain('appUrl = "https://public.example"');
      expect(code).toContain('flag = "public-flag"');
      // As in a client build: the rest of process.env is {}, NODE_ENV inlined.
      expect(code).toMatch(/unset = \{\}\.NEXT_PUBLIC_UNSET \?\? "fallback"/);
      expect(code).toContain("{}.FYLUN_SECRET_KEY");
      expect(code).toMatch(/mode = "(development|test)"/);
      expect(code).toContain("whole = {}");
      expect(code).toContain('"process.env.NEXT_PUBLIC_APP_URL"');
      expect(code).not.toContain("secret-value-must-not-ship");
      expect(server.config.env.NEXT_PUBLIC_FROM_FILE).toBe("from-file");
      expect(server.config.env.FILE_SECRET).toBeUndefined();

      const mod = (await server.ssrLoadModule("/entry.ts")) as Record<string, unknown> & { secret: () => string };
      expect(mod.appUrl).toBe("https://public.example");
      expect(mod.metaUrl).toBe("https://public.example");
      expect(mod.unset).toBe("fallback");
      // Server code still reads the real environment at runtime.
      expect(mod.secret()).toBe("secret-value-must-not-ship");
    } finally {
      await server.close();
    }
  });
});
