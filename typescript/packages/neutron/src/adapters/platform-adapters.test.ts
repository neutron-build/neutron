import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { adapterCloudflare } from "./cloudflare.js";
import { adapterDocker } from "./docker.js";
import { adapterNetlify } from "./netlify.js";
import { adapterStatic } from "./static.js";
import { adapterVercel } from "./vercel.js";

const tempDirs: string[] = [];

afterEach(() => {
  for (const dir of tempDirs.splice(0)) {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

function createTempOutDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "neutron-adapter-"));
  tempDirs.push(dir);
  return dir;
}

function writeStaticHeaders(
  outDir: string,
  headers: Record<string, Record<string, string>>
): void {
  fs.writeFileSync(
    path.join(outDir, ".neutron-static-headers.json"),
    JSON.stringify(headers, null, 2),
    "utf-8"
  );
}

describe("platform adapters", () => {
  it("cloudflare adapter writes _headers and metadata", async () => {
    const outDir = createTempOutDir();
    writeStaticHeaders(outDir, {
      "/": { "Cache-Control": "public, max-age=60" },
      "/about": { "X-Test": "yes" },
    });

    const adapter = adapterCloudflare();
    await adapter.adapt({
      rootDir: outDir,
      outDir,
      routes: { total: 2, static: 2, app: 0 },
      log: () => {},
    });

    const headersPath = path.join(outDir, "_headers");
    const metadataPath = path.join(outDir, ".neutron-adapter-cloudflare.json");
    expect(fs.existsSync(headersPath)).toBe(true);
    expect(fs.existsSync(metadataPath)).toBe(true);

    const headersText = fs.readFileSync(headersPath, "utf-8");
    expect(headersText).toContain("/");
    expect(headersText).toContain("Cache-Control: public, max-age=60");
    expect(headersText).toContain("/about");
  });

  it("cloudflare workers mode writes worker and wrangler config", async () => {
    const outDir = createTempOutDir();

    const adapter = adapterCloudflare({ mode: "workers", allowAppRoutes: true });
    await adapter.adapt({
      rootDir: outDir,
      outDir,
      routes: { total: 1, static: 0, app: 1 },
      ensureRuntimeBundle: async () => ({
        target: "worker",
        outDir,
        entryPath: path.join(outDir, "server/worker/entry.js"),
        entryRelativePath: "server/worker/entry.js",
      }),
      log: () => {},
    });

    expect(fs.existsSync(path.join(outDir, "_worker.js"))).toBe(true);
    expect(fs.existsSync(path.join(outDir, "wrangler.json"))).toBe(true);
    const wrangler = JSON.parse(
      fs.readFileSync(path.join(outDir, "wrangler.json"), "utf-8")
    ) as { compatibility_flags?: string[] };
    expect(wrangler.compatibility_flags).toContain("nodejs_compat");
  });

  it("cloudflare adapter throws for app routes by default", async () => {
    const outDir = createTempOutDir();
    const adapter = adapterCloudflare();
    await expect(
      adapter.adapt({
        rootDir: outDir,
        outDir,
        routes: { total: 1, static: 0, app: 1 },
        log: () => {},
      })
    ).rejects.toThrow(/runtime bundle support/i);
  });

  it("vercel adapter writes vercel config and metadata", async () => {
    const outDir = createTempOutDir();
    writeStaticHeaders(outDir, {
      "/about": { "X-Test": "1" },
    });

    const adapter = adapterVercel();
    await adapter.adapt({
      rootDir: outDir,
      outDir,
      routes: { total: 1, static: 1, app: 0 },
      log: () => {},
    });

    const vercelConfigPath = path.join(outDir, "vercel.json");
    const metadataPath = path.join(outDir, ".neutron-adapter-vercel.json");
    expect(fs.existsSync(vercelConfigPath)).toBe(true);
    expect(fs.existsSync(metadataPath)).toBe(true);

    const vercelConfig = JSON.parse(fs.readFileSync(vercelConfigPath, "utf-8")) as {
      headers?: Array<{ source: string }>;
      cleanUrls?: boolean;
    };
    expect(vercelConfig.cleanUrls).toBe(true);
    expect(vercelConfig.headers?.[0]?.source).toBe("/about");
  });

  it("vercel adapter throws for app routes by default", async () => {
    const outDir = createTempOutDir();
    const adapter = adapterVercel();
    await expect(
      adapter.adapt({
        rootDir: outDir,
        outDir,
        routes: { total: 1, static: 0, app: 1 },
        log: () => {},
      })
    ).rejects.toThrow(/runtime bundle support/i);
  });

  it("vercel adapter writes api handler for app routes", async () => {
    const outDir = createTempOutDir();
    const adapter = adapterVercel();
    await adapter.adapt({
      rootDir: outDir,
      outDir,
      routes: { total: 1, static: 0, app: 1 },
      ensureRuntimeBundle: async () => ({
        target: "node",
        outDir: path.join(outDir, "server/node"),
        entryPath: path.join(outDir, "server/node/entry.js"),
        entryRelativePath: "server/node/entry.js",
      }),
      log: () => {},
    });

    expect(fs.existsSync(path.join(outDir, "api/__neutron.mjs"))).toBe(true);
  });

  it("netlify adapter writes netlify config, headers and metadata for static routes", async () => {
    const outDir = createTempOutDir();
    writeStaticHeaders(outDir, {
      "/about": { "X-Test": "1" },
    });

    const adapter = adapterNetlify();
    await adapter.adapt({
      rootDir: outDir,
      outDir,
      routes: { total: 1, static: 1, app: 0 },
      log: () => {},
    });

    const netlifyConfigPath = path.join(outDir, "netlify.toml");
    const metadataPath = path.join(outDir, ".neutron-adapter-netlify.json");
    expect(fs.existsSync(netlifyConfigPath)).toBe(true);
    expect(fs.existsSync(metadataPath)).toBe(true);
    expect(fs.existsSync(path.join(outDir, "_headers"))).toBe(true);
    expect(fs.existsSync(path.join(outDir, "_redirects"))).toBe(false);
    expect(fs.existsSync(path.join(outDir, "functions"))).toBe(false);

    const netlifyConfig = fs.readFileSync(netlifyConfigPath, "utf-8");
    expect(netlifyConfig).toContain('publish = "');
    expect(netlifyConfig).not.toContain("[[redirects]]");
  });

  it("netlify adapter throws for app routes by default", async () => {
    const outDir = createTempOutDir();
    const adapter = adapterNetlify();
    await expect(
      adapter.adapt({
        rootDir: outDir,
        outDir,
        routes: { total: 1, static: 0, app: 1 },
        log: () => {},
      })
    ).rejects.toThrow(/runtime bundle support/i);
  });

  it("netlify adapter writes function and redirects for app routes", async () => {
    const outDir = createTempOutDir();
    const adapter = adapterNetlify();
    await adapter.adapt({
      rootDir: outDir,
      outDir,
      routes: { total: 1, static: 0, app: 1 },
      ensureRuntimeBundle: async () => ({
        target: "node",
        outDir: path.join(outDir, "server/node"),
        entryPath: path.join(outDir, "server/node/entry.js"),
        entryRelativePath: "server/node/entry.js",
      }),
      log: () => {},
    });

    const functionPath = path.join(outDir, "functions/__neutron.mjs");
    expect(fs.existsSync(functionPath)).toBe(true);

    const functionSource = fs.readFileSync(functionPath, "utf-8");
    expect(functionSource).toContain('from "../server/node/entry.js"');
    expect(/export\s+default/.test(functionSource)).toBe(true);

    const redirects = fs.readFileSync(path.join(outDir, "_redirects"), "utf-8");
    expect(redirects).toContain("/*  /.netlify/functions/__neutron  200");

    const netlifyConfig = fs.readFileSync(path.join(outDir, "netlify.toml"), "utf-8");
    expect(netlifyConfig).toContain("[[redirects]]");
    expect(netlifyConfig).toContain('to = "/.netlify/functions/__neutron"');
  });

  it("docker adapter writes docker deploy files", async () => {
    const outDir = createTempOutDir();
    const adapter = adapterDocker();
    await adapter.adapt({
      rootDir: outDir,
      outDir,
      routes: { total: 3, static: 2, app: 1 },
      ensureRuntimeBundle: async () => ({
        target: "node",
        outDir: path.join(outDir, "server/node"),
        entryPath: path.join(outDir, "server/node/entry.js"),
        entryRelativePath: "server/node/entry.js",
      }),
      log: () => {},
    });

    expect(fs.existsSync(path.join(outDir, "Dockerfile"))).toBe(true);
    expect(fs.existsSync(path.join(outDir, ".dockerignore"))).toBe(true);
    expect(fs.existsSync(path.join(outDir, "server.mjs"))).toBe(true);
    expect(fs.existsSync(path.join(outDir, ".neutron-adapter-docker.json"))).toBe(true);
  });

  it("static adapter writes headers policy and precompressed files", async () => {
    const outDir = createTempOutDir();
    fs.mkdirSync(path.join(outDir, "assets"), { recursive: true });
    fs.mkdirSync(path.join(outDir, "blog"), { recursive: true });
    fs.writeFileSync(path.join(outDir, "index.html"), "<html><body>home</body></html>", "utf-8");
    fs.writeFileSync(path.join(outDir, "blog/index.html"), "<html><body>blog</body></html>", "utf-8");
    fs.writeFileSync(
      path.join(outDir, "assets/app.js"),
      `export const value = "${"x".repeat(4000)}";`,
      "utf-8"
    );

    const adapter = adapterStatic();
    await adapter.adapt({
      rootDir: outDir,
      outDir,
      routes: { total: 2, static: 2, app: 0 },
      log: () => {},
    });

    const headersText = fs.readFileSync(path.join(outDir, "_headers"), "utf-8");
    expect(headersText).toContain("/assets/*");
    expect(headersText).toContain("/blog");
    expect(headersText).toContain("must-revalidate");
    // Default security headers are emitted for every route.
    expect(headersText).toContain("/*");
    expect(headersText).toContain("X-Content-Type-Options: nosniff");
    expect(headersText).toContain("X-Frame-Options: DENY");

    expect(fs.existsSync(path.join(outDir, "assets/app.js.gz"))).toBe(true);
    expect(fs.existsSync(path.join(outDir, "assets/app.js.br"))).toBe(true);
    expect(fs.existsSync(path.join(outDir, ".neutron-static-policy.json"))).toBe(true);
    expect(fs.existsSync(path.join(outDir, ".neutron-adapter-static.json"))).toBe(true);
  });
});
