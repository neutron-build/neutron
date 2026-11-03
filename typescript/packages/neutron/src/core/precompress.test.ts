import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { brotliDecompressSync, gunzipSync } from "node:zlib";
import { afterEach, describe, expect, it } from "vitest";
import { precompressBuildOutput } from "./precompress.js";

let dir = "";
afterEach(() => {
  if (dir) fs.rmSync(dir, { recursive: true, force: true });
  dir = "";
});

function write(relative: string, content: string | Buffer): string {
  const file = path.join(dir, relative);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, content);
  return file;
}

const big = (label: string) => `<p>${label}</p>`.repeat(400);

describe("precompressBuildOutput", () => {
  it("writes br + gz siblings for served text outputs only", () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), "neutron-precompress-"));
    const html = write("about/index.html", big("about"));
    const js = write("assets/app-abc.js", `console.log(${JSON.stringify(big("js"))});`);
    write("assets/tiny.css", "a{}");
    write("assets/logo.png", Buffer.alloc(4096, 1));
    write("server/start/entry.js", big("server"));
    write(".vite/manifest.json", JSON.stringify({ a: big("manifest") }));
    write("assets/node_modules/x.js", big("dep"));

    const summary = precompressBuildOutput(dir);
    expect(summary.files).toBe(2);

    for (const file of [html, js]) {
      const source = fs.readFileSync(file);
      expect(brotliDecompressSync(fs.readFileSync(`${file}.br`)).equals(source)).toBe(true);
      expect(gunzipSync(fs.readFileSync(`${file}.gz`)).equals(source)).toBe(true);
    }
    for (const skipped of [
      "assets/tiny.css",
      "assets/logo.png",
      "server/start/entry.js",
      ".vite/manifest.json",
      "assets/node_modules/x.js",
    ]) {
      expect(fs.existsSync(path.join(dir, `${skipped}.br`)), skipped).toBe(false);
      expect(fs.existsSync(path.join(dir, `${skipped}.gz`)), skipped).toBe(false);
    }
  });

  it("keeps current siblings and replaces stale ones", () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), "neutron-precompress-"));
    const html = write("index.html", big("v1"));
    precompressBuildOutput(dir);
    const firstMtime = fs.statSync(`${html}.br`).mtimeMs;

    precompressBuildOutput(dir);
    expect(fs.statSync(`${html}.br`).mtimeMs).toBe(firstMtime);

    const future = new Date(Date.now() + 5_000);
    fs.writeFileSync(html, big("v2"));
    fs.utimesSync(html, future, future);
    precompressBuildOutput(dir);
    expect(brotliDecompressSync(fs.readFileSync(`${html}.br`)).toString()).toBe(big("v2"));
  });
});
