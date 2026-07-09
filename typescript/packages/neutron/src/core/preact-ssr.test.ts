import { describe, it, expect, afterAll } from "vitest";
import * as fs from "node:fs";
import * as fsp from "node:fs/promises";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import { resolvePreactSsr, mergePreactAliases, importPreactSsr } from "./preact-ssr.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

describe("resolvePreactSsr", () => {
  const roots: string[] = [];

  afterAll(async () => {
    for (const root of roots) {
      await fsp.rm(root, { recursive: true, force: true });
    }
  });

  function makeApp(): string {
    const root = fs.mkdtempSync(path.join(__dirname, ".tmp-preact-ssr-"));
    roots.push(root);
    fs.writeFileSync(
      path.join(root, "package.json"),
      JSON.stringify({
        name: "preact-ssr-fixture",
        type: "module",
        dependencies: { preact: "^10.25.4" },
      })
    );
    const preactEntry = createRequire(import.meta.url).resolve("preact");
    const preactRoot = findPackageRoot(preactEntry, "preact");
    fs.mkdirSync(path.join(root, "node_modules"), { recursive: true });
    fs.symlinkSync(preactRoot, path.join(root, "node_modules", "preact"));
    return root;
  }

  it("resolves preact from the app and RTS from the framework", () => {
    const root = makeApp();
    const resolved = resolvePreactSsr(root, {
      from: [createRequire(import.meta.url).resolve("../../package.json")],
    });
    expect(fs.existsSync(resolved.paths.preact)).toBe(true);
    expect(fs.existsSync(resolved.paths["preact/hooks"])).toBe(true);
    expect(fs.existsSync(resolved.paths["preact-render-to-string"])).toBe(true);
    // App root must not need its own RTS install.
    expect(fs.existsSync(path.join(root, "node_modules", "preact-render-to-string"))).toBe(
      false
    );
    // Aliases are absolute paths.
    for (const value of Object.values(resolved.aliases)) {
      expect(path.isAbsolute(value)).toBe(true);
    }
  });

  it("mergePreactAliases rewrites react-compat bare targets to absolute paths", () => {
    const root = makeApp();
    const preact = resolvePreactSsr(root);
    const merged = mergePreactAliases(preact, {
      react: "preact/compat",
      "react-dom/server": "preact-render-to-string",
    });
    expect(merged.react).toBe(preact.paths["preact/compat"]);
    expect(merged["react-dom/server"]).toBe(preact.aliases["preact-render-to-string"]);
    // preact alias is the package root (not the entry file).
    expect(merged.preact).toBe(preact.aliases.preact);
    expect(fs.existsSync(path.join(merged.preact, "package.json"))).toBe(true);
  });

  it("importPreactSsr returns a working h + renderToString pair", async () => {
    const root = makeApp();
    const preact = resolvePreactSsr(root);
    const { h, renderToString } = await importPreactSsr(preact);
    const html = renderToString(h("div", { id: "x" }, "ok"));
    expect(html).toBe('<div id="x">ok</div>');
  });
});

function findPackageRoot(entryFile: string, name: string): string {
  let dir = path.dirname(entryFile);
  for (let i = 0; i < 10; i++) {
    const pkgPath = path.join(dir, "package.json");
    if (fs.existsSync(pkgPath)) {
      const pkg = JSON.parse(fs.readFileSync(pkgPath, "utf-8")) as { name?: string };
      if (pkg.name === name) return dir;
    }
    const parent = path.dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }
  throw new Error(`Could not find package root for ${name} from ${entryFile}`);
}
