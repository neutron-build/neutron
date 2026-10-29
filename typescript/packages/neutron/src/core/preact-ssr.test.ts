import { describe, it, expect, afterAll } from "vitest";
import * as fs from "node:fs";
import * as fsp from "node:fs/promises";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import {
  resolvePreactSsr,
  vitePreactAliases,
  mergePreactAliases,
  importPreactSsr,
} from "./preact-ssr.js";
import { resolveRuntimeAliases } from "../config.js";

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
    expect(fs.existsSync(resolved.paths["preact/jsx-runtime"])).toBe(true);
    expect(fs.existsSync(resolved.paths["preact/jsx-dev-runtime"])).toBe(true);
    expect(fs.existsSync(resolved.paths["preact-render-to-string"])).toBe(true);
    expect(fs.existsSync(path.join(root, "node_modules", "preact-render-to-string"))).toBe(
      false
    );
  });

  it("vitePreactAliases lists subpaths before bare preact (prefix-match safety)", () => {
    const root = makeApp();
    const preact = resolvePreactSsr(root);
    const aliases = vitePreactAliases(preact);
    const finds = aliases.map((a) => a.find);

    expect(finds).toContain("preact/jsx-dev-runtime");
    expect(finds).toContain("preact/jsx-runtime");
    expect(finds).toContain("preact/hooks");
    expect(finds).toContain("preact");
    expect(finds).toContain("preact-render-to-string");

    const bareIdx = finds.indexOf("preact");
    const jsxDevIdx = finds.indexOf("preact/jsx-dev-runtime");
    const jsxIdx = finds.indexOf("preact/jsx-runtime");
    expect(jsxDevIdx).toBeLessThan(bareIdx);
    expect(jsxIdx).toBeLessThan(bareIdx);

    // Subpath replacements are real files, not package roots.
    const jsxDev = aliases.find((a) => a.find === "preact/jsx-dev-runtime")!;
    expect(fs.existsSync(jsxDev.replacement)).toBe(true);
    expect(fs.statSync(jsxDev.replacement).isFile()).toBe(true);

    // Bare preact is the package root (exports for unlisted subpaths).
    const bare = aliases.find((a) => a.find === "preact")!;
    expect(fs.existsSync(path.join(bare.replacement, "package.json"))).toBe(true);
  });

  it("vitePreactAliases rewrites react-compat targets onto absolute preact paths", () => {
    const root = makeApp();
    const preact = resolvePreactSsr(root);
    const aliases = vitePreactAliases(preact, {
      react: "preact/compat",
      "react-dom/server": "preact-render-to-string",
    });
    const map = Object.fromEntries(aliases.map((a) => [a.find, a.replacement]));
    // Bare react → the preact/compat package ROOT (directory), not the
    // entry file — see the react-compat ordering test below for why.
    expect(map.react).toBe(preact.packageRoots["preact/compat"]);
    expect(map["react-dom/server"]).toBe(preact.packageRoots["preact-render-to-string"]);
  });

  it("react-compat runtime aliases order subpaths before bare finds (Vite prefix-match safety)", () => {
    const root = makeApp();
    const preact = resolvePreactSsr(root);
    const aliases = vitePreactAliases(preact, resolveRuntimeAliases("react-compat"));
    const finds = aliases.map((a) => a.find);

    // resolveRuntimeAliases deliberately lists bare `react`/`react-dom`
    // FIRST. Vite string aliases are prefix-matched in array order
    // (`react` matches `react/jsx-runtime`), so every subpath entry must be
    // sorted ahead of its bare parent or the subpath import rewrites onto
    // the bare replacement and the client build dies with "Not a directory
    // (os error 20)" — the W4 web-spike build blocker.
    const reactIdx = finds.indexOf("react");
    const reactDomIdx = finds.indexOf("react-dom");
    expect(reactIdx).toBeGreaterThanOrEqual(0);
    expect(reactDomIdx).toBeGreaterThanOrEqual(0);
    expect(finds.indexOf("react/jsx-runtime")).toBeLessThan(reactIdx);
    expect(finds.indexOf("react/jsx-dev-runtime")).toBeLessThan(reactIdx);
    expect(finds.indexOf("react-dom/client")).toBeLessThan(reactDomIdx);
    expect(finds.indexOf("react-dom/server")).toBeLessThan(reactDomIdx);
    expect(finds.indexOf("react-dom/server.browser")).toBeLessThan(reactDomIdx);
    expect(finds.indexOf("react-dom/test-utils")).toBeLessThan(reactDomIdx);
  });

  it("react-compat aliases resolve every react import to a distinct, correct target", () => {
    const root = makeApp();
    const preact = resolvePreactSsr(root);
    const aliases = vitePreactAliases(preact, resolveRuntimeAliases("react-compat"));

    // Vite's actual matching semantics (@rollup/plugin-alias `matches` +
    // prefix-join replacement), first entry wins.
    const matchAlias = (id: string): string | null => {
      for (const { find, replacement } of aliases) {
        if (id === find || id.startsWith(find + "/")) {
          return replacement + id.slice(find.length);
        }
      }
      return null;
    };

    // Listed subpaths resolve to their own preact files — not onto the
    // bare parent's replacement.
    expect(matchAlias("react/jsx-runtime")).toBe(preact.paths["preact/jsx-runtime"]);
    expect(matchAlias("react/jsx-dev-runtime")).toBe(
      preact.paths["preact/jsx-dev-runtime"]
    );
    expect(matchAlias("react-dom/client")).toBe(preact.paths["preact/compat"]);
    expect(matchAlias("react-dom/server")).toBe(
      preact.packageRoots["preact-render-to-string"]
    );
    expect(matchAlias("react-dom/server.browser")).toBe(
      preact.packageRoots["preact-render-to-string"]
    );

    // Bare react-family ids resolve to the preact/compat package ROOT (a
    // directory with its own package.json), not the entry file, so an
    // unlisted react/* subpath joins a real directory instead of a
    // guaranteed ENOTDIR (`<compat.mjs>/<subpath>`).
    const compatRoot = preact.packageRoots["preact/compat"];
    expect(matchAlias("react")).toBe(compatRoot);
    expect(matchAlias("react-dom")).toBe(compatRoot);
    expect(fs.existsSync(path.join(compatRoot, "package.json"))).toBe(true);
    // The latent case the spike flagged: an unlisted react/* subpath.
    expect(matchAlias("react/unlisted-subpath")).toBe(
      path.join(compatRoot, "unlisted-subpath")
    );
    // subpath targets stay distinct from each other and from the bare target
    expect(matchAlias("react/jsx-runtime")).not.toBe(matchAlias("react"));
    // (react/jsx-dev-runtime maps to the SAME file as react/jsx-runtime:
    // preact points both exports at jsx-runtime/dist/jsxRuntime.mjs.)

    // A package whose name merely starts with "react" is NOT captured:
    // Vite requires the exact find or a `/` boundary.
    expect(matchAlias("react-is")).toBeNull();
  });

  it("mergePreactAliases flat map still covers the ordered keys", () => {
    const root = makeApp();
    const preact = resolvePreactSsr(root);
    const map = mergePreactAliases(preact);
    expect(map["preact/jsx-dev-runtime"]).toBe(preact.paths["preact/jsx-dev-runtime"]);
    expect(map.preact).toBe(preact.packageRoots.preact);
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
