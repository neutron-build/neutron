import { describe, it, expect, afterAll } from "vitest";
import * as fs from "node:fs";
import * as fsp from "node:fs/promises";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { createServer } from "vite";
import { createRequire } from "node:module";
import { resolvePreactSsr, mergePreactAliases } from "../core/preact-ssr.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

/**
 * Regression guard for the dual-preact-instance SSR crash:
 *   TypeError: Cannot read properties of undefined (reading '__H')
 *
 * The old unit test (same-file vitest import of preact + hooks + RTS) was a
 * false negative: one module graph, one options object, so hooks always work.
 *
 * The real failure (Tebian site, 0/81 pages) is:
 *   - routes/components load via Vite's module-runner
 *   - preact-render-to-string is unresolvable from the app root under pnpm
 *     (app declares preact only; RTS lives under @neutron-build/core)
 *   - silent native fallback binds a second options object
 *   - ThemeToggle-style useState crashes
 *
 * This test forces that topology: a temp "app" with preact but NO
 * preact-render-to-string at its root, a hooks component, and Vite SSR.
 * Without resolvePreactSsr aliases the render must fail; with them it must pass.
 */
describe("SSR dual-preact graph (real path)", () => {
  const roots: string[] = [];

  afterAll(async () => {
    for (const root of roots) {
      await fsp.rm(root, { recursive: true, force: true });
    }
  });

  function makeApp(): string {
    const root = fs.mkdtempSync(path.join(__dirname, ".tmp-ssr-hooks-"));
    roots.push(root);
    fs.mkdirSync(path.join(root, "src", "components"), { recursive: true });
    // App declares preact only — mirrors real consumer package.json.
    fs.writeFileSync(
      path.join(root, "package.json"),
      JSON.stringify({
        name: "ssr-hooks-fixture",
        type: "module",
        dependencies: { preact: "^10.25.4" },
      })
    );
    // Point node at the monorepo preact so the fixture can import it without
    // a full pnpm install. We deliberately do NOT link preact-render-to-string.
    const preactEntry = createRequire(import.meta.url).resolve("preact");
    const preactRoot = findPackageRoot(preactEntry, "preact");
    fs.mkdirSync(path.join(root, "node_modules"), { recursive: true });
    fs.symlinkSync(preactRoot, path.join(root, "node_modules", "preact"));

    // Plain JS + h() so the fixture needs no JSX transform / preset-vite.
    fs.writeFileSync(
      path.join(root, "src", "components", "ThemeToggle.js"),
      `import { h } from "preact";
import { useState, useEffect } from "preact/hooks";
export function ThemeToggle() {
  const [icon, setIcon] = useState("x");
  useEffect(() => { setIcon("y"); }, []);
  return h("button", { class: "theme-toggle" }, icon);
}
`
    );
    return root;
  }

  it("app package.json does not declare preact-render-to-string (consumer shape)", () => {
    const root = makeApp();
    const pkg = JSON.parse(
      fs.readFileSync(path.join(root, "package.json"), "utf-8")
    ) as { dependencies: Record<string, string> };
    expect(pkg.dependencies.preact).toBeTruthy();
    expect(pkg.dependencies["preact-render-to-string"]).toBeUndefined();
    expect(fs.existsSync(path.join(root, "node_modules", "preact-render-to-string"))).toBe(
      false
    );
  });

  it("renders hooks through Vite SSR when resolvePreactSsr aliases are applied", async () => {
    const root = makeApp();
    const preactSsr = resolvePreactSsr(root, {
      from: [createRequire(import.meta.url).resolve("../../package.json")],
    });
    const aliases = mergePreactAliases(preactSsr);

    const server = await createServer({
      configFile: false,
      root,
      resolve: {
        alias: aliases,
        dedupe: ["preact", "preact/hooks", "preact/jsx-runtime", "preact/compat"],
      },
      ssr: { noExternal: preactSsr.noExternal },
      server: { middlewareMode: true, hmr: false, ws: false, fs: { strict: false } },
      optimizeDeps: { noDiscovery: true },
      appType: "custom",
    });

    try {
      const resolved = await server.pluginContainer.resolveId("preact-render-to-string");
      expect(resolved?.id).toBeTruthy();

      const appPreact = (await server.ssrLoadModule("preact")) as {
        h: typeof import("preact").h;
        options: { __r?: unknown };
      };
      // Force hooks to register the dispatcher on this options object.
      await server.ssrLoadModule("preact/hooks");

      const appRts = (await server.ssrLoadModule("preact-render-to-string")) as {
        renderToString?: (vnode: unknown) => string;
        default?: { renderToString?: (vnode: unknown) => string };
      };
      const render = appRts.renderToString ?? appRts.default?.renderToString;
      expect(render).toBeTypeOf("function");

      const theme = (await server.ssrLoadModule(
        path.join(root, "src/components/ThemeToggle.js")
      )) as { ThemeToggle: import("preact").FunctionComponent };

      const html = render!(appPreact.h(theme.ThemeToggle, {}));
      expect(html).toContain("theme-toggle");
      expect(html).toContain("<button");
    } finally {
      await server.close();
    }
  });

  it("resolvePreactSsr prefers the app preact and still finds RTS from core", () => {
    const root = makeApp();
    const preactSsr = resolvePreactSsr(root, {
      from: [createRequire(import.meta.url).resolve("../../package.json")],
    });
    expect(preactSsr.paths.preact).toContain(`${path.sep}preact${path.sep}`);
    expect(fs.existsSync(preactSsr.paths.preact)).toBe(true);
    expect(fs.existsSync(preactSsr.paths["preact-render-to-string"])).toBe(true);
    // App has no RTS at its root — proof we pulled it from the framework.
    expect(fs.existsSync(path.join(root, "node_modules", "preact-render-to-string"))).toBe(
      false
    );
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
