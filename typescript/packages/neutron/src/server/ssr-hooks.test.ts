import { describe, it, expect, afterAll } from "vitest";
import * as fs from "node:fs";
import * as fsp from "node:fs/promises";
import * as path from "node:path";
import * as net from "node:net";
import { fileURLToPath } from "node:url";
import { createServer } from "vite";
import { createRequire } from "node:module";
import { resolvePreactSsr, vitePreactAliases } from "../core/preact-ssr.js";

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
    const aliases = vitePreactAliases(preactSsr);

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

/**
 * The same dual-preact crash class, through the production start path:
 * `createServer` (mode "ssr") loads route modules through its own Vite SSR
 * runner, so the renderer it hands to renderAppRoute must come from that
 * same graph — not from core's static imports (Gap 5's react-compat variant
 * is the sibling test in core/react-compat-ssr.test.ts; this one pins the
 * default preact runtime).
 */
describe("SSR dual-preact graph through createServer (start shape)", () => {
  const roots: string[] = [];

  afterAll(async () => {
    for (const root of roots) {
      await fsp.rm(root, { recursive: true, force: true });
    }
  });

  function makeStartApp(): string {
    const root = fs.mkdtempSync(path.join(__dirname, ".tmp-ssr-start-"));
    roots.push(root);
    fs.writeFileSync(
      path.join(root, "package.json"),
      JSON.stringify({
        name: "ssr-start-fixture",
        type: "module",
        dependencies: { preact: "^10.25.4" },
      })
    );
    const nm = path.join(root, "node_modules");
    fs.mkdirSync(nm, { recursive: true });
    const preactRoot = findPackageRoot(
      createRequire(import.meta.url).resolve("preact"),
      "preact"
    );
    fs.symlinkSync(preactRoot, path.join(nm, "preact"), "dir");
    // The app installs core the way real consumers do, so routes can import
    // framework APIs from their own node_modules.
    fs.mkdirSync(path.join(nm, "@neutron-build"), { recursive: true });
    fs.symlinkSync(
      path.resolve(__dirname, "../.."),
      path.join(nm, "@neutron-build", "core"),
      "dir"
    );

    fs.mkdirSync(path.join(root, "src", "routes"), { recursive: true });
    // useState + core's useLocation in one component: both hook sources
    // (app preact, core) must land on the renderer's graph.
    fs.writeFileSync(
      path.join(root, "src", "routes", "index.ts"),
      [
        'import { h } from "preact";',
        'import { useState } from "preact/hooks";',
        'import { useLocation } from "@neutron-build/core";',
        "",
        'export const config = { mode: "app" };',
        "",
        "export default function Page() {",
        '  const [count] = useState(5);',
        "  const { pathname } = useLocation();",
        '  return h("span", { "data-hook": "both" }, `count:${count} at:${pathname}`);',
        "}",
        "",
      ].join("\n")
    );
    return root;
  }

  async function getFreePort(): Promise<number> {
    return await new Promise<number>((resolve, reject) => {
      const socket = net.createServer();
      socket.listen(0, "127.0.0.1", () => {
        const address = socket.address();
        if (!address || typeof address === "string") {
          reject(new Error("Failed to resolve test port"));
          return;
        }
        socket.close(() => resolve(address.port));
        socket.on("error", reject);
      });
    });
  }

  it("renders preact + core hooks per request through the start runtime", async () => {
    const { createServer: createNeutronServer } = await import("./index.js");
    const root = makeStartApp();
    const port = await getFreePort();
    const server = await createNeutronServer({
      mode: "ssr",
      runtime: "preact",
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
    });
    try {
      const res = await fetch(`http://127.0.0.1:${port}/`);
      expect(res.status).toBe(200);
      const html = await res.text();
      expect(html).toContain('data-hook="both"');
      expect(html).toContain("count:5");
      expect(html).toContain("at:/");
      expect(html).not.toContain("__H");
    } finally {
      await server.close();
    }
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
