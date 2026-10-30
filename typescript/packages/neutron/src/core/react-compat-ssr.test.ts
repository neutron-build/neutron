import { describe, it, expect, afterAll } from "vitest";
import * as fs from "node:fs";
import * as fsp from "node:fs/promises";
import * as path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { createRequire } from "node:module";
import { execFile } from "node:child_process";
import { createServer as createViteServer, build as viteBuild } from "vite";
import * as net from "node:net";
import { resolvePreactSsr, vitePreactAliases } from "./preact-ssr.js";
import { resolveRuntimeAliases, resolveRuntimeNoExternal } from "../config.js";
import { createServer as createNeutronServer } from "../server/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

/**
 * react-compat SSR regression coverage, from the Fylun web-spike findings
 * (FRAMEWORK_GAPS "W4 UI-spike findings" 1–2):
 *
 * 1. Alias ordering: `react/jsx-runtime` (what automatic JSX emits) must
 *    resolve to preact's jsx-runtime FILE, not onto the bare `react`
 *    replacement — the mis-ordering broke every automatic-JSX react-compat
 *    client build with "Not a directory (os error 20)".
 * 2. SSR externals: a dependency package that imports `react` must be
 *    processed by Vite's SSR graph so the react → preact/compat alias
 *    reaches it. When such a package stayed external (the old
 *    react*-only noExternal list), it natively resolved REAL React and
 *    crashed under preact-render-to-string — observed with Radix
 *    ("Cannot read properties of null (reading 'useRef')").
 *
 * The fixture mirrors a real react-compat app: `react` is installed (apps
 * keep it for @types/react), a local dependency package imports it, and the
 * Vite server/build is composed exactly the way dev/build/start compose it.
 */

const roots: string[] = [];

afterAll(async () => {
  for (const root of roots) {
    await fsp.rm(root, { recursive: true, force: true });
  }
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

/**
 * Real React, the way a react-compat app has it: installed in
 * node_modules, aliased away at build time. playground-react-compat
 * declares it; if the playground is absent (filtered install), fall back
 * to a stub whose import throws — either way the OLD behavior (externals
 * resolve their own react) fails this suite.
 */
function linkRealReact(appRoot: string): void {
  const nm = path.join(appRoot, "node_modules");
  const playgroundReact = path.resolve(
    __dirname,
    "../../../../apps/playground-react-compat/node_modules/react"
  );
  if (fs.existsSync(path.join(playgroundReact, "package.json"))) {
    fs.symlinkSync(playgroundReact, path.join(nm, "react"), "dir");
    return;
  }
  const stub = path.join(appRoot, "vendor", "react-stub");
  fs.mkdirSync(stub, { recursive: true });
  fs.writeFileSync(
    path.join(stub, "package.json"),
    JSON.stringify({ name: "react", version: "0.0.0-stub", type: "module" })
  );
  fs.writeFileSync(
    path.join(stub, "index.js"),
    'throw new Error("real react was loaded under react-compat SSR — externals policy broken");\n'
  );
  fs.symlinkSync(stub, path.join(nm, "react"), "dir");
}

/** A dependency package authored against React, like a Radix primitive. */
function makeReactEchoLib(appRoot: string): string {
  // The package's REAL path must live under a node_modules segment: Vite
  // externalizes SSR imports only when their resolved realpath is inside
  // node_modules (a pnpm-installed dep's realpath is
  // node_modules/.pnpm/…). A plain ./vendor path would be treated as app
  // source and always processed, which is not what the regression needs.
  const lib = path.join(appRoot, "vendor", "node_modules", "react-echo-lib");
  fs.mkdirSync(lib, { recursive: true });
  fs.writeFileSync(
    path.join(lib, "package.json"),
    JSON.stringify({
      name: "react-echo-lib",
      version: "1.0.0",
      type: "module",
      main: "./index.mjs",
      exports: "./index.mjs",
    })
  );
  fs.writeFileSync(
    path.join(lib, "index.mjs"),
    [
      'import { createElement, useRef, version } from "react";',
      "",
      "// Hook + element from `react`, the shape of every React-ecosystem lib.",
      "// On preact/compat this renders; on real React under",
      "// preact-render-to-string the hook throws (null dispatcher).",
      "export function ReactEcho({ label }) {",
      "  const ref = useRef(null);",
      "  return createElement(\"span\", { \"data-echo\": label, ref }, \"echo:\" + label);",
      "}",
      "",
      "// preact/compat re-exports preact's version string; real react reports",
      "// its own (e.g. 19.2.4). Names the failure mode if externals regress.",
      "export function reactVersion() {",
      "  return version;",
      "}",
      "",
    ].join("\n")
  );
  return lib;
}

function makeReactCompatApp(): string {
  const root = fs.mkdtempSync(path.join(__dirname, ".tmp-react-compat-ssr-"));
  roots.push(root);
  fs.writeFileSync(
    path.join(root, "package.json"),
    JSON.stringify({
      name: "react-compat-ssr-fixture",
      type: "module",
      dependencies: { preact: "^10.25.4", react: "*", "react-echo-lib": "*" },
    })
  );

  const nm = path.join(root, "node_modules");
  fs.mkdirSync(nm, { recursive: true });
  const preactRoot = findPackageRoot(createRequire(import.meta.url).resolve("preact"), "preact");
  fs.symlinkSync(preactRoot, path.join(nm, "preact"), "dir");
  fs.symlinkSync(makeReactEchoLib(root), path.join(nm, "react-echo-lib"), "dir");
  // A react-compat app installs core (the way this package's own consumers
  // do) so route files can import framework APIs from their own node_modules.
  fs.mkdirSync(path.join(nm, "@neutron-build"), { recursive: true });
  fs.symlinkSync(
    path.resolve(__dirname, "../.."),
    path.join(nm, "@neutron-build", "core"),
    "dir"
  );
  linkRealReact(root);

  fs.mkdirSync(path.join(root, "src"), { recursive: true });
  fs.writeFileSync(
    path.join(root, "src", "entry.mjs"),
    [
      'import { h } from "preact";',
      'import { renderToString } from "preact-render-to-string";',
      'import { ReactEcho, reactVersion } from "react-echo-lib";',
      "",
      "export function render() {",
      '  return renderToString(h(ReactEcho, { label: "spike" }));',
      "}",
      "",
      "export { reactVersion };",
      "",
    ].join("\n")
  );
  return root;
}

/** The production start shape: createServer + its own Vite SSR runner. */
function makeStartShapeApp(): string {
  const root = makeReactCompatApp();
  fs.rmSync(path.join(root, "src", "entry.mjs"));
  fs.mkdirSync(path.join(root, "src", "routes"), { recursive: true });
  // Plain h() (no JSX) so the fixture needs no tsconfig/jsx setup. The
  // route imports BOTH the react lib and @neutron-build/core's ROOT index:
  // core's re-exports pull its CJS chain (content/mdx), which the
  // production module-runner cannot evaluate — createSsrServer must keep
  // core external while blanket-processing the app's react graph.
  //
  // The component itself is hook-free: `start`'s streaming renderer
  // (core-side preact-render-to-string/stream) does not install a compat
  // hooks context — a separate, pre-existing react-compat gap outside
  // these fixes' scope (hooks render fine through dev and build, where
  // the renderer is loaded through the same Vite graph).
  fs.writeFileSync(
    path.join(root, "src", "routes", "index.ts"),
    [
      'import { h } from "preact";',
      'import { getCookie } from "@neutron-build/core";',
      'import { reactVersion } from "react-echo-lib";',
      "",
      "export const config = { mode: \"app\" };",
      "",
      "export async function loader({ request }) {",
      "  return {",
      "    session: getCookie(request, \"sid\"),",
      "    react: reactVersion(),",
      "  };",
      "}",
      "",
      "export default function Page({ data }) {",
      "  return h(\"span\", { \"data-variant\": data.session ? \"app\" : \"marketing\" },",
      "    \"react:\" + data.react);",
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

/** The resolve/noExternal composition dev/build/start derive from the runtime. */
function composeRuntimeSsrConfig(root: string) {
  const preactSsr = resolvePreactSsr(root, {
    from: [createRequire(import.meta.url).resolve("../../package.json")],
  });
  const runtimeAliases = resolveRuntimeAliases("react-compat");
  const runtimeNoExternal = resolveRuntimeNoExternal("react-compat");
  const noExternal: true | string[] =
    runtimeNoExternal === true ? true : [...preactSsr.noExternal, ...runtimeNoExternal];
  return {
    preactSsr,
    resolve: {
      alias: vitePreactAliases(preactSsr, runtimeAliases),
      dedupe: ["preact", "preact/hooks", "preact/jsx-runtime", "preact/compat"],
    },
    ssr: { noExternal },
  };
}

describe("react-compat SSR through the Vite graph", () => {
  it("resolves react/jsx-runtime to preact's file, not onto the bare react replacement", async () => {
    const root = makeReactCompatApp();
    const composed = composeRuntimeSsrConfig(root);
    const server = await createViteServer({
      configFile: false,
      root,
      logLevel: "error",
      resolve: composed.resolve,
      ssr: composed.ssr,
      server: { middlewareMode: true, hmr: false, ws: false, fs: { strict: false } },
      optimizeDeps: { noDiscovery: true },
      appType: "custom",
    });
    try {
      const jsxRuntime = await server.pluginContainer.resolveId("react/jsx-runtime");
      expect(jsxRuntime?.id).toBe(composed.preactSsr.paths["preact/jsx-runtime"]);

      const bareReact = await server.pluginContainer.resolveId("react");
      // Bare react resolves INSIDE the preact/compat package (its entry via
      // the compat package.json) — never to real react in node_modules.
      const compatRoot = composed.preactSsr.packageRoots["preact/compat"];
      expect(path.resolve(bareReact?.id ?? "")).toContain(path.resolve(compatRoot));
      expect(fs.existsSync(bareReact?.id ?? "")).toBe(true);
    } finally {
      await server.close();
    }
  });

  it("renders a react-importing dependency package instead of crashing (dev SSR graph)", async () => {
    const root = makeReactCompatApp();
    const composed = composeRuntimeSsrConfig(root);
    const server = await createViteServer({
      configFile: false,
      root,
      logLevel: "error",
      resolve: composed.resolve,
      ssr: composed.ssr,
      server: { middlewareMode: true, hmr: false, ws: false, fs: { strict: false } },
      optimizeDeps: { noDiscovery: true },
      appType: "custom",
    });
    try {
      // Old behavior (react*-only noExternal) externalized react-echo-lib;
      // it natively imported real react and render() threw
      // "Cannot read properties of null (reading 'useRef')".
      const mod = (await server.ssrLoadModule(path.join(root, "src", "entry.mjs"))) as {
        render: () => string;
        reactVersion: () => string;
      };
      // preact/compat deliberately re-exports a REACT version string
      // ("18.3.1" at preact 10.x) for library feature detection — so assert
      // the discriminator that matters: NOT the installed real react's
      // version (the old externals behavior served 19.2.4 here).
      const realReactVersion = (
        JSON.parse(
          fs.readFileSync(path.join(root, "node_modules", "react", "package.json"), "utf-8")
        ) as { version: string }
      ).version;
      expect(mod.reactVersion()).not.toBe(realReactVersion);
      expect(mod.reactVersion()).toMatch(/^\d+\.\d+\.\d+$/);
      const html = mod.render();
      expect(html).toContain('data-echo="spike"');
      expect(html).toContain("echo:spike");
    } finally {
      await server.close();
    }
  });

  it("builds the SSR bundle with the dependency inlined and renders it (runtime-bundle shape)", async () => {
    const root = makeReactCompatApp();
    const composed = composeRuntimeSsrConfig(root);
    const outDir = path.join(root, "dist-ssr");
    await viteBuild({
      configFile: false,
      root,
      logLevel: "error",
      resolve: composed.resolve,
      ssr: composed.ssr,
      build: {
        ssr: path.join(root, "src", "entry.mjs"),
        outDir,
        emptyOutDir: true,
        rolldownOptions: {
          output: { format: "esm", entryFileNames: "entry.js" },
        },
      },
    });

    const entryPath = path.join(outDir, "entry.js");
    expect(fs.existsSync(entryPath)).toBe(true);
    const bundled = fs.readFileSync(entryPath, "utf-8");
    // The dependency is INLINED: an externalized react-echo-lib would leave
    // a bare import that resolves real react at runtime.
    expect(bundled).not.toMatch(/from\s+["']react-echo-lib["']/);

    // Load the bundle in a real node process (not vitest's module runner,
    // which would transform it): this is the `neutron-ts start` shape.
    const probe = await new Promise<{ code: number; stdout: string; stderr: string }>(
      (resolve) => {
        const child = execFile(
          process.execPath,
          [
            "--input-type=module",
            "-e",
            `const m = await import(${JSON.stringify(pathToFileURL(entryPath).href)});` +
              `console.log(m.reactVersion());` +
              `console.log(m.render());`,
          ],
          { cwd: root },
          (error, stdout, stderr) => {
            resolve({
              code: error ? (error.code as number ?? 1) : 0,
              stdout: String(stdout),
              stderr: String(stderr),
            });
          }
        );
        child.stdin?.end();
      }
    );
    expect(probe.stderr).toBe("");
    expect(probe.code).toBe(0);
    const [versionLine, html] = probe.stdout.split("\n");
    // preact/compat reports a React-shaped version ("18.3.1"), never the
    // installed real react's own version (old externals behavior: 19.2.4).
    const realReactVersion = (
      JSON.parse(
        fs.readFileSync(path.join(root, "node_modules", "react", "package.json"), "utf-8")
      ) as { version: string }
    ).version;
    expect(versionLine).not.toBe(realReactVersion);
    expect(versionLine).toMatch(/^\d+\.\d+\.\d+$/);
    expect(html).toContain('data-echo="spike"');
  });

  it("start shape: createServer renders a react lib + core-importing app route (core stays external)", async () => {
    const root = makeStartShapeApp();
    const port = await getFreePort();
    const server = await createNeutronServer({
      mode: "ssr",
      runtime: "react-compat",
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
    });
    const base = `http://127.0.0.1:${port}`;

    try {
      // The route imports @neutron-build/core's ROOT index: if
      // createSsrServer stops keeping core external under react-compat,
      // Vite's production module-runner evaluates core's CJS chain
      // (mdx → acorn-jsx) as ESM and this route 500s with
      // "require is not defined".
      // The loader runs INSIDE the Vite graph, so reactVersion() reports
      // preact/compat's React-shaped version string ("18.3.1"), never the
      // real react the externals policy would hand it (19.2.4) — asserted
      // through the rendered HTML.
      const realReactVersion = (
        JSON.parse(
          fs.readFileSync(path.join(root, "node_modules", "react", "package.json"), "utf-8")
        ) as { version: string }
      ).version;
      const marketing = await fetch(`${base}/`);
      expect(marketing.status).toBe(200);
      const marketingHtml = await marketing.text();
      expect(marketingHtml).toContain('data-variant="marketing"');
      expect(marketingHtml).toMatch(/react:\d+\.\d+\.\d+/);
      expect(marketingHtml).not.toContain(`react:${realReactVersion}`);

      const session = await fetch(`${base}/`, {
        headers: { cookie: "sid=s1" },
      });
      expect(session.status).toBe(200);
      expect(await session.text()).toContain('data-variant="app"');

      // JSON transport answers (payload is devalue-encoded; the HTML leg
      // above already pins the loader's react identity).
      const data = await fetch(`${base}/`, {
        headers: { "x-neutron-data": "true" },
      });
      expect(data.status).toBe(200);
      expect(data.headers.get("content-type")).toContain("application/json");
    } finally {
      await server.close();
    }
  });
});
