import * as path from "node:path";
import * as fs from "node:fs";
import { createRequire } from "node:module";
import { createServer, loadConfigFromFile, mergeConfig } from "vite";
import {
  neutronPlugin,
} from "@neutron-build/core/vite";
import {
  prepareContentCollections,
  prepareRouteTypes,
  resolveRuntime,
  resolveRuntimeAliases,
  resolveRuntimeNoExternal,
  resolvePreactSsr,
  vitePreactAliases,
} from "@neutron-build/core";
import { loadNeutronConfig } from "../lib/config.js";

export async function dev(): Promise<void> {
  const cwd = process.cwd();
  const routesDir = path.resolve(cwd, "src/routes");
  const neutronConfig = await loadNeutronConfig(cwd, { mode: "development" });
  const runtime = resolveRuntime(neutronConfig);
  const runtimeAliases = resolveRuntimeAliases(runtime);
  const runtimeNoExternal = resolveRuntimeNoExternal(runtime);
  const preactSsr = resolvePreactSsr(cwd, {
    from: [createRequire(import.meta.url).resolve("../../package.json")],
  });
  // Ordered: jsx-dev-runtime / jsx-runtime / hooks before bare preact so Vite
  // does not prefix-match `preact` onto export-map-only subpaths.
  const preactAliases = vitePreactAliases(preactSsr, runtimeAliases);

  // Parse CLI args
  const args = process.argv.slice(3);
  let port = 3000;
  let host: string | undefined;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--port" && args[i + 1]) {
      port = parseInt(args[i + 1], 10);
      i++;
    } else if (args[i].startsWith("--port=")) {
      port = parseInt(args[i].split("=")[1], 10);
    } else if (args[i] === "--host" && args[i + 1]) {
      host = args[i + 1];
      i++;
    } else if (args[i].startsWith("--host=")) {
      host = args[i].split("=")[1];
    }
  }

  await prepareContentCollections({
    rootDir: cwd,
    writeManifest: false,
    writeTypes: true,
  });
  await prepareRouteTypes({
    rootDir: cwd,
    routesDir: "src/routes",
    writeTypes: true,
  });

  if (!fs.existsSync(routesDir)) {
    console.error(`Routes directory not found: ${routesDir}`);
    console.error(`Create src/routes/ with your route files.`);
    process.exit(1);
  }

  const loadedConfig = await loadConfigFromFile(
    { command: "serve", mode: "development" },
    undefined,
    cwd
  );

  const userConfig = loadedConfig?.config || {};

  // Strip plugins that the CLI will add to avoid duplicates from mergeConfig concatenation
  const cliPluginNames = new Set(["neutron:core"]);
  const filteredPlugins = (userConfig.plugins || []).filter((plugin: unknown) => {
    if (plugin && typeof plugin === "object" && "name" in plugin) {
      return !cliPluginNames.has((plugin as { name: string }).name);
    }
    return true;
  });

  const server = await createServer(
    mergeConfig({ ...userConfig, plugins: filteredPlugins }, {
      // Prevent Vite's resolveConfig from loading vite.config.ts a second time.
      // We already loaded it above via loadConfigFromFile and merged the result.
      // Without this, plugins (including @prefresh/vite) are instantiated twice,
      // causing double HMR preamble injection ("Identifier 'flushUpdates' has
      // already been declared").
      configFile: false,
      root: cwd,
      plugins: [
        neutronPlugin({
          routesDir,
          rootDir: cwd,
          writeRouteTypes: true,
          routeRules: neutronConfig.routes,
        }),
      ],
      resolve: {
        // Absolute aliases so preact-render-to-string is resolvable even when
        // the app only declares `preact` (pnpm keeps RTS under core/cli), and
        // so jsx-dev-runtime resolves to a real file under the client graph.
        alias: preactAliases,
        dedupe: [
          "preact",
          "preact/hooks",
          "preact/jsx-runtime",
          "preact/jsx-dev-runtime",
          "preact/compat",
        ],
      },
      ssr: {
        // @neutron-build/core shares the SSR graph's preact too, so its inline
        // hook components (e.g. Link) don't crash dev SSR with "reading '__H'".
        noExternal: [...preactSsr.noExternal, ...runtimeNoExternal],
      },
      optimizeDeps: {
        // Force the client optimizer to pre-bundle the same absolute entries
        // we alias — without this, @preact/preset-vite's include of
        // `preact/jsx-dev-runtime` fails under pnpm + file: linked packages.
        include: [
          "preact",
          "preact/hooks",
          "preact/jsx-runtime",
          "preact/jsx-dev-runtime",
          "preact/compat",
        ],
      },
      server: {
        port,
        ...(host ? { host } : {}),
      },
    })
  );

  await server.listen();

  const resolvedPort = server.config.server.port || port;

  console.log(`
  Neutron dev server running:

  Local:   http://localhost:${resolvedPort}
  Routes:  ${routesDir}

  Press Ctrl+C to stop
`);
}
