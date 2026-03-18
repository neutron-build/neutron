import * as path from "node:path";
import * as fs from "node:fs";
import { createServer, loadConfigFromFile, mergeConfig } from "vite";
import {
  neutronPlugin,
} from "neutron/vite";
import {
  prepareContentCollections,
  prepareRouteTypes,
  resolveRuntime,
  resolveRuntimeAliases,
  resolveRuntimeNoExternal,
  type NeutronConfig,
} from "neutron";

export async function dev(): Promise<void> {
  const cwd = process.cwd();
  const routesDir = path.resolve(cwd, "src/routes");
  const neutronConfig = await loadNeutronConfig(cwd);
  const runtime = resolveRuntime(neutronConfig);
  const runtimeAliases = resolveRuntimeAliases(runtime);
  const runtimeNoExternal = resolveRuntimeNoExternal(runtime);

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
      ...(runtimeAliases ? { resolve: { alias: runtimeAliases } } : {}),
      ...(runtimeNoExternal.length > 0 ? { ssr: { noExternal: runtimeNoExternal } } : {}),
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

async function loadNeutronConfig(cwd: string): Promise<NeutronConfig> {
  const candidates = [
    "neutron.config.ts",
    "neutron.config.js",
    "neutron.config.mjs",
    "neutron.config.cjs",
  ];

  for (const file of candidates) {
    const fullPath = path.resolve(cwd, file);
    if (!fs.existsSync(fullPath)) {
      continue;
    }

    const loaded = await loadConfigFromFile(
      { command: "serve", mode: "development" },
      fullPath,
      cwd
    );
    if (loaded?.config) {
      return loaded.config as NeutronConfig;
    }
  }

  return {};
}
