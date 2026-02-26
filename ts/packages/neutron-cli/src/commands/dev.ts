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

  const server = await createServer(
    mergeConfig(userConfig, {
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
        port: 3000,
      },
    })
  );

  await server.listen();

  const port = server.config.server.port || 3000;

  console.log(`
  Neutron dev server running:

  Local:   http://localhost:${port}
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
