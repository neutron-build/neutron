import * as path from "node:path";
import * as fs from "node:fs";
import { createRequire } from "node:module";
import { createServer, loadConfigFromFile, mergeConfig } from "vite";
import {
  neutronPlugin,
} from "@neutron-build/core/vite";
import { runtimeEsbuild, stripCliOwnedPlugins } from "../lib/vite-shared.js";
import {
  prepareContentCollections,
  prepareRouteTypes,
  resolveRuntime,
  resolveRuntimeAliases,
  resolveRuntimeNoExternal,
  resolvePreactSsr,
  vitePreactAliases,
} from "@neutron-build/core";
import { onShutdownSignal } from "@neutron-build/core/server";
import { loadNeutronConfig } from "../lib/config.js";
import { resolveListenAddress, type ListenAddress } from "../lib/listen.js";

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
  const esbuildJsx = runtimeEsbuild(runtime);

  let address: ListenAddress;
  try {
    address = resolveListenAddress({
      argv: process.argv.slice(3),
      env: process.env,
      config: neutronConfig.server,
    });
  } catch (error) {
    console.error(`neutron-ts dev: ${(error as Error).message}`);
    process.exit(1);
  }
  const { port, host } = address;

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

  const filteredPlugins = stripCliOwnedPlugins(userConfig.plugins);

  // Vite installs its own SIGTERM handler that destroys every open socket and
  // exits 143, dropping in-flight requests. Remove it; the drain below
  // replaces it (FRAMEWORK_CONTRACT.md §8).
  const sigtermListenersBefore = new Set(process.listeners("SIGTERM"));

  const server = await createServer(
    mergeConfig({ ...userConfig, plugins: filteredPlugins }, {
      esbuild: esbuildJsx,
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
          version: neutronConfig.server?.version,
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
        // An explicitly configured port must bind or fail, not silently move.
        strictPort: address.portExplicit,
        ...(host ? { host } : {}),
      },
    })
  );

  for (const listener of process.listeners("SIGTERM")) {
    if (!sigtermListenersBefore.has(listener)) {
      process.off("SIGTERM", listener);
    }
  }

  await server.listen();

  onShutdownSignal(async () => {
    const httpServer = server.httpServer;
    if (httpServer) {
      // Stop accepting, then wait for in-flight requests. HMR sockets are
      // upgraded connections that never go idle, so close them first or
      // the drain would always run to the timeout with a browser tab open.
      const drained = new Promise<void>((resolve, reject) => {
        httpServer.close((err) => (err ? reject(err) : resolve()));
      });
      (httpServer as { closeIdleConnections?: () => void }).closeIdleConnections?.();
      await server.ws.close();
      await drained;
    }
    await server.close();
  });

  const boundAddress = server.httpServer?.address();
  const resolvedPort =
    boundAddress && typeof boundAddress === "object" ? boundAddress.port : port;

  console.log(`
  Neutron dev server running:

  Local:   http://localhost:${resolvedPort}
  Routes:  ${routesDir}

  Press Ctrl+C to stop
`);
}
