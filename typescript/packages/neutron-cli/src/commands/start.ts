import { createServer, startServer } from "@neutron-build/core/server";
import { prepareContentCollections, resolveRuntime } from "@neutron-build/core";
import { loadEnv } from "vite";
import { loadNeutronConfig } from "../lib/config.js";
import { resolveListenAddress, type ListenAddress } from "../lib/listen.js";

export async function start() {
  const cwd = process.cwd();
  applyEnv(cwd, "production");
  const neutronConfig = await loadNeutronConfig(cwd);
  let address: ListenAddress;
  try {
    address = resolveListenAddress({
      argv: process.argv.slice(3),
      env: process.env,
      config: neutronConfig.server,
      defaultHost: "0.0.0.0",
    });
  } catch (error) {
    console.error(`neutron-ts start: ${(error as Error).message}`);
    process.exit(1);
  }

  await prepareContentCollections({
    rootDir: cwd,
    writeManifest: false,
    writeTypes: false,
  });

  await startServer({
    ...neutronConfig.server,
    routes: neutronConfig.routes,
    runtime: resolveRuntime(neutronConfig),
    port: address.port,
    host: address.host,
    rootDir: cwd,
  });
}

export { createServer };

function applyEnv(cwd: string, mode: string): void {
  const env = loadEnv(mode, cwd, "");
  for (const [key, value] of Object.entries(env)) {
    if (process.env[key] === undefined) {
      process.env[key] = value;
    }
  }
}
