import { createServer, startServer } from "@neutron-build/core/server";
import { prepareContentCollections, resolveRuntime } from "@neutron-build/core";
import { loadEnv } from "vite";
import { loadNeutronConfig } from "../lib/config.js";

export async function start() {
  const cwd = process.cwd();
  applyEnv(cwd, "production");
  const neutronConfig = await loadNeutronConfig(cwd);
  await prepareContentCollections({
    rootDir: cwd,
    writeManifest: false,
    writeTypes: false,
  });

  // Parse CLI args
  const args = process.argv.slice(3);
  let port = neutronConfig.server?.port || 3000;
  let host = neutronConfig.server?.host || "0.0.0.0";

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

  await startServer({
    ...neutronConfig.server,
    routes: neutronConfig.routes,
    runtime: resolveRuntime(neutronConfig),
    port,
    host,
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
