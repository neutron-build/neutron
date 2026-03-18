import * as fs from "node:fs";
import * as path from "node:path";
import { createServer, startServer } from "neutron/server";
import { prepareContentCollections, resolveRuntime, type NeutronConfig } from "neutron";
import { loadConfigFromFile, loadEnv } from "vite";

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
      { command: "serve", mode: "production" },
      fullPath,
      cwd
    );
    if (loaded?.config) {
      return loaded.config as NeutronConfig;
    }
  }

  return {};
}

function applyEnv(cwd: string, mode: string): void {
  const env = loadEnv(mode, cwd, "");
  for (const [key, value] of Object.entries(env)) {
    if (process.env[key] === undefined) {
      process.env[key] = value;
    }
  }
}
