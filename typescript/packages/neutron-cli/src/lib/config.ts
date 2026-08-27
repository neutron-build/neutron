import * as fs from "node:fs";
import * as path from "node:path";
import { loadConfigFromFile } from "vite";
import type { NeutronConfig } from "@neutron-build/core";

const CONFIG_CANDIDATES = [
  "neutron.config.ts",
  "neutron.config.js",
  "neutron.config.mjs",
  "neutron.config.cjs",
];

export interface LoadNeutronConfigOptions {
  command?: "serve" | "build";
  mode?: string;
}

export async function loadNeutronConfig(
  cwd: string,
  options: LoadNeutronConfigOptions = {}
): Promise<NeutronConfig> {
  const command = options.command ?? "serve";
  const mode = options.mode ?? "production";

  for (const file of CONFIG_CANDIDATES) {
    const fullPath = path.resolve(cwd, file);
    if (!fs.existsSync(fullPath)) {
      continue;
    }

    const loaded = await loadConfigFromFile({ command, mode }, fullPath, cwd);
    if (loaded?.config) {
      return loaded.config as NeutronConfig;
    }
  }

  return {};
}
