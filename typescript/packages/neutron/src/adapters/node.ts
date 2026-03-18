import * as fs from "node:fs";
import * as path from "node:path";
import type { AdapterBuildContext, NeutronAdapter } from "./adapter.js";

export interface NodeAdapterOptions {
  entry?: string;
}

export function adapterNode(options: NodeAdapterOptions = {}): NeutronAdapter {
  return {
    name: "node",
    adapt(context: AdapterBuildContext) {
      const metadata = {
        adapter: "node",
        entry: options.entry || "neutron/server",
        routes: context.routes,
        generatedAt: new Date().toISOString(),
      };

      const metadataPath = path.join(context.outDir, ".neutron-adapter-node.json");
      fs.writeFileSync(metadataPath, JSON.stringify(metadata, null, 2), "utf-8");
      context.log(`Node adapter metadata written: ${path.relative(context.rootDir, metadataPath)}`);
    },
  };
}
