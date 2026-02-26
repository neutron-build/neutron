import * as fs from "node:fs";
import { builtinModules } from "node:module";
import * as path from "node:path";
import type { AdapterBuildContext, NeutronAdapter } from "./adapter.js";
import {
  readStaticHeadersMap,
  toCloudflareHeadersFile,
} from "./headers.js";

export interface CloudflareAdapterOptions {
  mode?: "pages" | "workers";
  allowAppRoutes?: boolean;
  compatibilityDate?: string;
  workerEntry?: string;
}

export function adapterCloudflare(
  options: CloudflareAdapterOptions = {}
): NeutronAdapter {
  return {
    name: "cloudflare",
    async adapt(context: AdapterBuildContext) {
      const mode = options.mode || "pages";
      const hasAppRoutes = context.routes.app > 0;

      if (hasAppRoutes && !context.ensureRuntimeBundle) {
        if (!options.allowAppRoutes) {
          throw new Error("Cloudflare adapter requires runtime bundle support from neutron build.");
        }
        context.log(
          `Warning: ${context.routes.app} app route(s) found but no runtime bundler available. ` +
          "App routes will not be functional in the deployed output."
        );
      }

      let runtimeEntryImport: string | null = null;
      let nodeCompatRequired = false;
      if (hasAppRoutes && context.ensureRuntimeBundle) {
        const runtimeBundle = await context.ensureRuntimeBundle("worker");
        runtimeEntryImport = `./${runtimeBundle.entryRelativePath}`.replace(/\\/g, "/");
        nodeCompatRequired = runtimeBundleNeedsNodeCompat(runtimeBundle.entryPath);
        context.log(
          `Cloudflare worker runtime bundle: ${runtimeBundle.entryRelativePath}`
        );
        if (nodeCompatRequired) {
          context.log("Cloudflare worker runtime requires nodejs_compat.");
        }
      }

      const headersByRoute = readStaticHeadersMap(context.outDir);
      const headersFile = toCloudflareHeadersFile(headersByRoute);
      if (headersFile.trim().length > 0) {
        const headersPath = path.join(context.outDir, "_headers");
        fs.writeFileSync(headersPath, headersFile, "utf-8");
        context.log(
          `Cloudflare headers file written: ${path.relative(context.rootDir, headersPath)}`
        );
      }

      if (mode === "workers" || hasAppRoutes) {
        const workerEntry = options.workerEntry || "_worker.js";
        const workerPath = path.join(context.outDir, workerEntry);
        fs.mkdirSync(path.dirname(workerPath), { recursive: true });
        fs.writeFileSync(
          workerPath,
          buildCloudflareWorkerSource(runtimeEntryImport),
          "utf-8"
        );
        context.log(
          `Cloudflare worker entry written: ${path.relative(context.rootDir, workerPath)}`
        );

        const wranglerPath = path.join(context.outDir, "wrangler.json");
        const projectName = path
          .basename(context.rootDir)
          .toLowerCase()
          .replace(/[^a-z0-9-]/g, "-")
          .replace(/^-+|-+$/g, "");
        const wranglerConfig = {
          name: projectName || "neutron-app",
          main: `./${workerEntry}`.replace(/\\/g, "/"),
          compatibility_date: options.compatibilityDate || "2026-02-12",
          ...(nodeCompatRequired
            ? { compatibility_flags: ["nodejs_compat"] }
            : {}),
          assets: {
            binding: "ASSETS",
            directory: ".",
          },
        };
        fs.writeFileSync(wranglerPath, JSON.stringify(wranglerConfig, null, 2), "utf-8");
        context.log(
          `Cloudflare wrangler config written: ${path.relative(context.rootDir, wranglerPath)}`
        );
      }

      const metadata = {
        adapter: "cloudflare",
        mode,
        routes: context.routes,
        generatedAt: new Date().toISOString(),
      };
      const metadataPath = path.join(context.outDir, ".neutron-adapter-cloudflare.json");
      fs.writeFileSync(metadataPath, JSON.stringify(metadata, null, 2), "utf-8");
      context.log(
        `Cloudflare adapter metadata written: ${path.relative(context.rootDir, metadataPath)}`
      );
    },
  };
}

function runtimeBundleNeedsNodeCompat(entryPath: string): boolean {
  if (!fs.existsSync(entryPath)) {
    return true;
  }

  let source: string;
  try {
    source = fs.readFileSync(entryPath, "utf-8");
  } catch {
    return true;
  }

  const patterns = [
    /^\s*import[\s\S]*?from\s+["']([^"']+)["'];?/gm,
    /\brequire\s*\(\s*["']([^"']+)["']\s*\)/gm,
    /\bimport\s*\(\s*["']([^"']+)["']\s*\)/gm,
    /\bexport\s+[\s\S]*?from\s+["']([^"']+)["'];?/gm,
  ];
  const imports = new Set<string>();
  for (const pattern of patterns) {
    for (const match of source.matchAll(pattern)) {
      if (match[1]) {
        imports.add(match[1]);
      }
    }
  }

  const builtinNames = new Set([
    ...builtinModules,
    ...builtinModules.filter((name) => !name.startsWith("node:")).map((name) => `node:${name}`),
  ]);

  for (const id of imports) {
    if (id.startsWith("node:") || builtinNames.has(id)) {
      return true;
    }
  }

  return false;
}

function buildCloudflareWorkerSource(runtimeEntryImport: string | null): string {
  if (!runtimeEntryImport) {
    return `export default {
  async fetch(request, env) {
    return env.ASSETS.fetch(request);
  },
};
`;
  }

  return `import { handleNeutronRequest } from "${runtimeEntryImport}";

export default {
  async fetch(request, env) {
    const assetResponse = await env.ASSETS.fetch(request);
    if (assetResponse.status !== 404) {
      return assetResponse;
    }
    return handleNeutronRequest(request);
  },
};
`;
}
