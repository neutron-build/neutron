import * as fs from "node:fs";
import * as path from "node:path";
import { brotliCompressSync, constants as zlibConstants, gzipSync } from "node:zlib";
import type { AdapterBuildContext, NeutronAdapter } from "./adapter.js";
import { readStaticHeadersMap, toCloudflareHeadersFile } from "./headers.js";

export interface StaticAdapterOptions {
  allowAppRoutes?: boolean;
  precompress?: boolean;
  writeHeadersFile?: boolean;
}

export function adapterStatic(options: StaticAdapterOptions = {}): NeutronAdapter {
  return {
    name: "static",
    adapt(context: AdapterBuildContext) {
      if (context.routes.app > 0 && !options.allowAppRoutes) {
        throw new Error(
          `Static adapter cannot package app routes (${context.routes.app} found). ` +
            "Set allowAppRoutes: true if this is intentional."
        );
      }

      const defaultHeaders = [
        {
          source: "/assets/*",
          headers: {
            "Cache-Control": "public, max-age=31536000, immutable",
          },
        },
      ];
      const htmlRouteHeaders = toHtmlRouteHeaderRules(context.outDir);
      const staticRouteHeaders = readStaticHeadersMap(context.outDir);
      const mergedHeaders = mergeHeaderRules(
        [...defaultHeaders, ...htmlRouteHeaders],
        staticRouteHeaders
      );

      if (options.writeHeadersFile !== false) {
        const headersPath = path.join(context.outDir, "_headers");
        fs.writeFileSync(
          headersPath,
          toCloudflareHeadersFile(mergedHeaders),
          "utf-8"
        );
        context.log(`Static headers file written: ${path.relative(context.rootDir, headersPath)}`);
      }

      const policyPath = path.join(context.outDir, ".neutron-static-policy.json");
      fs.writeFileSync(
        policyPath,
        JSON.stringify(
          {
            generatedAt: new Date().toISOString(),
            headers: mergedHeaders,
          },
          null,
          2
        ),
        "utf-8"
      );
      context.log(`Static policy written: ${path.relative(context.rootDir, policyPath)}`);

      const compressionSummary =
        options.precompress === false
          ? { enabled: false, files: 0, gzipBytesSaved: 0, brotliBytesSaved: 0 }
          : precompressFiles(context.outDir, context);

      const metadata = {
        adapter: "static",
        routes: context.routes,
        compression: compressionSummary,
        generatedAt: new Date().toISOString(),
      };
      const metadataPath = path.join(context.outDir, ".neutron-adapter-static.json");
      fs.writeFileSync(metadataPath, JSON.stringify(metadata, null, 2), "utf-8");
      context.log(`Static adapter metadata written: ${path.relative(context.rootDir, metadataPath)}`);
    },
  };
}

function mergeHeaderRules(
  defaultRules: Array<{ source: string; headers: Record<string, string> }>,
  routeHeaders: Record<string, Record<string, string>>
): Record<string, Record<string, string>> {
  const merged: Record<string, Record<string, string>> = {};

  for (const rule of defaultRules) {
    merged[rule.source] = { ...rule.headers };
  }

  for (const [route, headers] of Object.entries(routeHeaders)) {
    merged[route] = {
      ...(merged[route] || {}),
      ...headers,
    };
  }

  return merged;
}

function toHtmlRouteHeaderRules(
  outDir: string
): Array<{ source: string; headers: Record<string, string> }> {
  const routes = collectHtmlRoutes(outDir);
  return routes.map((route) => ({
    source: route,
    headers: {
      "Cache-Control": "public, max-age=0, must-revalidate",
    },
  }));
}

function collectHtmlRoutes(outDir: string): string[] {
  const htmlFiles = collectFilesByExtension(outDir, ".html");
  const routes = new Set<string>();

  for (const filePath of htmlFiles) {
    const relative = path.relative(outDir, filePath).split(path.sep).join("/");
    if (relative === "index.html") {
      routes.add("/");
      continue;
    }

    if (relative.endsWith("/index.html")) {
      const route = "/" + relative.slice(0, -"/index.html".length);
      routes.add(route);
      routes.add(`${route}/`);
    }
  }

  return [...routes].sort();
}

function collectFilesByExtension(dir: string, extension: string): string[] {
  const output: string[] = [];
  const entries = fs.readdirSync(dir, { withFileTypes: true });

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      output.push(...collectFilesByExtension(fullPath, extension));
      continue;
    }
    if (entry.isFile() && entry.name.toLowerCase().endsWith(extension)) {
      output.push(fullPath);
    }
  }

  return output;
}

function precompressFiles(
  outDir: string,
  context: AdapterBuildContext
): { enabled: boolean; files: number; gzipBytesSaved: number; brotliBytesSaved: number } {
  const targets = collectCompressibleFiles(outDir);
  let files = 0;
  let gzipBytesSaved = 0;
  let brotliBytesSaved = 0;

  for (const filePath of targets) {
    const source = fs.readFileSync(filePath);
    if (source.length < 128) {
      continue;
    }

    const gzip = gzipSync(source, { level: 9 });
    const brotli = brotliCompressSync(source, {
      params: {
        [zlibConstants.BROTLI_PARAM_QUALITY]: 11,
      },
    });

    const gzipPath = `${filePath}.gz`;
    const brotliPath = `${filePath}.br`;
    if (gzip.length < source.length) {
      fs.writeFileSync(gzipPath, gzip);
      gzipBytesSaved += source.length - gzip.length;
    }
    if (brotli.length < source.length) {
      fs.writeFileSync(brotliPath, brotli);
      brotliBytesSaved += source.length - brotli.length;
    }

    files += 1;
  }

  context.log(
    `Static precompression complete: ${files} files (${formatBytes(
      gzipBytesSaved
    )} gzip saved, ${formatBytes(brotliBytesSaved)} brotli saved)`
  );

  return {
    enabled: true,
    files,
    gzipBytesSaved,
    brotliBytesSaved,
  };
}

function collectCompressibleFiles(dir: string): string[] {
  const output: string[] = [];
  const entries = fs.readdirSync(dir, { withFileTypes: true });

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      output.push(...collectCompressibleFiles(fullPath));
      continue;
    }

    if (!entry.isFile()) {
      continue;
    }

    if (entry.name.endsWith(".gz") || entry.name.endsWith(".br")) {
      continue;
    }

    const ext = path.extname(entry.name).toLowerCase();
    if (!isCompressibleExtension(ext)) {
      continue;
    }

    output.push(fullPath);
  }

  return output;
}

function isCompressibleExtension(ext: string): boolean {
  return (
    ext === ".html" ||
    ext === ".js" ||
    ext === ".mjs" ||
    ext === ".cjs" ||
    ext === ".css" ||
    ext === ".json" ||
    ext === ".svg" ||
    ext === ".xml" ||
    ext === ".txt" ||
    ext === ".map"
  );
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) {
    return `${bytes} B`;
  }
  const kb = bytes / 1024;
  if (kb < 1024) {
    return `${kb.toFixed(1)} KB`;
  }
  return `${(kb / 1024).toFixed(2)} MB`;
}
