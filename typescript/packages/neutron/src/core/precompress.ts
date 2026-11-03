import * as fs from "node:fs";
import * as path from "node:path";
import { brotliCompressSync, constants as zlibConstants, gzipSync } from "node:zlib";

/**
 * Build-time `.br` / `.gz` siblings for text outputs. `neutron-ts build`
 * writes them for client assets and prerendered HTML, which the production
 * server then sends as-is instead of compressing per request; the static
 * adapter writes them for hosts that serve precompressed files.
 */

/** Below this, `neutron-ts build` leaves a file uncompressed. */
export const PRECOMPRESS_MIN_BYTES = 1024;

const PRECOMPRESSIBLE_EXTENSIONS = new Set([
  ".html",
  ".js",
  ".mjs",
  ".cjs",
  ".css",
  ".json",
  ".svg",
  ".xml",
  ".txt",
  ".wasm",
]);

export function isPrecompressibleFile(filePath: string): boolean {
  return PRECOMPRESSIBLE_EXTENSIONS.has(path.extname(filePath).toLowerCase());
}

export interface PrecompressSummary {
  files: number;
  gzipBytesSaved: number;
  brotliBytesSaved: number;
}

export interface PrecompressOptions {
  minBytes?: number;
  /** Skip an entry, given its path relative to the root (posix separators). */
  skip?: (relativePath: string, isDirectory: boolean) => boolean;
}

/**
 * Write `<file>.br` (quality 11) and `<file>.gz` (level 9) next to every
 * precompressible file under `rootDir` of at least `minBytes`. A sibling is
 * only written when it is smaller than the source, and one already newer
 * than its source is kept, so a second pass over the same output is cheap.
 */
export function precompressDirectory(rootDir: string, options: PrecompressOptions = {}): PrecompressSummary {
  const minBytes = options.minBytes ?? PRECOMPRESS_MIN_BYTES;
  const summary: PrecompressSummary = { files: 0, gzipBytesSaved: 0, brotliBytesSaved: 0 };
  if (!fs.existsSync(rootDir)) return summary;

  const pending = [rootDir];
  while (pending.length > 0) {
    const dir = pending.pop()!;
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const fullPath = path.join(dir, entry.name);
      const relativePath = path.relative(rootDir, fullPath).split(path.sep).join("/");
      if (entry.isDirectory()) {
        if (!options.skip?.(relativePath, true)) pending.push(fullPath);
        continue;
      }
      if (!entry.isFile() || !isPrecompressibleFile(entry.name) || options.skip?.(relativePath, false)) {
        continue;
      }
      const stat = fs.statSync(fullPath);
      if (stat.size < minBytes) continue;
      const source = fs.readFileSync(fullPath);
      summary.files += 1;
      summary.gzipBytesSaved += writeSibling(`${fullPath}.gz`, source, stat.mtimeMs, () =>
        gzipSync(source, { level: 9 })
      );
      summary.brotliBytesSaved += writeSibling(`${fullPath}.br`, source, stat.mtimeMs, () =>
        brotliCompressSync(source, {
          params: {
            [zlibConstants.BROTLI_PARAM_QUALITY]: 11,
            [zlibConstants.BROTLI_PARAM_SIZE_HINT]: source.length,
          },
        })
      );
    }
  }
  return summary;
}

function writeSibling(target: string, source: Buffer, sourceMtimeMs: number, compress: () => Buffer): number {
  try {
    const existing = fs.statSync(target);
    if (existing.mtimeMs >= sourceMtimeMs) return Math.max(0, source.length - existing.size);
  } catch {
    // No sibling yet.
  }
  const compressed = compress();
  if (compressed.length >= source.length) {
    // A stale sibling from an earlier build must not outlive its source.
    fs.rmSync(target, { force: true });
    return 0;
  }
  fs.writeFileSync(target, compressed);
  return source.length - compressed.length;
}

/**
 * Precompress what the production server sends from a `neutron-ts build`
 * output: client assets, public files and prerendered HTML. The server
 * bundle (`server/`), dependency trees and dot-prefixed build metadata are
 * never served, so they are left alone.
 */
export function precompressBuildOutput(distDir: string): PrecompressSummary {
  return precompressDirectory(distDir, {
    skip: (relativePath, isDirectory) => {
      const name = relativePath.slice(relativePath.lastIndexOf("/") + 1);
      if (name.startsWith(".")) return true;
      if (isDirectory) return relativePath === "server" || name === "node_modules";
      return false;
    },
  });
}
