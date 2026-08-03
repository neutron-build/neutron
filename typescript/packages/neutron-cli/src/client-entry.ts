/**
 * Resolving which built chunk is the client entry.
 *
 * Split out of `commands/build.ts` so it can be tested directly: that module
 * imports vite and preact at load time, which forced its tests to be
 * hand-mirrored copies of the source. A mirrored copy can pass while the real
 * function is broken, which is how the bug below survived.
 *
 * Only `node:fs` and `node:path` here — keep it that way.
 */

import * as fs from "node:fs";
import * as path from "node:path";

interface ManifestRecord {
  file?: string;
  isEntry?: boolean;
}

/**
 * Chunk filenames that Vite attributed to a route module.
 *
 * These are the reason the filename heuristic cannot be trusted alone: a
 * project with `src/routes/index.tsx` — every project with a homepage — emits
 * `assets/index-<hash>.js` for that route, indistinguishable by name from the
 * real client entry.
 */
export function routeChunkFilenames(manifest: Record<string, unknown>): Set<string> {
  const files = new Set<string>();
  for (const [key, value] of Object.entries(manifest)) {
    const record = value as ManifestRecord | null;
    if (!record?.file) continue;
    // Keys are project-relative source paths; the client-route pipeline also
    // appends a query, so compare only the path portion.
    const source = key.split("?")[0];
    if (/(^|\/)routes\//.test(source)) {
      files.add(record.file.replace(/^\/+/, ""));
    }
  }
  return files;
}

export function readViteManifest(outputDir: string): Record<string, unknown> | null {
  // Vite 5+ writes `.vite/manifest.json`; older versions wrote it at the root.
  for (const rel of [path.join(".vite", "manifest.json"), "manifest.json"]) {
    const p = path.join(outputDir, rel);
    if (!fs.existsSync(p)) continue;
    try {
      return JSON.parse(fs.readFileSync(p, "utf-8")) as Record<string, unknown>;
    } catch {
      return null;
    }
  }
  return null;
}

/**
 * Resolve the JS the browser should load to start the client runtime.
 *
 * Prefers Vite's manifest, which states outright which chunk is an entry.
 *
 * The previous implementation globbed `assets/index-*.js` and took the last
 * after sorting, so in any app with both a real client entry and a route named
 * `index` the winner was decided by which hash sorted higher. When it lost, the
 * page's entire client runtime became the route's stripped config module —
 * `const o={mode:"app"};export{o as config};`, 42 bytes, no router.
 *
 * The failure is silent. The page still renders, because it is server-rendered;
 * nothing errors. The app simply never hydrates, so no link is marked static,
 * so the speculation rules scoped to those marks match nothing, and every
 * navigation becomes a cold full page load. Observed live on covely.io.
 */
export function extractClientEntryScriptSrc(outputDir: string): string | null {
  const manifest = readViteManifest(outputDir);
  const routeChunks = manifest ? routeChunkFilenames(manifest) : new Set<string>();

  if (manifest) {
    const entries = Object.values(manifest)
      .map((value) => value as ManifestRecord | null)
      .filter((record): record is { file: string; isEntry?: boolean } =>
        Boolean(record?.isEntry && record.file?.endsWith(".js"))
      )
      .map((record) => record.file.replace(/^\/+/, ""))
      .filter((file) => !routeChunks.has(file));

    if (entries.length > 0) {
      // Sorted for determinism across platforms. Ties are only possible when a
      // project declares several JS entries, where any is as good as another.
      return "/" + entries.sort()[0];
    }
  }

  const assetsDir = path.join(outputDir, "assets");
  if (fs.existsSync(assetsDir)) {
    const candidates = fs
      .readdirSync(assetsDir)
      .filter((name) => name.startsWith("index-") && name.endsWith(".js"))
      .filter((name) => !routeChunks.has(`assets/${name}`))
      .sort();

    if (candidates.length > 0) {
      return `/assets/${candidates[candidates.length - 1]}`;
    }
  }

  const indexPath = path.join(outputDir, "index.html");
  if (!fs.existsSync(indexPath)) {
    return null;
  }

  const html = fs.readFileSync(indexPath, "utf-8");
  const match = html.match(/<script[^>]*type="module"[^>]*src="([^"]+)"[^>]*><\/script>/i);
  return match?.[1] || null;
}
