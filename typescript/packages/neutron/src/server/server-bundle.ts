import * as fs from "node:fs";
import * as path from "node:path";
import { pathToFileURL } from "node:url";
import type { NeutronRuntime } from "../config.js";

/**
 * Prebuilt server bundle: every module the production server would otherwise
 * load through a Vite SSR runtime (route modules, layouts, not-found pages,
 * resource routes, global middleware) plus the renderer pair, compiled by
 * `neutron-ts build` into ONE module graph. `createServer` loads it natively
 * when the dist carries one, so `start` neither transforms source at boot nor
 * constructs a Vite server.
 *
 * The renderer ids are re-exported from the same graph as the routes, for
 * the reason {@link RouteRenderer} documents: hooks and renderer must share
 * one Preact `options` object, and router providers must come from the same
 * module instance as their consumers.
 */

/** Written to the dist root by `neutron-ts build`. */
export const SERVER_BUNDLE_MANIFEST = ".neutron-server-bundle.json";
/** Bumped when the entry's export shape changes; a mismatch falls back to Vite. */
export const SERVER_BUNDLE_FORMAT = 1;

/** Bare ids the server resolves its renderer (and island handler) from. */
export const SERVER_BUNDLE_RENDERER_IDS = [
  "preact",
  "preact-render-to-string",
  "preact-render-to-string/stream",
  "@neutron-build/core",
] as const;

export interface ServerBundleManifest {
  format: number;
  /** Entry module, relative to the dist dir (posix separators). */
  entry: string;
  runtime: NeutronRuntime;
}

export interface LoadedServerBundle {
  /** Absolute path of the entry module. */
  entryPath: string;
  /** Lazy module loaders keyed by source path relative to the app root. */
  modules: Record<string, () => Promise<unknown>>;
  /** Renderer-side modules keyed by bare id ({@link SERVER_BUNDLE_RENDERER_IDS}). */
  renderer: Record<string, unknown>;
}

/** Bundle key of a source file: its path relative to the app root, posix. */
export function serverBundleKey(rootDir: string, file: string): string {
  return path.relative(rootDir, path.resolve(rootDir, file)).split(path.sep).join("/");
}

/**
 * Source of the bundle entry `neutron-ts build` compiles. Route modules are
 * dynamic imports, so each loads on its first request like it does through
 * the Vite runtime: a module's top-level code still runs lazily, and a module
 * that fails to load fails its route, not the boot.
 */
export function generateServerBundleEntry(options: {
  entryPath: string;
  rootDir: string;
  files: string[];
  runtime: NeutronRuntime;
}): string {
  const entryDir = path.dirname(options.entryPath);
  const lines: string[] = [];
  SERVER_BUNDLE_RENDERER_IDS.forEach((id, index) => {
    lines.push(`import * as __renderer${index} from ${JSON.stringify(id)};`);
  });
  lines.push("");
  lines.push(`export const format = ${SERVER_BUNDLE_FORMAT};`);
  lines.push(`export const runtime = ${JSON.stringify(options.runtime)};`);
  lines.push("export const renderer = {");
  SERVER_BUNDLE_RENDERER_IDS.forEach((id, index) => {
    lines.push(`  ${JSON.stringify(id)}: __renderer${index},`);
  });
  lines.push("};");
  lines.push("export const modules = {");
  const seen = new Set<string>();
  for (const file of options.files) {
    const key = serverBundleKey(options.rootDir, file);
    if (seen.has(key)) continue;
    seen.add(key);
    let specifier = path.relative(entryDir, path.resolve(options.rootDir, file)).split(path.sep).join("/");
    if (!specifier.startsWith(".")) specifier = `./${specifier}`;
    lines.push(`  ${JSON.stringify(key)}: () => import(${JSON.stringify(specifier)}),`);
  }
  lines.push("};");
  return lines.join("\n") + "\n";
}

/**
 * Load the dist's server bundle, or return null to use the Vite runtime.
 *
 * Null (with a warning) when the bundle cannot stand in for the source: built
 * by another format version, for another runtime, or missing a module the
 * server needs (a route added after the build). Null without a warning when
 * there is no bundle at all — dev, tests and dists from older CLIs. A bundle
 * that is present and current but fails to import is a startup error: the
 * Vite fallback would load the same broken graph more slowly.
 */
export async function loadServerBundle(options: {
  rootDir: string;
  distDir: string;
  runtime: NeutronRuntime;
  requiredFiles: string[];
}): Promise<LoadedServerBundle | null> {
  const manifestPath = path.join(options.distDir, SERVER_BUNDLE_MANIFEST);
  if (!fs.existsSync(manifestPath)) {
    return null;
  }
  const fallback = (reason: string): null => {
    console.warn(
      `[Neutron] Not using the server bundle (${reason}); route modules will be ` +
        "transformed through Vite at runtime. Run `neutron-ts build` to rebuild it."
    );
    return null;
  };

  let manifest: ServerBundleManifest;
  try {
    manifest = JSON.parse(fs.readFileSync(manifestPath, "utf-8")) as ServerBundleManifest;
  } catch (error) {
    return fallback(`${SERVER_BUNDLE_MANIFEST} is unreadable: ${String(error)}`);
  }
  if (manifest.format !== SERVER_BUNDLE_FORMAT || typeof manifest.entry !== "string") {
    return fallback(`format ${String(manifest.format)}, this server reads ${SERVER_BUNDLE_FORMAT}`);
  }
  if (manifest.runtime !== options.runtime) {
    return fallback(`built for runtime "${manifest.runtime}", configured "${options.runtime}"`);
  }

  const entryPath = path.resolve(options.distDir, manifest.entry);
  let loaded: { format?: unknown; modules?: unknown; renderer?: unknown };
  try {
    loaded = (await import(pathToFileURL(entryPath).href)) as typeof loaded;
  } catch (error) {
    throw new Error(
      `Failed to load the server bundle ${path.relative(options.rootDir, entryPath)}: ` +
        `${String(error)}. Rebuild with \`neutron-ts build\`, or delete ` +
        `${path.relative(options.rootDir, manifestPath)} to transform routes through Vite.`
    );
  }
  if (
    loaded.format !== SERVER_BUNDLE_FORMAT ||
    !loaded.modules ||
    typeof loaded.modules !== "object" ||
    !loaded.renderer ||
    typeof loaded.renderer !== "object"
  ) {
    return fallback(`${manifest.entry} does not have the expected exports`);
  }

  const modules = loaded.modules as Record<string, () => Promise<unknown>>;
  const missing = options.requiredFiles
    .map((file) => serverBundleKey(options.rootDir, file))
    .filter((key) => typeof modules[key] !== "function");
  if (missing.length > 0) {
    const shown = missing.slice(0, 5).join(", ");
    const more = missing.length > 5 ? ` and ${missing.length - 5} more` : "";
    return fallback(`it does not contain ${shown}${more}`);
  }

  return {
    entryPath,
    modules,
    renderer: loaded.renderer as Record<string, unknown>,
  };
}
