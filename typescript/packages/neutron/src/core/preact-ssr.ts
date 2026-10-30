import * as fs from "node:fs";
import * as path from "node:path";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";

/**
 * Single physical Preact graph for Vite (client + SSR) and native SSG.
 *
 * Why this exists
 * ---------------
 * 1. Apps usually declare `preact` but not `preact-render-to-string` (that ships
 *    on @neutron-build/core). Under pnpm the renderer is unresolvable from the
 *    app root; a native fallback then binds a second `options` object and hooks
 *    crash with `reading '__H'`.
 * 2. Aliasing bare `preact` to the package root alone is not enough for the
 *    *client*: Vite joins subpaths onto the root and bypasses package
 *    `"exports"`, so `preact/jsx-dev-runtime` becomes `<root>/jsx-dev-runtime`
 *    (no such directory — the export only maps to jsx-runtime's file). Dev
 *    hydration dies while SSR still works.
 *
 * Strategy
 * --------
 * Resolve absolute ESM entry files (app first, framework second). Emit Vite
 * aliases with **subpaths before** bare `preact`, so the first-match scan never
 * treats `preact/jsx-dev-runtime` as a child of the package root.
 */
export interface PreactSsrResolution {
  /** Absolute ESM entry files keyed by bare package id. */
  paths: {
    preact: string;
    "preact/hooks": string;
    "preact/jsx-runtime": string;
    "preact/jsx-dev-runtime": string;
    "preact/compat": string;
    "preact-render-to-string": string;
    /**
     * ESM entry of the streaming renderer (`renderToReadableStream`), or
     * undefined when the resolved preact-render-to-string does not export
     * `./stream` (older versions). Optional so `resolvePreactSsr` keeps
     * working against packages that predate the subpath.
     */
    "preact-render-to-string/stream"?: string;
  };
  /**
   * Package roots for bare ids (so Node/Vite can still honor package
   * `"exports"` for subpaths that are not explicitly file-aliased).
   * `"preact/compat"` is the preact/compat package root (preact ships it
   * as a nested package with its own `"exports"` map covering the React
   * subpath surface: ./client, ./server, ./jsx-runtime, ./scheduler). If
   * the installed preact does not ship a nested package.json there, it
   * falls back to the compat entry FILE (alias ordering still applies;
   * unlisted react/* subpaths then fail resolution as before). Prefer
   * {@link vitePreactAliases} over this map when configuring Vite — order
   * matters.
   */
  packageRoots: {
    preact: string;
    "preact-render-to-string": string;
    "preact/compat": string;
  };
  /** Bare ids that must go through Vite's SSR graph (not native node). */
  noExternal: string[];
}

export interface ViteAliasEntry {
  find: string;
  replacement: string;
}

const PREACT_IDS = [
  "preact",
  "preact/hooks",
  "preact/jsx-runtime",
  "preact/jsx-dev-runtime",
  "preact/compat",
  "preact-render-to-string",
] as const;

/** Subpath ids that must be file-aliased (export-map targets, not directories). */
const PREACT_SUBPATH_ALIASES = [
  "preact/jsx-dev-runtime",
  "preact/jsx-runtime",
  "preact/hooks",
  "preact/compat",
] as const;

type PreactId = (typeof PREACT_IDS)[number];

/**
 * Resolve Preact package paths for an app rooted at `appRoot`.
 *
 * Lookup order per id:
 * 1. App `package.json` (so the app's declared preact wins)
 * 2. Each `from` package (framework packages that ship preact / RTS)
 * 3. This module's own dependency graph (core)
 */
export function resolvePreactSsr(
  appRoot: string,
  options: { from?: string[] } = {}
): PreactSsrResolution {
  const roots: string[] = [];

  const appPkg = path.join(appRoot, "package.json");
  if (fs.existsSync(appPkg)) {
    roots.push(appPkg);
  }

  for (const entry of options.from ?? []) {
    if (!entry) continue;
    if (entry.endsWith("package.json") && fs.existsSync(entry)) {
      roots.push(entry);
      continue;
    }
    try {
      if (path.isAbsolute(entry) && fs.existsSync(path.join(entry, "package.json"))) {
        roots.push(path.join(entry, "package.json"));
        continue;
      }
      const req = createRequire(appPkg);
      roots.push(req.resolve(`${entry}/package.json`));
    } catch {
      // ignore unreachable roots
    }
  }

  try {
    const selfReq = createRequire(import.meta.url);
    roots.push(selfReq.resolve("../../package.json"));
  } catch {
    // source vs dist layout
  }

  const seen = new Set<string>();
  const uniqueRoots = roots.filter((r) => {
    const abs = path.resolve(r);
    if (seen.has(abs) || !fs.existsSync(abs)) return false;
    seen.add(abs);
    return true;
  });

  const paths = {} as PreactSsrResolution["paths"];

  for (const id of PREACT_IDS) {
    const resolved = resolveId(id, uniqueRoots);
    if (!resolved) {
      throw new Error(
        `[Neutron] Cannot resolve "${id}" for SSR. Install preact in the app, ` +
          `and ensure @neutron-build/core (or @neutron-build/cli) is installed ` +
          `so preact-render-to-string is available.`
      );
    }
    paths[id] = resolved;
  }

  // The streaming renderer is optional: resolved from the same roots, but a
  // preact-render-to-string without the `./stream` export must not fail the
  // whole resolution — callers fall back to the buffered renderer.
  const streamEntry = resolveId("preact-render-to-string/stream", uniqueRoots);
  if (streamEntry) {
    paths["preact-render-to-string/stream"] = streamEntry;
  }

  // Pin every preact/* file to the app's preact package so RTS (often resolved
  // from core) cannot pull a second physical copy.
  const preactRoot = packageRootFromEntry(paths.preact, "preact");
  if (preactRoot) {
    for (const id of PREACT_SUBPATH_ALIASES) {
      try {
        paths[id] = preferEsmEntry(
          createRequire(path.join(preactRoot, "package.json")).resolve(id)
        );
      } catch {
        // keep resolveId result
      }
    }
    paths.preact = preferEsmEntry(
      createRequire(path.join(preactRoot, "package.json")).resolve("preact")
    );
  }

  const rtsRoot =
    packageRootFromEntry(paths["preact-render-to-string"], "preact-render-to-string") ??
    path.dirname(paths["preact-render-to-string"]);

  // preact/compat package root for BARE react/react-dom alias targets (see
  // vitePreactAliases). Never let the walk land on the preact package root
  // itself — aliasing react → <preact root> would silently give react apps
  // plain preact's entry (no forwardRef/memo/createPortal). If compat has no
  // nested package.json (layout change), fall back to the entry file, which
  // keeps bare-react semantics at status quo.
  const compatRoot = nearestPackageRoot(paths["preact/compat"]);

  return {
    paths,
    packageRoots: {
      preact: preactRoot ?? path.dirname(paths.preact),
      "preact-render-to-string": rtsRoot,
      "preact/compat":
        compatRoot && compatRoot !== preactRoot
          ? compatRoot
          : paths["preact/compat"],
    },
    noExternal: [
      "preact",
      "preact/hooks",
      "preact/jsx-runtime",
      "preact/jsx-dev-runtime",
      "preact/compat",
      "preact-render-to-string",
      "@neutron-build/core",
    ],
  };
}

/**
 * Ordered Vite `resolve.alias` entries.
 *
 * Subpaths are listed before bare `preact`. Vite/rollup alias matching is
 * first-match and treats a string find as a prefix (`preact` matches
 * `preact/hooks`); putting the bare package last is load-bearing for
 * `preact/jsx-dev-runtime` under the client optimizer.
 *
 * The same rule governs the react-compat (runtime) aliases: entries are
 * sorted longest-find-first so every `react/*` / `react-dom/*` subpath
 * precedes its bare parent. Bare react-family finds resolve to the
 * preact/compat package ROOT rather than its entry file, mirroring bare
 * `preact`: an unlisted react subpath (e.g. `react/client`, or one a newer
 * React adds) then joins a real directory and resolves through compat's own
 * `"exports"` map instead of dying on `<compat.mjs>/<subpath>` ("Not a
 * directory", os error 20).
 */
export function vitePreactAliases(
  resolution: PreactSsrResolution,
  runtimeAliases?: Record<string, string>
): ViteAliasEntry[] {
  const entries: ViteAliasEntry[] = [];
  const used = new Set<string>();

  const push = (find: string, replacement: string) => {
    if (used.has(find)) return;
    used.add(find);
    entries.push({ find, replacement });
  };

  // 1. Export-map / subpath files — must win over bare `preact`.
  for (const id of PREACT_SUBPATH_ALIASES) {
    push(id, resolution.paths[id]);
  }

  // 2. Runtime (react-compat) aliases, rewritten onto absolute preact paths.
  //    Longest find FIRST: Vite matches string aliases by prefix in array
  //    order, and `resolveRuntimeAliases` deliberately lists bare
  //    `react`/`react-dom` before their subpaths — without the sort, every
  //    subpath import (react/jsx-runtime above all: it is what automatic JSX
  //    emits) is rewritten onto the bare entry's replacement and the client
  //    build fails with ENOTDIR for every react-compat app.
  if (runtimeAliases) {
    const runtimeEntries = Object.entries(runtimeAliases).sort(
      (a, b) => b[0].length - a[0].length
    );
    for (const [find, target] of runtimeEntries) {
      if (target === "preact") {
        push(find, resolution.packageRoots.preact);
      } else if (target === "preact-render-to-string") {
        push(find, resolution.packageRoots["preact-render-to-string"]);
      } else if ((PREACT_SUBPATH_ALIASES as readonly string[]).includes(target)) {
        if (target === "preact/compat" && !find.includes("/")) {
          // Bare react-family find → compat PACKAGE ROOT, not the entry
          // file: prefix matches on unlisted react/* subpaths then join a
          // real directory (see docblock).
          push(find, resolution.packageRoots["preact/compat"]);
        } else {
          push(find, resolution.paths[target as (typeof PREACT_SUBPATH_ALIASES)[number]]);
        }
      } else {
        push(find, target);
      }
    }
  }

  // 3. Renderer package root (pnpm: not hoisted to the app). The /stream
  //    FILE alias must precede it: string aliases prefix-match, so the bare
  //    root would rewrite the subpath onto `<rts root>/stream` (not a file)
  //    and break every runner/bundler load of the streaming renderer.
  if (resolution.paths["preact-render-to-string/stream"]) {
    push(
      "preact-render-to-string/stream",
      resolution.paths["preact-render-to-string/stream"]
    );
  }
  push(
    "preact-render-to-string",
    resolution.packageRoots["preact-render-to-string"]
  );

  // 4. Bare preact LAST — package root so unlisted subpaths still use exports.
  push("preact", resolution.packageRoots.preact);

  return entries;
}

/**
 * @deprecated Prefer {@link vitePreactAliases} (ordered). Kept for call sites
 * that only need a flat map; key order is subpaths-then-bare but is easy to
 * scramble under object merges.
 */
export function mergePreactAliases(
  preact: PreactSsrResolution,
  runtimeAliases?: Record<string, string>
): Record<string, string> {
  const ordered = vitePreactAliases(preact, runtimeAliases);
  const map: Record<string, string> = {};
  for (const { find, replacement } of ordered) {
    map[find] = replacement;
  }
  return map;
}

/**
 * The streaming renderer signature shared by every load path (bundled
 * import in render-app-route, {@link importPreactSsr}, and the Vite
 * SSR-graph legs the start server uses).
 */
export type StreamRenderFn = (
  element: import("preact").VNode
) => ReadableStream<Uint8Array> & { allReady?: Promise<void> };

/**
 * Dynamically import preact + preact-render-to-string from the resolved paths.
 * Used by non-Vite paths (renderStatic) so they share the app's preact.
 *
 * Also imports the streaming renderer (`./stream`) when the resolution has
 * one; `renderToReadableStream` is undefined for preact-render-to-string
 * builds without the subpath, and callers fall back to the buffered
 * `renderToString`.
 */
export async function importPreactSsr(resolution: PreactSsrResolution): Promise<{
  h: typeof import("preact").h;
  renderToString: typeof import("preact-render-to-string").renderToString;
  renderToReadableStream?: StreamRenderFn;
  preact: typeof import("preact");
}> {
  const preactHref = pathToFileURL(resolution.paths.preact).href;
  const hooksHref = pathToFileURL(resolution.paths["preact/hooks"]).href;
  const rtsHref = pathToFileURL(
    preferEsmEntry(resolution.paths["preact-render-to-string"])
  ).href;

  const preact = (await importEsm(
    preactHref,
    resolution.paths.preact,
    "preact"
  )) as typeof import("preact");
  await importEsm(hooksHref, resolution.paths["preact/hooks"], "preact/hooks");
  const rts = (await importEsm(
    rtsHref,
    resolution.paths["preact-render-to-string"],
    "preact-render-to-string"
  )) as typeof import("preact-render-to-string") & {
    default?: { renderToString?: typeof import("preact-render-to-string").renderToString };
  };
  const renderToString = rts.renderToString ?? rts.default?.renderToString;
  if (!preact.h || !renderToString) {
    throw new Error("[Neutron] Resolved preact / preact-render-to-string exports are incomplete.");
  }

  let renderToReadableStream: StreamRenderFn | undefined;
  const streamEntry = resolution.paths["preact-render-to-string/stream"];
  if (streamEntry) {
    try {
      const streamModule = (await importEsm(
        pathToFileURL(streamEntry).href,
        streamEntry,
        "preact-render-to-string/stream"
      )) as {
        renderToReadableStream?: StreamRenderFn;
        default?: { renderToReadableStream?: StreamRenderFn };
      };
      renderToReadableStream =
        streamModule.renderToReadableStream ??
        streamModule.default?.renderToReadableStream;
    } catch {
      // Optional leg: buffered rendering stays available.
    }
  }

  return { h: preact.h, renderToString, renderToReadableStream, preact };
}

async function importEsm(
  primaryHref: string,
  absolutePath: string,
  bareId: string
): Promise<unknown> {
  try {
    return await import(primaryHref);
  } catch {
    try {
      return await import(pathToFileURL(preferEsmEntry(absolutePath)).href);
    } catch {
      return await import(bareId);
    }
  }
}

function resolveId(id: string, roots: string[]): string | null {
  for (const root of roots) {
    try {
      return preferEsmEntry(createRequire(root).resolve(id));
    } catch {
      // try next root
    }
  }
  try {
    return preferEsmEntry(createRequire(import.meta.url).resolve(id));
  } catch {
    return null;
  }
}

/**
 * createRequire() returns the CJS "require" condition (preact/dist/preact.js).
 * Vite's SSR module-runner evaluates aliased packages as ESM, so CJS hits
 * `exports is not defined`. Prefer the package's ESM build when present.
 */
function preferEsmEntry(resolved: string): string {
  if (resolved.endsWith(".mjs")) {
    return resolved;
  }
  if (resolved.endsWith(".js")) {
    const mjs = resolved.slice(0, -3) + ".mjs";
    if (fs.existsSync(mjs)) {
      return mjs;
    }
    const moduleJs = resolved.slice(0, -3) + ".module.js";
    if (fs.existsSync(moduleJs)) {
      return moduleJs;
    }
  }
  return resolved;
}

function packageRootFromEntry(entryFile: string, name: string): string | null {
  let dir = path.dirname(entryFile);
  for (let i = 0; i < 8; i++) {
    if (fs.existsSync(path.join(dir, "package.json"))) {
      try {
        const pkg = JSON.parse(fs.readFileSync(path.join(dir, "package.json"), "utf-8")) as {
          name?: string;
        };
        if (pkg.name === name) return dir;
      } catch {
        // keep walking
      }
    }
    const parent = path.dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }
  return null;
}

/**
 * Nearest ancestor directory of `entryFile` containing a package.json,
 * regardless of the package name (preact/compat is a nested package whose
 * name, "preact-compat", does not contain "preact/compat"). Null if the
 * walk hits the filesystem root.
 */
function nearestPackageRoot(entryFile: string): string | null {
  let dir = path.dirname(entryFile);
  for (let i = 0; i < 8; i++) {
    if (fs.existsSync(path.join(dir, "package.json"))) {
      return dir;
    }
    const parent = path.dirname(dir);
    if (parent === dir) return null;
    dir = parent;
  }
  return null;
}

