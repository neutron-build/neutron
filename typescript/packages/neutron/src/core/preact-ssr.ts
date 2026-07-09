import * as fs from "node:fs";
import * as path from "node:path";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";

/**
 * Absolute package paths for the Preact SSR graph.
 *
 * Apps typically declare `preact` but not `preact-render-to-string` (that lives
 * on @neutron-build/core / @neutron-build/cli). Under pnpm the renderer is then
 * unresolvable from the app root, so bare `ssrLoadModule("preact-render-to-string")`
 * fails and a native fallback binds a *second* Preact `options` object — hooks
 * crash with `Cannot read properties of undefined (reading '__H')`.
 *
 * Resolve every SSR-critical id to one absolute path (app first, framework
 * second) and feed those into Vite `resolve.alias` so routes, hooks, core, and
 * the renderer share one instance.
 */
export interface PreactSsrResolution {
  /** Absolute filesystem paths keyed by bare package id. */
  paths: {
    preact: string;
    "preact/hooks": string;
    "preact/jsx-runtime": string;
    "preact/jsx-dev-runtime": string;
    "preact/compat": string;
    "preact-render-to-string": string;
  };
  /** Vite `resolve.alias` entries (same paths). */
  aliases: Record<string, string>;
  /** Bare ids that must go through Vite's SSR graph (not native node). */
  noExternal: string[];
}

const PREACT_IDS = [
  "preact",
  "preact/hooks",
  "preact/jsx-runtime",
  "preact/jsx-dev-runtime",
  "preact/compat",
  "preact-render-to-string",
] as const;

type PreactId = (typeof PREACT_IDS)[number];

/**
 * Resolve Preact SSR package paths for an app rooted at `appRoot`.
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
    // Treat as a package name or absolute path to a package root.
    try {
      if (path.isAbsolute(entry) && fs.existsSync(path.join(entry, "package.json"))) {
        roots.push(path.join(entry, "package.json"));
        continue;
      }
      // Resolve package.json of a dependency reachable from the app.
      const req = createRequire(appPkg);
      const resolved = req.resolve(`${entry}/package.json`);
      roots.push(resolved);
    } catch {
      // ignore unreachable from roots
    }
  }

  // Always end with this package (core) so RTS is findable even when the app
  // does not declare it. Walk up from this module to the package root.
  try {
    const selfReq = createRequire(import.meta.url);
    // dist/core/preact-ssr.js → ../../package.json; src/core → same relative.
    roots.push(selfReq.resolve("../../package.json"));
  } catch {
    // package layout may differ when running from source vs dist; ignore
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

  // Prefer the app's preact for every preact/* subpath so RTS (often resolved
  // from core) still imports the same physical preact the routes use.
  const preactRoot = packageRootFromEntry(paths.preact, "preact");
  if (preactRoot) {
    for (const id of [
      "preact/hooks",
      "preact/jsx-runtime",
      "preact/jsx-dev-runtime",
      "preact/compat",
    ] as const) {
      try {
        const sub = preferEsmEntry(
          createRequire(path.join(preactRoot, "package.json")).resolve(id)
        );
        paths[id] = sub;
      } catch {
        // keep whatever resolveId found
      }
    }
  }

  // Alias package *roots* only — never file entries.
  // - File aliases (dist/preact.js) make Vite evaluate CJS as ESM →
  //   `exports is not defined`, or break subpaths as `preact.mjs/hooks`.
  // - Package-root aliases let Vite honor package.json "exports" → ESM, and
  //   `preact/hooks` resolves under the same physical package.
  const rtsRoot = packageRootFromEntry(
    paths["preact-render-to-string"],
    "preact-render-to-string"
  );
  const aliases: Record<string, string> = {
    preact: preactRoot ?? paths.preact,
    "preact-render-to-string": rtsRoot ?? paths["preact-render-to-string"],
  };

  return {
    paths,
    aliases,
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
 * Merge Preact SSR aliases with any runtime aliases (e.g. react-compat).
 * Preact paths win for keys they own so react-dom/server → RTS still points
 * at the absolute RTS path.
 */
export function mergePreactAliases(
  preact: PreactSsrResolution,
  runtimeAliases?: Record<string, string>
): Record<string, string> {
  if (!runtimeAliases) {
    return { ...preact.aliases };
  }
  const merged: Record<string, string> = { ...runtimeAliases };
  // Rewrite runtime aliases that target bare preact packages to the absolute
  // package roots we resolved (e.g. react-dom/server → <rtsRoot>).
  for (const [key, value] of Object.entries(merged)) {
    if (value === "preact" || value.startsWith("preact/")) {
      // Map react → preact/compat to the preact package root; Vite then
      // resolves the /compat subpath via package exports when the alias is
      // a package root. For explicit subpath targets, use the file path.
      if (value === "preact") {
        merged[key] = preact.aliases.preact;
      } else if (value in preact.paths) {
        // Subpath file (hooks/compat/jsx) — keep absolute file so nested
        // pnpm can't pick a different preact.
        merged[key] = preact.paths[value as PreactId];
      }
    } else if (value === "preact-render-to-string") {
      merged[key] = preact.aliases["preact-render-to-string"];
    }
  }
  // Absolute preact graph always wins for its own keys.
  Object.assign(merged, preact.aliases);
  return merged;
}

/**
 * Dynamically import preact + preact-render-to-string from the resolved paths.
 * Used by non-Vite paths (renderStatic) so they share the app's preact.
 */
export async function importPreactSsr(resolution: PreactSsrResolution): Promise<{
  h: typeof import("preact").h;
  renderToString: typeof import("preact-render-to-string").renderToString;
  preact: typeof import("preact");
}> {
  // Prefer package-root import so Node honors "exports" → ESM. Falling back to
  // the absolute file path (already ESM-preferred via preferEsmEntry).
  const preactRoot = packageRootFromEntry(resolution.paths.preact, "preact");
  const rtsRoot = packageRootFromEntry(
    resolution.paths["preact-render-to-string"],
    "preact-render-to-string"
  );
  const preactHref = preactRoot
    ? pathToFileURL(path.join(preactRoot, "dist", "preact.mjs")).href
    : pathToFileURL(resolution.paths.preact).href;
  const hooksHref = preactRoot
    ? pathToFileURL(path.join(preactRoot, "hooks", "dist", "hooks.mjs")).href
    : pathToFileURL(resolution.paths["preact/hooks"]).href;
  const rtsHref = rtsRoot
    ? pathToFileURL(path.join(rtsRoot, "dist", "index.mjs")).href
    : pathToFileURL(resolution.paths["preact-render-to-string"]).href;

  // Fallback chain: package ESM file → preferEsm absolute path → bare import.
  const preact = (await importEsm(preactHref, resolution.paths.preact, "preact")) as typeof import("preact");
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
  return { h: preact.h, renderToString, preact };
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
      const resolved = createRequire(root).resolve(id);
      return preferEsmEntry(resolved);
    } catch {
      // try next root
    }
  }
  // Last resort: resolve from this module.
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
    // preact also ships *.module.js as the browser ESM build.
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
