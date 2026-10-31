import * as fs from "node:fs";
import * as path from "node:path";
import type { MiddlewareFn } from "./types.js";
import { normalizeMiddlewareExport } from "./middleware.js";

// The app's optional global middleware: <rootDir>/src/middleware.{ts,tsx,js,mjs}.
// Shared by the production server and the dev server so both find the same
// file and accept the same export shapes.
export const GLOBAL_MIDDLEWARE_CANDIDATES = [
  "src/middleware.ts",
  "src/middleware.tsx",
  "src/middleware.js",
  "src/middleware.mjs",
];

/**
 * Absolute path of the app's global middleware file, or null.
 *
 * Split out from {@link loadGlobalMiddleware} because the answer is needed
 * BEFORE the SSR runtime is created — its presence is part of deciding whether
 * to create one at all. A plain fs check, so it costs nothing at boot. A-022.
 */
export function findGlobalMiddlewareFile(rootDir: string): string | null {
  for (const rel of GLOBAL_MIDDLEWARE_CANDIDATES) {
    const abs = path.join(rootDir, rel);
    if (fs.existsSync(abs)) {
      return abs;
    }
  }
  return null;
}

export async function loadGlobalMiddleware(
  loadModule: (id: string) => Promise<unknown>,
  absolutePath: string
): Promise<MiddlewareFn[]> {
  let mod: { default?: unknown; middleware?: unknown };
  try {
    mod = (await loadModule(absolutePath)) as {
      default?: unknown;
      middleware?: unknown;
    };
  } catch (error) {
    // A file that exists but cannot be imported is a startup error (TS-01):
    // continuing without it silently removes whatever gate it carried.
    throw new Error(
      `Failed to load global middleware ${path.basename(absolutePath)}: ${String(error)}`
    );
  }
  // Documented form: `export const middleware: MiddlewareFn[]`. Also
  // accept a single function (default or named) for ergonomics. An export
  // that exists but is not usable is likewise a startup error.
  const exported = mod.middleware ?? mod.default;
  try {
    return normalizeMiddlewareExport(exported);
  } catch {
    throw new TypeError(
      `Invalid global middleware ${path.basename(absolutePath)}: expected \`middleware\` or default ` +
        `export of a function or a nonempty array of functions (got ${typeof exported}).`
    );
  }
}
