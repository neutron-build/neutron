import * as fs from "node:fs";
import * as path from "node:path";
import { isBuiltin } from "node:module";

/**
 * A self-contained server bundle's production dependency set, as real
 * directories inside the output (the docker preset: the image context is
 * dist/ alone, with no node_modules beside it).
 *
 * The bundle keeps @neutron-build/core's own dependencies external, as the
 * `start` bundle does, because under pnpm with `resolve.preserveSymlinks`
 * they cannot be bundled at all (gray-matter's section-matter is invisible
 * from core's linked path). What the bundle imports at runtime is shipped
 * here instead, `pnpm deploy`-style: each package directory is copied with
 * symlinks dereferenced, and its own `dependencies`, `optionalDependencies`
 * and installed peers follow, resolved from the package's REAL directory as
 * Node resolves them. Packages are hoisted to `<outputDir>/node_modules`
 * unless a different copy of the same name is already visible on the
 * dependent's resolution path, in which case they nest under the dependent.
 */
export interface ShippedDependencyRoot {
  /** The package's real directory on the build machine. */
  realDir: string;
  /** Where it goes in the output (a `node_modules/<name>` directory). */
  dest: string;
}

/** Package name of a bare specifier (`@scope/pkg/sub` → `@scope/pkg`), or null for builtins/relative ids. */
export function bareSpecifierPackage(specifier: string): string | null {
  if (
    specifier.startsWith(".") ||
    specifier.startsWith("/") ||
    specifier.startsWith("\0") ||
    specifier.includes(":") ||
    isBuiltin(specifier)
  ) {
    return null;
  }
  const parts = specifier.split("/");
  return specifier.startsWith("@") ? parts.slice(0, 2).join("/") : parts[0];
}

/** The real directory of package `name` as Node would resolve it from `fromDir`. */
export function resolvePackageDir(fromDir: string, name: string): string | null {
  for (let dir = fromDir; ; dir = path.dirname(dir)) {
    if (path.basename(dir) !== "node_modules") {
      const candidate = path.join(dir, "node_modules", name);
      if (fs.existsSync(path.join(candidate, "package.json"))) {
        return fs.realpathSync(candidate);
      }
    }
    if (path.dirname(dir) === dir) return null;
  }
}

interface PackageManifest {
  name?: string;
  dependencies?: Record<string, string>;
  optionalDependencies?: Record<string, string>;
  peerDependencies?: Record<string, string>;
}

function readManifest(dir: string): PackageManifest {
  try {
    return JSON.parse(fs.readFileSync(path.join(dir, "package.json"), "utf-8")) as PackageManifest;
  } catch {
    return {};
  }
}

/**
 * Copy `roots` and their transitive production dependencies into the output.
 * `outputDir` bounds hoisting: nothing is placed above `<outputDir>/node_modules`.
 * Returns the number of package directories written.
 */
export function shipDependencies(outputDir: string, roots: ShippedDependencyRoot[]): number {
  const topDir = path.join(outputDir, "node_modules");
  // dest directory -> real directory copied there
  const placed = new Map<string, string>();
  const queue: Array<{ realDir: string; dest: string }> = [];

  const place = (realDir: string, dest: string): void => {
    if (placed.has(dest)) return;
    placed.set(dest, realDir);
    fs.rmSync(dest, { recursive: true, force: true });
    fs.mkdirSync(path.dirname(dest), { recursive: true });
    const ownModules = path.join(realDir, "node_modules");
    fs.cpSync(realDir, dest, {
      recursive: true,
      dereference: true,
      // A package's own node_modules is pnpm's (or npm's nested) install, not
      // its contents; what it needs is resolved and placed below.
      filter: (source) => source !== ownModules && !source.startsWith(ownModules + path.sep),
    });
    queue.push({ realDir, dest });
  };

  for (const root of roots) {
    place(root.realDir, root.dest);
  }

  while (queue.length > 0) {
    const { realDir, dest } = queue.shift()!;
    const manifest = readManifest(realDir);
    const names = new Set([
      ...Object.keys(manifest.dependencies ?? {}),
      ...Object.keys(manifest.optionalDependencies ?? {}),
      ...Object.keys(manifest.peerDependencies ?? {}),
    ]);
    for (const name of names) {
      // Not installed: an optional dependency or peer this install skipped.
      const depReal = resolvePackageDir(realDir, name);
      if (!depReal) continue;

      // What Node would find from the copy: the nearest placed `name` on the
      // dependent's resolution path inside the output.
      let visible: string | null = null;
      for (let dir = dest; ; dir = path.dirname(dir)) {
        if (path.basename(dir) !== "node_modules") {
          const slot = path.join(dir, "node_modules", name);
          if (placed.has(slot)) {
            visible = slot;
            break;
          }
        }
        if (dir === outputDir || path.dirname(dir) === dir) break;
      }
      if (visible) {
        if (placed.get(visible) === depReal) continue;
        place(depReal, path.join(dest, "node_modules", name));
      } else {
        place(depReal, path.join(topDir, name));
      }
    }
  }

  return placed.size;
}
