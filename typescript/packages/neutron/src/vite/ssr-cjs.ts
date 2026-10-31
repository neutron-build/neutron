/**
 * CommonJS in Vite's SSR module runner.
 *
 * The runner evaluates ESM only: a CommonJS module it inlines — every
 * dependency under react-compat's blanket `ssr.noExternal`, or a listed one
 * otherwise — dies on its first `require`/`module`/`exports`. Leaving such a
 * package to Node instead is right for most of them, but not for the
 * React-ecosystem ones: natively, their `require("react")` loads real React
 * beside the Preact the page renders with. So each inlined CommonJS module is
 * rewritten to ESM here, one of two ways, by its package:
 *
 * - **Native** — a package that declares no react/react-dom/preact
 *   dependency is loaded by Node (`createRequire`), with Node's CommonJS
 *   semantics for everything beneath it.
 * - **In-graph** — a React-ecosystem package (`use-sync-external-store`,
 *   `react-is`-style shims, UMD builds) runs in the runner with its own
 *   `require`: requests for the React family, for its own relative files and
 *   for other React-ecosystem packages are hoisted to imports, so they go
 *   through Vite's resolver and its react → preact/compat aliases and share
 *   the page's Preact instance; anything else is required natively.
 *
 * Either way the module's `module.exports` is its default export (its
 * `default` when it was compiled from ESM, `__esModule`), and its keys become
 * named exports on the runner's module object. Builds are untouched: Rolldown
 * bundles CommonJS itself.
 */
import * as fs from "node:fs";
import * as path from "node:path";
import { createRequire } from "node:module";

const REACT_FAMILY = /^(react|react-dom|preact)(\/|$)/;
const REQUIRE_CALL = /\brequire\s*\(\s*(['"])([^'"\n]+)\1\s*\)/g;

interface PackageInfo {
  dir: string;
  reactDependent: boolean;
}

const manifestCache = new Map<string, Record<string, unknown> | null>();

function readManifest(dir: string): Record<string, unknown> | null {
  let manifest = manifestCache.get(dir);
  if (manifest === undefined) {
    try {
      manifest = JSON.parse(fs.readFileSync(path.join(dir, "package.json"), "utf-8")) as Record<string, unknown>;
    } catch {
      manifest = null;
    }
    manifestCache.set(dir, manifest);
  }
  return manifest;
}

/** The nearest package.json above `file` (that has a `name`, when `named`). */
function nearestManifest(file: string, named: boolean): { dir: string; manifest: Record<string, unknown> } | null {
  for (let dir = path.dirname(file); ; dir = path.dirname(dir)) {
    const manifest = readManifest(dir);
    if (manifest && (!named || typeof manifest.name === "string")) return { dir, manifest };
    if (path.dirname(dir) === dir) return null;
  }
}

function nearestPackage(file: string): PackageInfo | null {
  const found = nearestManifest(file, true);
  if (!found) return null;
  const { manifest } = found;
  const declared = {
    ...(manifest.dependencies as Record<string, string> | undefined),
    ...(manifest.peerDependencies as Record<string, string> | undefined),
    ...(manifest.optionalDependencies as Record<string, string> | undefined),
  };
  return {
    dir: found.dir,
    reactDependent:
      REACT_FAMILY.test(String(manifest.name)) || Object.keys(declared).some((name) => REACT_FAMILY.test(name)),
  };
}

const ESM_SYNTAX = /^\s*(import(\s+[\w*{]|\s*["'{*])|export(\s+[\w*{]|\s*[{*])|import\.meta\b)/m;
const CJS_SYNTAX = /\b(module\.exports|exports\.[\w$]+\s*=|exports\[|require\s*\(|Object\.defineProperty\(\s*exports\b)/;

/** Whether an inlined node_modules file is CommonJS the runner cannot evaluate. */
export function isCommonJsModule(file: string, code: string): boolean {
  if (file.endsWith(".cjs")) return true;
  if (!/\.(js|cjs)$/.test(file)) return false;
  // Node reads a .js file's format from the nearest package.json, named or not.
  if (nearestManifest(file, false)?.manifest.type === "module") return false;
  if (ESM_SYNTAX.test(code)) return false;
  return CJS_SYNTAX.test(code);
}

const EXPORTS_EPILOGUE = `
const __neutron_cjs_default__ = __neutron_cjs__ && __neutron_cjs__.__esModule && "default" in __neutron_cjs__ ? __neutron_cjs__.default : __neutron_cjs__;
export default __neutron_cjs_default__;
if (typeof __vite_ssr_exports__ !== "undefined") {
  Object.defineProperty(__vite_ssr_exports__, "module.exports", { configurable: true, get: () => __neutron_cjs__ });
  if (__neutron_cjs__ !== null && (typeof __neutron_cjs__ === "object" || typeof __neutron_cjs__ === "function")) {
    for (const key of Object.keys(__neutron_cjs__)) {
      if (key === "default" || key in __vite_ssr_exports__) continue;
      Object.defineProperty(__vite_ssr_exports__, key, { enumerable: true, configurable: true, get: () => __neutron_cjs__[key] });
    }
  }
}
`;

/**
 * ESM for a CommonJS module the SSR runner would inline, or null when the
 * module is not CommonJS.
 */
export function transformSsrCommonJs(code: string, file: string): string | null {
  if (!isCommonJsModule(file, code)) return null;
  const pkg = nearestPackage(file);
  const fileJson = JSON.stringify(file);
  if (!pkg?.reactDependent) {
    return `import { createRequire as __neutron_createRequire__ } from "node:module";
const __neutron_cjs__ = __neutron_createRequire__(${fileJson})(${fileJson});
${EXPORTS_EPILOGUE}`;
  }

  const nativeRequire = createRequire(file);
  const hoisted = new Map<string, string>();
  for (const match of code.matchAll(REQUIRE_CALL)) {
    const request = match[2];
    if (hoisted.has(request) || !shouldHoist(request, nativeRequire)) continue;
    hoisted.set(request, `__neutron_req${hoisted.size}__`);
  }
  const imports = [...hoisted].map(([request, local]) => `import * as ${local} from ${JSON.stringify(request)};`);
  const table = [...hoisted].map(([request, local]) => `  ${JSON.stringify(request)}: ${local},`);
  return `${imports.join("\n")}
import { createRequire as __neutron_createRequire__ } from "node:module";
const __neutron_native_require__ = __neutron_createRequire__(${fileJson});
const __neutron_requires__ = {
${table.join("\n")}
};
function __neutron_require__(request) {
  if (Object.prototype.hasOwnProperty.call(__neutron_requires__, request)) {
    const ns = __neutron_requires__[request];
    return "module.exports" in ns ? ns["module.exports"] : ns;
  }
  return __neutron_native_require__(request);
}
__neutron_require__.resolve = (request, options) => __neutron_native_require__.resolve(request, options);
const __neutron_module__ = { exports: {} };
(function (exports, require, module, __filename, __dirname) {
${code.replace(/^#!.*/, "")}
}).call(__neutron_module__.exports, __neutron_module__.exports, __neutron_require__, __neutron_module__, ${fileJson}, ${JSON.stringify(path.dirname(file))});
const __neutron_cjs__ = __neutron_module__.exports;
${EXPORTS_EPILOGUE}`;
}

function shouldHoist(request: string, nativeRequire: NodeRequire): boolean {
  if (REACT_FAMILY.test(request)) return true;
  if (request.startsWith("node:")) return false;
  let resolved: string;
  try {
    resolved = nativeRequire.resolve(request);
  } catch {
    // Optional dependencies (`try { require("x") } catch {}`): resolve at run time.
    return false;
  }
  if (!path.isAbsolute(resolved)) return false; // builtin
  if (request.startsWith(".")) return true;
  return nearestPackage(resolved)?.reactDependent === true;
}
