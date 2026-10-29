// Node module hooks that make raw Node load packages the way the Vite build
// does, so the matrix measures library compatibility — not the gaps between
// bundler resolution and Node's stricter ESM rules.
//
// The harness exists to answer "does this React library work when react is
// aliased to preact/compat?" — the same substitution Neutron's Vite build
// makes for `runtime: "react-compat"`. A bundler deviates from plain Node ESM
// in well-defined ways, and without the deviations below, several real
// libraries fail inside Node for reasons that cannot occur under Vite:
//
// 1. One preact instance. Bundlers load each module once. Node loads a
//    package's ESM and CJS builds as two separate module instances with two
//    separate `options` objects. Any package whose CJS build `require("react")`
//    (tiptap and React Flow both reach react through the CJS
//    use-sync-external-store shim) ends up calling hooks on a second preact
//    whose hooks state is never initialized, and every component dies with
//    `Cannot read properties of undefined (reading '__H')` — the exact
//    false-failure shape `assertOnePreact` in run.mjs exists to prevent.
//    Forcing the `import` condition on every preact resolution makes CJS
//    consumers load the ESM dist via require(esm): one instance everywhere.
//    (Needs Node >= 22.12 for require(esm), >= 22.15 for registerHooks; CI
//    runs Node 22-latest and 24.)
//
// 2. Require-style resolution as a fallback. Node ESM demands exact file
//    paths; `require` (and every bundler) appends `.js`, resolves directory
//    imports to `index.js` or a nested package.json `main`. Excalidraw's dist
//    imports `roughjs/bin/rough` (file is rough.js), rough's own files import
//    `./canvas` extensionless, and `react-remove-scroll-bar/constants` is a
//    directory entrypoint with its own package.json — all fine under Vite,
//    all ERR_MODULE_NOT_FOUND / ERR_UNSUPPORTED_DIR_IMPORT under Node.
//
// 3. The `module` field, for imports made from inside node_modules. Vite's
//    default resolve.mainFields prefers `module` over `main`. When a CJS
//    package has an ESM sibling build, Node's CJS named-export lexer sometimes
//    cannot see the exports (`@excalidraw/laser-pointer`), which kills the
//    link step with "does not provide an export named". Scoped to
//    dependency-internal imports only: what the harness itself imports keeps
//    plain Node semantics, so existing rows and their recorded errors do not
//    move.
//
// 4. JSON modules without import attributes. Bundlers accept
//    `import x from "pkg/x.json"` bare; Node demands `with { type: "json" }`
//    and Excalidraw does not add it (open-color).
//
// None of this rescues a library from its own failures: a component that
// reads `window` during render (excalidraw, at module evaluation) still fails,
// and the matrix records the real error.
import module, { createRequire } from "node:module";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

export function registerBundlerParityHooks() {
  if (typeof module.registerHooks !== "function") {
    throw new Error(
      "compat-matrix needs module.registerHooks (Node >= 22.15) so CJS dependencies " +
        "and the harness share one preact instance; run with a current Node 22 or 24."
    );
  }

  // Files reached through rule 3: parsed as ESM whatever the nearest
  // package.json says, because the `module` field points at ESM by definition.
  const forceModuleFormat = new Set();

  const nearestManifest = (dir) => {
    while (true) {
      const manifestPath = join(dir, "package.json");
      if (existsSync(manifestPath)) {
        return { dir, manifest: JSON.parse(readFileSync(manifestPath, "utf-8")) };
      }
      const parent = dirname(dir);
      if (parent === dir) return null;
      dir = parent;
    }
  };

  module.registerHooks({
    resolve(specifier, context, nextResolve) {
      // Rule 1: one preact instance.
      if (specifier === "preact" || specifier.startsWith("preact/")) {
        const conditions = [...new Set([...(context.conditions ?? []), "import"])];
        return nextResolve(specifier, { ...context, conditions });
      }

      let resolved;
      try {
        resolved = nextResolve(specifier, context);
      } catch (error) {
        // Rule 2: fall back to require resolution semantics. Only reached
        // when ESM resolution has already failed, so it never overrides a
        // resolution Node would have made.
        if (
          (error?.code === "ERR_MODULE_NOT_FOUND" || error?.code === "ERR_UNSUPPORTED_DIR_IMPORT") &&
          context.parentURL?.startsWith("file:") &&
          !specifier.startsWith("node:")
        ) {
          try {
            const target = createRequire(context.parentURL).resolve(specifier);
            resolved = { url: pathToFileURL(target).href, shortCircuit: false, format: undefined };
          } catch {
            throw error;
          }
        } else {
          throw error;
        }
      }

      // Rule 3: prefer the `module` field, dependency-internal imports only.
      const isBareRoot =
        !specifier.startsWith("/") &&
        !specifier.startsWith(".") &&
        (specifier.indexOf("/") === -1 || (specifier.startsWith("@") && specifier.split("/").length === 2));
      const parentPath = context.parentURL?.startsWith("file:") ? fileURLToPath(context.parentURL) : null;
      if (parentPath && parentPath.includes("/node_modules/") && !specifier.startsWith("node:") && isBareRoot) {
        try {
          const hit = nearestManifest(dirname(fileURLToPath(resolved.url)));
          if (hit && !hit.manifest.exports && hit.manifest.module) {
            const esmEntry = join(hit.dir, hit.manifest.module);
            if (esmEntry !== fileURLToPath(resolved.url) && existsSync(esmEntry)) {
              const url = pathToFileURL(esmEntry).href;
              forceModuleFormat.add(url);
              return { url, format: "module", shortCircuit: true };
            }
          }
        } catch {
          // Not a package shape we understand; keep the normal resolution.
        }
      }
      return resolved;
    },

    load(url, context, nextLoad) {
      // Rule 4: JSON without import attributes. Only when the native loader
      // refuses the import (`with { type: "json" }` missing): a CJS
      // require() of a .json (character-entities-legacy's package main is
      // index.json) must keep its native JSON load, and Node parses whatever
      // source a hook returns for such URLs as JSON — so never volunteer.
      if (url.endsWith(".json")) {
        try {
          return nextLoad(url, context);
        } catch (error) {
          if (error?.code === "ERR_IMPORT_ATTRIBUTE_MISSING") {
            return {
              format: "module",
              shortCircuit: true,
              source: `export default ${readFileSync(new URL(url), "utf-8").trim()};`,
            };
          }
          throw error;
        }
      }
      if (forceModuleFormat.has(url)) {
        const loaded = nextLoad(url, context);
        loaded.format = "module";
        return loaded;
      }
      return nextLoad(url, context);
    },
  });
}
