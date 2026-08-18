import * as path from "node:path";
import { defineConfig } from "vite";
import preact from "@preact/preset-vite";
import { neutronPlugin } from "@neutron-build/core/vite";

// The point of this fixture: install-style aliasing. The app imports
// `@neutron-build/core/...` specifiers, but a Vite alias points the package at
// a local checkout, the way a `file:` install, a pnpm `link:`, or a
// monorepo-hostile environment does. `neutron dev` must survive the dep
// optimizer seeing the aliased copy.
//
// The include entry forces the optimizer to bundle the aliased runtime. On
// vite 6 a spec-preserving alias alone is kept out of the optimizer by the
// plugin's own exclude, but the moment the optimizer meets the runtime by any
// other route (include entries, cross-package imports, other vite versions'
// alias handling), esbuild — which runs without Vite plugins — cannot resolve
// `virtual:neutron-islands` and the dev server dies. This config makes that
// meeting deterministic instead of version-dependent.
const localCore = path.resolve(import.meta.dirname, "../../packages/neutron/dist");

export default defineConfig({
  plugins: [preact(), neutronPlugin()],
  optimizeDeps: {
    include: ["@neutron-build/core/client"],
  },
  resolve: {
    alias: {
      "@neutron-build/core": localCore,
    },
  },
});
