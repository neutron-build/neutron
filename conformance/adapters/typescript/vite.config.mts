// Vite config for the TypeScript conformance app's SSR runtime.
//
// The adapter directory sits OUTSIDE the pnpm workspace, so the bare imports
// a real Neutron app uses (`@neutron-build/core` in route files) would not
// resolve from here. Aliasing to the built dist keeps the route files
// idiomatic — they exercise the SDK exactly the way a user's routes do —
// while the dist itself resolves its own dependencies (hono, zod, preact)
// from inside the workspace.
//
// Plain object, deliberately no `defineConfig` import: vite is not installed
// at this directory level, and loadConfigFromFile accepts a bare config.
import { fileURLToPath } from "node:url";

export default {
  resolve: {
    alias: {
      "@neutron-build/core": fileURLToPath(
        new URL("../../../typescript/packages/neutron/dist/index.js", import.meta.url),
      ),
    },
  },
};
