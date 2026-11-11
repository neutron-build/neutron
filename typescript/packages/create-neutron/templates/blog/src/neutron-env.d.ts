/// <reference types="vite/client" />

// Ambient types for Neutron's Vite virtual modules so `tsc` is clean before the
// first dev/build run (which generates the per-route and content type files).
declare module "virtual:neutron/routes" {
  // Typed as exactly what registerRoutes() accepts, so it can never drift.
  export const routes: Parameters<
    typeof import("@neutron-build/core/client").registerRoutes
  >[0];
}
