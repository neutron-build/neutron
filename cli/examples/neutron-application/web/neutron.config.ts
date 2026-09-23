import { defineConfig } from "@neutron-build/core";

export default defineConfig({
  runtime: "preact",
  // FRAMEWORK_CONTRACT §4: serve /openapi.json.
  server: { openapi: { title: "Example web", version: "1.0.0" } },
});
