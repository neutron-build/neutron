import { defineConfig } from "@neutron-build/core";

const runtime = process.env.NEUTRON_RUNTIME === "react-compat" ? "react-compat" : "preact";

export default defineConfig({
  runtime,
  worker: {
    entry: "src/worker.ts",
  },
});
