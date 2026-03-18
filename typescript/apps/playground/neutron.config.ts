import { defineConfig } from "neutron";

const runtime = process.env.NEUTRON_RUNTIME === "react-compat" ? "react-compat" : "preact";

export default defineConfig({
  runtime,
  worker: {
    entry: "src/worker.ts",
  },
});
