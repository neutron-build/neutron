import { defineConfig } from "vite";
import preact from "@preact/preset-vite";
import { neutronPlugin } from "@neutron-build/core/vite";

export default defineConfig({
  plugins: [preact(), neutronPlugin()],
});
