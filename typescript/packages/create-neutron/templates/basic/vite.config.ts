import { defineConfig } from "vite";
import preact from "@preact/preset-vite";
import { neutronPlugin } from "neutron/vite";

export default defineConfig({
  plugins: [preact(), neutronPlugin()],
});
