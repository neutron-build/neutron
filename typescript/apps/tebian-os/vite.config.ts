import { defineConfig } from "vite";
import preact from "@preact/preset-vite";

export default defineConfig({
  plugins: [
    preact({
      // Disabled: prefresh injects its HMR preamble twice when combined with
      // Neutron's vite plugin, causing duplicate declaration errors in the browser.
      // TODO: investigate @prefresh/vite double-transform root cause
      prefreshEnabled: false,
    }),
  ],
});
