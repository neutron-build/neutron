import { defineConfig, adapterStatic } from "@neutron-build/core";

export default defineConfig({
  runtime: "preact",
  adapter: adapterStatic({ precompress: true }),
  markdown: {
    syntaxHighlight: {
      theme: "css-variables"
    }
  }
});
