// A bundler may move this package's code, so its CSS and browser scripts travel inside it as strings.
// The visual editor is bundled here once, so sites install nothing for it.
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";

const read = (name) => readFileSync(new URL(`../src/${name}`, import.meta.url), "utf8");
const editor = await build({
  entryPoints: [fileURLToPath(new URL("../src/editor.client.js", import.meta.url))],
  bundle: true,
  minify: true,
  format: "iife",
  target: ["safari16", "chrome111", "firefox115"],
  legalComments: "none",
  write: false,
  // prosemirror-markdown builds a markdown-it parser it is never asked to use here: the editor reads
  // Markdown with marked, as the site does. Leaving it out saves most of the bundle.
  plugins: [{
    name: "no-markdown-it",
    setup(build) {
      build.onResolve({ filter: /^markdown-it$/ }, () => ({ path: "markdown-it", namespace: "stub" }));
      build.onLoad({ filter: /.*/, namespace: "stub" }, () => ({ contents: "export default function markdownIt() { return {}; }" }));
    },
  }],
});
const assets = { ADMIN_CSS: read("admin.css"), ADMIN_JS: read("admin.client.js"), EDIT_JS: read("edit.client.js"), EDITOR_JS: editor.outputFiles[0].text };
writeFileSync(
  new URL("../src/assets.generated.ts", import.meta.url),
  Object.entries(assets).map(([name, text]) => `export const ${name} = ${JSON.stringify(text)};\n`).join(""),
);
