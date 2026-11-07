// A bundler may move this package's code, so its CSS and browser script travel inside it as strings.
import { readFileSync, writeFileSync } from "node:fs";

const read = (name) => JSON.stringify(readFileSync(new URL(`../src/${name}`, import.meta.url), "utf8"));
writeFileSync(
  new URL("../src/assets.generated.ts", import.meta.url),
  `export const ADMIN_CSS = ${read("admin.css")};\nexport const ADMIN_JS = ${read("admin.client.js")};\n`,
);
