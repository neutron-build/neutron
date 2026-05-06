import { defineConfig, adapterStatic, setActiveMarkdownConfig } from "@neutron-build/core";
import { codexMarkdownConfig, setShippedUnitIds } from "./src/lib/markdown-config.js";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";

// Pre-build: scan content/units for `status: shipped` units so the
// cross-reference renderer can render real links instead of pending badges.
function collectShippedIds(root: string): string[] {
  const ids: string[] = [];
  const walk = (dir: string) => {
    let entries: string[];
    try { entries = readdirSync(dir); } catch { return; }
    for (const name of entries) {
      const path = join(dir, name);
      let s;
      try { s = statSync(path); } catch { continue; }
      if (s.isDirectory()) { walk(path); continue; }
      if (!name.endsWith(".md")) continue;
      let body: string;
      try { body = readFileSync(path, "utf-8"); } catch { continue; }
      const fmEnd = body.indexOf("\n---", 4);
      const fm = fmEnd > 0 ? body.slice(4, fmEnd) : "";
      const idMatch = /^id:\s*([\w.]+)\s*$/m.exec(fm);
      const statusMatch = /^status:\s*(\w+)\s*$/m.exec(fm);
      if (idMatch && statusMatch && statusMatch[1] === "shipped") {
        ids.push(idMatch[1]);
      }
    }
  };
  walk(root);
  return ids;
}

setShippedUnitIds(collectShippedIds("./src/content/units"));
setActiveMarkdownConfig(codexMarkdownConfig);

export default defineConfig({
  runtime: "preact",
  adapter: adapterStatic({ precompress: true }),
  markdown: codexMarkdownConfig,
});
