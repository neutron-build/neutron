// Codex markdown pipeline configuration. Imported by both
// `neutron.config.ts` (build-time CLI) and the route `_layout` (SSR worker)
// so the marked extensions are wired in *both* processes that need them.

import markedKatex from "marked-katex-extension";
import { createDirectives, presetDirectiveConfigs } from "marked-directive";
import remarkMath from "remark-math";
import rehypeKatex from "rehype-katex";
import { codexMarkedExtensions, setShippedUnitIds } from "./marked-codex.js";

// We compute the shipped-unit-id set lazily so we don't run filesystem reads
// in code paths that don't need it. The CLI sets this explicitly during
// build via the export in neutron.config.ts; the SSR side reads from the
// already-populated set.
export { setShippedUnitIds };

export const codexMarkdownConfig = {
  markedExtensions: [
    // `nonStandard: true` relaxes the strict-delimiter rule so `$X$-foo`
    // patterns parse correctly. Strict mode would otherwise reject these
    // because the closing `$` is immediately followed by an alphanumeric.
    markedKatex({ throwOnError: false, output: "html", strict: "ignore", nonStandard: true }),
    createDirectives([
      ...presetDirectiveConfigs,
      {
        level: "container" as const,
        marker: ":::",
        renderer(token: any) {
          const meta = token.meta || {};
          const name = meta.name || "callout";
          const tokens = token.tokens || [];
          const inner = (this as any).parser.parse(tokens);
          if (name === "exercise") {
            return `<aside class="exercise">${inner}</aside>`;
          }
          return `<div class="callout callout--${name}">${inner}</div>`;
        },
      },
    ]),
    codexMarkedExtensions,
  ],
  remarkPlugins: [remarkMath],
  rehypePlugins: [[rehypeKatex, { strict: "ignore", trust: true }]] as any[],
  syntaxHighlight: { theme: "github-dark" as const },
};
