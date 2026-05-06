// Custom `marked` extensions for Codex inline syntaxes.
//
// Implements:
//   [ref: source locator]   — citation footnote, hyperlinked + tooltipped
//   [unit-id]               — cross-reference to another unit (status-aware)
//
// Both are inline-level extensions. They do not interact with each other
// or with KaTeX (they require literal square brackets in markdown text).
// The unit-id matcher is restricted to the canonical \d{2}\.\d{2}\.\d{2}
// pattern so it does not collide with ordinary markdown link references.

import type { TokenizerExtension, RendererExtension } from "marked";

type Tok = {
  type: string;
  raw: string;
  source: string;
  locator?: string;
} | {
  type: string;
  raw: string;
  unitId: string;
};

// ---------------------------------------------------------------------------
// [ref: source locator]
// ---------------------------------------------------------------------------

const REF_RE = /^\[ref:\s+([\w\-]+)(?:\s+([^\]]+))?\]/;

const refTokenizer: TokenizerExtension = {
  name: "codexRef",
  level: "inline",
  start(src: string) {
    const i = src.indexOf("[ref:");
    return i === -1 ? undefined : i;
  },
  tokenizer(src: string) {
    const m = REF_RE.exec(src);
    if (!m) return undefined;
    return {
      type: "codexRef",
      raw: m[0],
      source: m[1],
      locator: m[2]?.trim(),
    };
  },
};

const refRenderer: RendererExtension = {
  name: "codexRef",
  renderer(token: Tok) {
    const t = token as { source: string; locator?: string };
    const isPending = t.source === "TODO_REF" || t.source.toLowerCase() === "pending";
    const cls = isPending ? "ref ref--pending" : "ref";
    const sourceAttr = escapeHtml(t.source);
    const locatorAttr = escapeHtml(t.locator || "");
    const title = t.locator
      ? escapeHtml(`${t.source} — ${t.locator}`)
      : escapeHtml(t.source);
    const supBody = t.locator
      ? `${escapeHtml(t.source)} ${escapeHtml(t.locator)}`
      : escapeHtml(t.source);
    return `<span class="${cls}" data-ref-source="${sourceAttr}" data-ref-locator="${locatorAttr}" title="${title}"><sup>[${supBody}]</sup></span>`;
  },
};

// ---------------------------------------------------------------------------
// [<section>.<chapter>.<ordinal>]  — bare unit-id reference
// ---------------------------------------------------------------------------

const UNIT_ID_RE = /^\[(\d{2}\.\d{2}\.\d{2})\]/;

const unitRefTokenizer: TokenizerExtension = {
  name: "codexUnitRef",
  level: "inline",
  start(src: string) {
    const m = src.match(/\[\d{2}\.\d{2}\.\d{2}\]/);
    return m ? src.indexOf(m[0]) : undefined;
  },
  tokenizer(src: string) {
    const m = UNIT_ID_RE.exec(src);
    if (!m) return undefined;
    return {
      type: "codexUnitRef",
      raw: m[0],
      unitId: m[1],
    };
  },
};

// Build-time set of shipped unit IDs (filled in by content collection scan).
// We default to empty; the codex app populates it via a side-effect import.
const shippedUnitIds = new Set<string>();

export function setShippedUnitIds(ids: Iterable<string>) {
  shippedUnitIds.clear();
  for (const id of ids) shippedUnitIds.add(id);
}

const unitRefRenderer: RendererExtension = {
  name: "codexUnitRef",
  renderer(token: Tok) {
    const t = token as { unitId: string };
    if (shippedUnitIds.has(t.unitId)) {
      return `<a class="unit-ref" href="/u/${t.unitId}"><code>${t.unitId}</code></a>`;
    }
    return `<span class="unit-ref unit-ref--pending" title="unit pending"><code>${t.unitId}</code> <span class="unit-ref__badge">pending</span></span>`;
  },
};

// ---------------------------------------------------------------------------
// Image renderer override
// ---------------------------------------------------------------------------
// Default marked v15 passes alt text through unescaped, so quoted labels in
// the markdown source ("ker T", "coker T", etc.) produce HTML attributes with
// literal inner double quotes — which break Vite's parse5 HTML transform in
// dev, and any strict downstream HTML consumer. Override the renderer to
// escape href, alt, and title at attribute level.

interface MarkedImageToken {
  href: string;
  title: string | null;
  text: string;
}

// ---------------------------------------------------------------------------
// Public extension
// ---------------------------------------------------------------------------

export const codexMarkedExtensions = {
  extensions: [
    refTokenizer,
    refRenderer,
    unitRefTokenizer,
    unitRefRenderer,
  ],
  renderer: {
    image(token: MarkedImageToken) {
      const { href, title, text } = token;
      const safeHref = escapeHtml(href);
      const safeAlt = escapeHtml(text);
      const titleAttr = title ? ` title="${escapeHtml(title)}"` : "";
      return `<img src="${safeHref}" alt="${safeAlt}"${titleAttr} />`;
    },
  },
};

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}
