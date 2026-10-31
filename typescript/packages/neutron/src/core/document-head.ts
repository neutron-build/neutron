/**
 * The app's `index.html` `<head>` in server-rendered documents (F-P6-1).
 *
 * Vite's model: what an app writes in index.html's head (viewport,
 * theme-color, icons, verification tags, the site title) is on every page.
 * Server-rendered documents used to drop it, so an app migrating from a Vite
 * SPA, or following the fragment guard's advice to move tags there, lost
 * them silently. It is now the lowest layer of the head under the route
 * chain's `head()`: a page or layout tag with the same key replaces it
 * (seo.ts `headTagKey`), and the framework's charset always wins.
 *
 * Carried over, as written: `<title>`, `<meta>` (not charset) and `<link>` (not
 * stylesheets or modulepreload — Neutron links each route's CSS and its
 * client entry itself). Not carried over: `<script>` (the client entry is
 * Neutron's, and an inline script would bypass a nonce CSP — use
 * `headScripts`), `<style>` (import the CSS), `<base>`.
 *
 * `neutron-ts build` records the head of the built `dist/index.html` (Vite
 * has replaced `%ENV%` placeholders and asset URLs there) in
 * `dist/.neutron-document-head.json` before any page is prerendered; `start`
 * and the adapter runtimes read that, dev reads the source index.html.
 *
 * Kept free of `node:` imports: the edge runtime bundles embed the tags.
 */
import { headTagKey, scanHeadTags, type DocumentHeadTag } from "./seo.js";

export const DOCUMENT_HEAD_FILE = ".neutron-document-head.json";
export const DOCUMENT_HEAD_FORMAT = 1;

export interface DocumentHeadManifest {
  format: typeof DOCUMENT_HEAD_FORMAT;
  tags: DocumentHeadTag[];
}

/**
 * The head tags of an index.html that server-rendered documents carry, each
 * as written (the app's own markup, emitted verbatim as Vite does) with the
 * key a route-chain tag replaces it by.
 */
export function extractDocumentHead(html: string): DocumentHeadTag[] {
  const head = /<head\b[^>]*>([\s\S]*?)<\/head\s*>/i.exec(html)?.[1] ?? "";
  return scanHeadTags(head)
    .filter((tag) => {
      if (tag.tag === "title") return tag.content !== "";
      if (tag.tag === "meta") return tag.attrs.charset === undefined && Object.keys(tag.attrs).length > 0;
      const rel = (tag.attrs.rel ?? "").toLowerCase().split(/\s+/);
      return !rel.includes("stylesheet") && !rel.includes("modulepreload");
    })
    .map((tag) => ({ key: headTagKey(tag), html: tag.html }));
}

export function parseDocumentHead(source: string): DocumentHeadTag[] {
  try {
    const parsed = JSON.parse(source) as Partial<DocumentHeadManifest>;
    return parsed?.format === DOCUMENT_HEAD_FORMAT && Array.isArray(parsed.tags) ? parsed.tags : [];
  } catch {
    return [];
  }
}

export function serializeDocumentHead(tags: DocumentHeadTag[]): string {
  return JSON.stringify({ format: DOCUMENT_HEAD_FORMAT, tags } satisfies DocumentHeadManifest, null, 2);
}
