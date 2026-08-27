/**
 * Shared HTML/XML escaping helpers.
 *
 * Both escape the full set of metacharacters (including quotes) so a single
 * function is safe in text content and in single- or double-quoted attribute
 * contexts. HTML uses `&#39;` for the apostrophe (universally supported);
 * XML uses `&apos;` (valid in XML, used for RSS/sitemaps).
 */

export function escapeHtml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

export function escapeXml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

/**
 * Render a CSP `nonce` attribute, only for a syntactically safe nonce
 * (base64/base64url charset) so it can never inject extra attributes.
 * Anything else — empty, wrong charset — renders no attribute at all.
 */
export function nonceAttr(nonce?: string): string {
  if (!nonce || !/^[A-Za-z0-9+/_=-]+$/.test(nonce)) {
    return "";
  }
  return ` nonce="${nonce}"`;
}
