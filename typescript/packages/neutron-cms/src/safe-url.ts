/**
 * Addresses that can't run code when followed: web, mail and phone links, and paths on this site.
 * The editor's browser bundle has its own copy, in editor/schema.js.
 */
export function isSafeUrl(href: string): boolean {
  // Browsers ignore spaces and control characters inside a scheme, so "java script:" counts as one.
  const plain = href.replace(/[\u0000- \u007f]/g, "");
  const scheme = plain.match(/^([a-z][a-z0-9+.-]*):/i)?.[1].toLowerCase();
  return !scheme || scheme === "http" || scheme === "https" || scheme === "mailto" || scheme === "tel";
}
