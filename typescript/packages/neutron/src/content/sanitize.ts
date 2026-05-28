/**
 * HTML sanitization for untrusted content.
 *
 * Neutron treats local content-collection files as trusted authored content and
 * renders them faithfully — the same model as Astro, Next, SvelteKit and Nuxt.
 * When a collection's HTML originates from an untrusted source (a CMS, user
 * submissions, remote fetch), opt in with `sanitize: true` on the collection,
 * or call `sanitizeHtml()` directly.
 *
 * It delegates to the `sanitize-html` package — a real, parser-based allow-list
 * sanitizer — which is an optional peer dependency. A regular expression can
 * never safely sanitize HTML, so there is deliberately no built-in fallback.
 */

export interface SanitizeOptions {
  /** Allowed tag names. Defaults to sanitize-html's safe allow-list. */
  allowedTags?: string[];
  /** Allowed attributes per tag. Defaults to sanitize-html's safe allow-list. */
  allowedAttributes?: Record<string, string[]>;
  /** URL schemes permitted in href/src. Defaults to http, https, mailto, tel. */
  allowedSchemes?: string[];
}

type SanitizeFn = (html: string, options?: unknown) => string;

let cachedSanitizer: SanitizeFn | null = null;

async function loadSanitizer(): Promise<SanitizeFn> {
  if (cachedSanitizer) {
    return cachedSanitizer;
  }
  // Variable specifier keeps the optional dependency out of static module
  // resolution and bundling — it is only required when sanitization is used.
  const moduleName = "sanitize-html";
  let mod: unknown;
  try {
    mod = await import(/* @vite-ignore */ moduleName);
  } catch {
    throw new Error(
      "[neutron] HTML sanitization requires the optional 'sanitize-html' " +
        "dependency. Install it (e.g. `npm install sanitize-html`) to render " +
        "untrusted content with `sanitize: true`.",
    );
  }
  const fn = ((mod as { default?: SanitizeFn }).default ?? mod) as SanitizeFn;
  if (typeof fn !== "function") {
    throw new Error("[neutron] 'sanitize-html' did not export a callable sanitizer.");
  }
  cachedSanitizer = fn;
  return fn;
}

/**
 * Sanitize an HTML string using a parser-based allow-list. With no options the
 * sanitize-html defaults apply (script/style and event-handler attributes are
 * dropped, only safe tags/attributes survive).
 */
export async function sanitizeHtml(
  html: string,
  options?: SanitizeOptions,
): Promise<string> {
  const sanitize = await loadSanitizer();
  return options ? sanitize(html, options) : sanitize(html);
}
