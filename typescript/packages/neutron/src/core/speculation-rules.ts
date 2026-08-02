/**
 * Browser-native instant navigation for pages that ship no router.
 *
 * A page in the static tier has no client-side router to intercept clicks, and
 * that is the point — but it should still navigate instantly. The Speculation
 * Rules API does that better than any framework could: the browser *prerenders*
 * the next document, so the target is already parsed, styled and painted when
 * the click lands. A JS prefetcher can only have the data ready; the render
 * still has to happen after the click. Prerender wins, and it costs zero
 * framework bytes, which is what makes it usable in a tier whose whole purpose
 * is shipping no JS.
 *
 * Document rules are used rather than a URL list so the rules do not have to be
 * regenerated per page or grow with the site.
 *
 * ## Why `moderate` and not `eager`
 *
 * Prerendering *executes* the target page: it is a real request and a real
 * render, server-side and client-side. `eager` would speculate every matching
 * link in the document, so one dense navigation menu turns a single visitor
 * into a dozen renders. `moderate` triggers on hover/pointerdown, which is the
 * same intent signal the JS prefetcher uses, and keeps the amplification factor
 * near one.
 *
 * ## Safety
 *
 * A prerendered page runs its own scripts and its own requests, so it must
 * never be a URL with side effects. Two guards:
 *
 * - Only same-origin paths are eligible (`href_matches: "/*"`).
 * - `[data-neutron-prefetch=false]` is excluded, the same per-link opt-out the
 *   JS prefetcher honours, so one attribute governs both mechanisms.
 *
 * Unsupported browsers ignore the script entirely and navigate normally, so
 * this is a pure progressive enhancement with no fallback path to maintain.
 */

export interface SpeculationRulesOptions {
  /** Extra path prefixes to exclude, e.g. "/logout", "/admin/*". */
  exclude?: string[];
  /** Defaults to "moderate". "eager" is a load-amplification risk. */
  eagerness?: "conservative" | "moderate" | "eager";
}

/**
 * The `<script type="speculationrules">` tag for a static-tier document, or an
 * empty string when there is nothing safe to speculate.
 *
 * `nonce` must be threaded through when a nonce-based CSP is active: the tag is
 * inline, so without it the policy drops the rules silently and navigation
 * quietly stops being instant.
 */
export function renderSpeculationRules(
  options: SpeculationRulesOptions = {},
  nonce?: string
): string {
  const { exclude = [], eagerness = "moderate" } = options;

  const conditions: unknown[] = [
    { href_matches: "/*" },
    { not: { selector_matches: "[data-neutron-prefetch=false]" } },
    // Links that leave the document or download are not navigations to warm.
    { not: { selector_matches: "[download]" } },
    { not: { selector_matches: '[target="_blank"]' } },
  ];

  for (const pattern of exclude) {
    conditions.push({ not: { href_matches: pattern } });
  }

  const rules = {
    prerender: [{ where: { and: conditions }, eagerness }],
  };

  // The payload is generated from a closed structure — no user strings reach it
  // except `exclude`, which is escaped below — so it cannot break out of the
  // script element.
  const json = JSON.stringify(rules).replace(/</g, "\\u003c");
  const nonceAttr = nonce ? ` nonce="${nonce}"` : "";
  return `<script type="speculationrules"${nonceAttr}>${json}</script>`;
}
