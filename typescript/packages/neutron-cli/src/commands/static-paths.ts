/**
 * Path helpers for the static writer. Split out of build.ts (which imports
 * vite at load time) so the traversal guard can be tested from source —
 * the same reason client-entry.ts exists.
 */

/**
 * Resolve a route pattern with params to an actual path.
 * Handles both named params and catch-all (splat) params:
 *   "/blog/:slug"  + { slug: "hello" }                → "/blog/hello"
 *   "/docs/*"      + { "*": "getting-started/intro" }  → "/docs/getting-started/intro"
 */
export function resolvePath(pattern: string, params: Record<string, string>): string {
  let resolved = pattern;

  for (const [key, value] of Object.entries(params)) {
    // Named param — :slug or [slug]
    const bracketReplaced = resolved.replace(`[${key}]`, value);
    const colonReplaced = resolved.replace(`:${key}`, value);

    if (bracketReplaced !== resolved) {
      resolved = bracketReplaced;
    } else if (colonReplaced !== resolved) {
      resolved = colonReplaced;
    } else {
      // Catch-all — replace *paramName (e.g., *slug) or bare *
      const splatPattern = key === "*" ? "*" : `*${key}`;
      resolved = resolved.replace(splatPattern, value);
    }
  }

  return resolved;
}

/**
 * getStaticPaths params are untrusted (CMS slugs, external data): a value
 * like "../../evil" substituted into the output-path join writes outside
 * dist/ — arbitrary file write on the build machine. The runtime handler has
 * always guarded this; the static writer must too. Segment-precise so legal
 * dot-containing slugs ("/a..b", "/v1.2..3") are not rejected.
 */
export function isUnsafeResolvedPath(resolvedPath: string): boolean {
  if (!resolvedPath.startsWith("/")) return true;
  return resolvedPath.split(/[\\/]/).includes("..");
}
