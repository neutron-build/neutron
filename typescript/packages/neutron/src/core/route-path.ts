/**
 * Shared route-path token model.
 *
 * One place decides how a route pattern like `/users/:id.json` or
 * `/docs/*slug.md` becomes tokens, and one binder decides how a concrete URL
 * binds back to parameter names. The trie router, not-found scope selection,
 * and route declaration generation all consume these so they cannot grow
 * different interpretations of suffixes and parameter names (the shape-sharing
 * trie used to bind whichever parameter name happened to register first —
 * NA-06 — and the not-found selector used to compare literal prefixes —
 * NA-07).
 */

export type PathSegment =
  | { type: "static"; value: string }
  | { type: "param"; value: string; suffix: string }
  | { type: "wildcard"; value: string; suffix: string };

export function parsePath(path: string): PathSegment[] {
  const parts = path.split("/").filter(Boolean);
  const segments: PathSegment[] = [];

  for (const part of parts) {
    if (part.startsWith("*")) {
      const { name, suffix } = splitDynamicSegment(part.slice(1), "*");
      segments.push({ type: "wildcard", value: name, suffix });
    } else if (part.startsWith(":")) {
      const { name, suffix } = splitDynamicSegment(part.slice(1), "");
      segments.push({ type: "param", value: name, suffix });
    } else {
      segments.push({ type: "static", value: part });
    }
  }

  return segments;
}

function splitDynamicSegment(value: string, fallback: string): { name: string; suffix: string } {
  const dot = value.indexOf(".");
  if (dot === -1) return { name: value || fallback, suffix: "" };
  return { name: value.slice(0, dot) || fallback, suffix: value.slice(dot) };
}

/**
 * Bind concrete URL segments against a route pattern's tokens, returning the
 * parameter map — or null when the segments do not match the pattern.
 *
 * With `prefix` the pattern may cover only a prefix of the segments (a
 * not-found scope covering its whole subtree); without it the pattern must
 * consume the segments exactly.
 *
 * The map has a null prototype: a parameter named `__proto__` or `constructor`
 * must not change the map's own behavior. Names come from the pattern, never
 * from whichever sibling route registered into a shared trie edge first.
 */
export function bindPathParams(
  pattern: string,
  segments: readonly string[],
  prefix = false
): Record<string, string> | null {
  const params: Record<string, string> = Object.create(null);
  const tokens = parsePath(pattern);
  let index = 0;

  for (let t = 0; t < tokens.length; t++) {
    const token = tokens[t];
    if (token.type === "static") {
      if (segments[index++] !== token.value) return null;
      continue;
    }
    // Malformed routes are rejected at insertion; this is the backstop for
    // hand-built route tables that never went through discovery.
    if (Object.prototype.hasOwnProperty.call(params, token.value)) {
      throw new Error(`Duplicate route parameter: ${token.value}`);
    }
    if (token.type === "wildcard" && t !== tokens.length - 1) {
      throw new Error("A wildcard must be the final route segment");
    }
    const text =
      token.type === "wildcard" ? segments.slice(index).join("/") : segments[index];
    if (text === undefined || !text.endsWith(token.suffix)) return null;
    const value = token.suffix ? text.slice(0, -token.suffix.length) : text;
    if (!value) return null;
    params[token.value] = value;
    index = token.type === "wildcard" ? segments.length : index + 1;
  }

  return prefix || index === segments.length ? params : null;
}

/** Split a URL path into its non-empty segments. */
export function parseUrlPath(path: string): string[] {
  return path.split("/").filter(Boolean);
}
