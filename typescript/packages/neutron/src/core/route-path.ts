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

import type { Route } from "./types.js";

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

/** Static segments beat parameters, which beat catch-alls; longer suffixes win. */
export function comparePathSpecificity(a: string, b: string): number {
  const left = parsePath(a), right = parsePath(b);
  const rank = { static: 3, param: 2, wildcard: 1 };
  for (let index = 0; index < Math.max(left.length, right.length); index++) {
    const x = left[index], y = right[index];
    if (!x || !y) return right.length - left.length;
    const difference = rank[y.type] - rank[x.type];
    if (difference) return difference;
    if (x.type !== 'static' && y.type !== 'static' && x.suffix.length !== y.suffix.length) return y.suffix.length - x.suffix.length;
  }
  return 0;
}

export function normalizePathname(pathname: string): string | null {
  let decoded: string;
  try {
    decoded = decodeURIComponent(pathname || "/");
  } catch {
    return null;
  }

  // Traversal is a whole segment equal to "..", not a substring: `/a..b`
  // and `/v1.2..3` are legal paths, `/a/../b` is not.
  if (!decoded.startsWith("/") || decoded.includes("\\") || decoded.includes("\0") || decoded.split("/").includes("..")) {
    return null;
  }

  if (decoded.length > 1 && decoded.endsWith("/")) {
    return decoded.slice(0, -1);
  }

  return decoded;
}

export interface NotFoundMatch {
  route: Route;
  /** The scope's parameters for this URL (NA-07): `/org/acme/missing` under a
   * `/org/:orgId` scope binds `orgId = "acme"`. */
  params: Record<string, string>;
}

/**
 * The not-found page covering `urlPath`, selected structurally.
 *
 * A scope is matched by binding its tokens against the URL (a dynamic scope
 * like `/org/:orgId` used to be compared as a literal prefix, so it could
 * never match its own subtree), and the winner is chosen by route structure —
 * deeper first, then static over parameter over wildcard at each position —
 * never by the character length of the pattern, which let a longer parameter
 * name look "more specific" than a shorter one (NA-07).
 */
export function findNotFoundMatch(routes: Route[], urlPath: string): NotFoundMatch | null {
  const segments = urlPath.split("/").filter(Boolean);
  const candidates: NotFoundMatch[] = [];
  for (const route of routes) {
    if (!route.isNotFound) continue;
    const params = bindPathParams(route.path, segments, true);
    if (params !== null) candidates.push({ route, params });
  }
  if (candidates.length === 0) return null;
  candidates.sort((a, b) => compareScopeSpecificity(a.route, b.route));
  return candidates[0];
}

/** The literal suffix of a dynamic token; static tokens have none. */
function suffixOf(token: PathSegment): string {
  return token.type === "static" ? "" : token.suffix;
}

/**
 * Structural specificity between two not-found scopes: deeper scopes first;
 * at equal depth, compare tokens left to right with static before parameter
 * before wildcard, and a longer literal suffix before a shorter one. Equal
 * structures tie-break stably by route id so selection never depends on
 * discovery order or parameter-name spelling.
 */
function compareScopeSpecificity(a: Route, b: Route): number {
  const aa = parsePath(a.path);
  const bb = parsePath(b.path);
  if (aa.length !== bb.length) return bb.length - aa.length;
  const rank = { static: 3, param: 2, wildcard: 1 } as const;
  for (let i = 0; i < aa.length; i++) {
    const delta = rank[bb[i].type] - rank[aa[i].type];
    if (delta) return delta;
    const aSuffix = suffixOf(aa[i]);
    const bSuffix = suffixOf(bb[i]);
    if (aSuffix.length !== bSuffix.length) return bSuffix.length - aSuffix.length;
  }
  return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
}

/**
 * The not-found page covering `urlPath`: the deepest one whose directory is a
 * prefix of the request.
 *
 * Deepest wins so a section can present its own 404 — a miss under `/admin`
 * should look like the admin app, not like the marketing site — while the root
 * page still catches everything else. Retained as the route-only view of
 * `findNotFoundMatch` for existing callers.
 */
export function findNotFoundRoute(routes: Route[], urlPath: string): Route | undefined {
  return findNotFoundMatch(routes, urlPath)?.route;
}
