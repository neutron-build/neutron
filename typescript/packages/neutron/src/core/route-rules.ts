import type {
  NeutronHeaderRule,
  NeutronRedirectRule,
  NeutronRewriteRule,
  NeutronRoutesConfig,
} from "../config.js";

interface CompiledPathRule<TMeta> {
  source: string;
  regex: RegExp;
  params: string[];
  meta: TMeta;
}

interface CompiledRedirectMeta {
  destination: string;
  status: number;
}

interface CompiledRewriteMeta {
  destination: string;
}

interface CompiledHeaderMeta {
  headers: Record<string, string | null>;
}

export interface CompiledRouteRules {
  redirects: Array<CompiledPathRule<CompiledRedirectMeta>>;
  rewrites: Array<CompiledPathRule<CompiledRewriteMeta>>;
  headers: Array<CompiledPathRule<CompiledHeaderMeta>>;
}

export interface RouteRuleRedirectResult {
  location: string;
  status: number;
}

export interface RouteRuleRewriteResult {
  pathname: string;
  matchedSource: string;
}

export interface RouteRuleHeadersResult {
  source: string;
  /** `null` = send no such header on this path (suppresses a security default). */
  headers: Record<string, string | null>;
}

export function compileRouteRules(
  rules: NeutronRoutesConfig | undefined
): CompiledRouteRules {
  return {
    redirects: compileRedirectRules(rules?.redirects || []),
    rewrites: compileRewriteRules(rules?.rewrites || []),
    headers: compileHeaderRules(rules?.headers || []),
  };
}

export function resolveRouteRuleRedirect(
  compiled: CompiledRouteRules,
  pathname: string,
  search: string
): RouteRuleRedirectResult | null {
  for (const rule of compiled.redirects) {
    const params = matchPathRule(rule, pathname);
    if (!params) {
      continue;
    }

    const destination = substituteDestination(rule.meta.destination, params);
    return {
      location: sanitizeRedirectLocation(mergeRedirectQuery(destination, search)),
      status: rule.meta.status,
    };
  }

  return null;
}

/**
 * Carry the incoming query onto a redirect destination, as Next.js does: every
 * incoming parameter is appended, except keys the destination's own query
 * sets — the destination wins per key, and its query string is kept as
 * written. A destination without a query takes the incoming one verbatim.
 */
function mergeRedirectQuery(destination: string, search: string): string {
  const incoming = search.startsWith("?") ? search.slice(1) : search;
  if (!incoming) {
    return destination;
  }
  const hashIndex = destination.indexOf("#");
  const hash = hashIndex === -1 ? "" : destination.slice(hashIndex);
  const beforeHash = hashIndex === -1 ? destination : destination.slice(0, hashIndex);
  const queryIndex = beforeHash.indexOf("?");
  if (queryIndex === -1) {
    return `${beforeHash}?${incoming}${hash}`;
  }
  const destinationQuery = beforeHash.slice(queryIndex + 1);
  const destinationKeys = new Set(new URLSearchParams(destinationQuery).keys());
  const carried = new URLSearchParams();
  for (const [key, value] of new URLSearchParams(incoming)) {
    if (!destinationKeys.has(key)) {
      carried.append(key, value);
    }
  }
  const carriedQuery = carried.toString();
  if (!carriedQuery) {
    return destination;
  }
  const joined = destinationQuery ? `${destinationQuery}&${carriedQuery}` : carriedQuery;
  return `${beforeHash.slice(0, queryIndex)}?${joined}${hash}`;
}

/**
 * Prevent open redirects from request-controlled params substituted into a
 * destination. A relative destination must not become protocol-relative
 * (`//host`) or backslash-tricked (`/\host`) — collapse any leading run of
 * slashes/backslashes to a single same-origin slash. Intentional absolute
 * destinations (`https://...`) start with a scheme and are left untouched.
 */
function sanitizeRedirectLocation(location: string): string {
  // Browsers strip TAB/LF/CR anywhere in a URL, so "/\t/evil" resolves to
  // "//evil" client-side. Remove them before checking, then collapse a leading
  // run of slashes/backslashes so a relative destination can't become
  // protocol-relative (//host) or backslash-tricked (/\host).
  const cleaned = location.replace(/[\t\n\r]/g, "");
  if (/^[/\\]{2,}/.test(cleaned)) {
    return "/" + cleaned.replace(/^[/\\]+/, "");
  }
  return cleaned;
}

export function resolveRouteRuleRewrite(
  compiled: CompiledRouteRules,
  pathname: string
): RouteRuleRewriteResult | null {
  for (const rule of compiled.rewrites) {
    const params = matchPathRule(rule, pathname);
    if (!params) {
      continue;
    }

    const destination = substituteDestination(rule.meta.destination, params);
    return {
      pathname: destinationToPathname(destination),
      matchedSource: rule.source,
    };
  }

  return null;
}

export function resolveRouteRuleHeaders(
  compiled: CompiledRouteRules,
  pathname: string
): RouteRuleHeadersResult[] {
  const matches: RouteRuleHeadersResult[] = [];
  for (const rule of compiled.headers) {
    if (!matchPathRule(rule, pathname)) {
      continue;
    }
    matches.push({
      source: rule.source,
      headers: { ...rule.meta.headers },
    });
  }
  return matches;
}

/**
 * Apply matched header rules to a response: each string value is set unless
 * the response already carries the header (the response's own headers win);
 * `null` values set nothing. Returns the lower-cased names a rule nulled, for
 * the security-header pass to leave out.
 */
export function applyRouteRuleHeaders(
  response: Response,
  matches: Array<{ headers: Record<string, string | null> }>
): Set<string> {
  const suppressed = new Set<string>();
  for (const rule of matches) {
    for (const [name, value] of Object.entries(rule.headers)) {
      if (value === null) {
        suppressed.add(name.toLowerCase());
        continue;
      }
      try {
        if (!response.headers.has(name)) {
          response.headers.set(name, value);
        }
      } catch {
        // Some Response instances can expose immutable headers (e.g. redirects).
      }
    }
  }
  return suppressed;
}

function compileRedirectRules(
  rules: NeutronRedirectRule[]
): Array<CompiledPathRule<CompiledRedirectMeta>> {
  const compiled: Array<CompiledPathRule<CompiledRedirectMeta>> = [];
  for (const rule of rules) {
    const pathRule = compilePathRule(rule.source, {
      destination: rule.destination,
      status: rule.statusCode || (rule.permanent ? 308 : 307),
    });
    if (pathRule) {
      compiled.push(pathRule);
    }
  }
  return compiled;
}

function compileRewriteRules(
  rules: NeutronRewriteRule[]
): Array<CompiledPathRule<CompiledRewriteMeta>> {
  const compiled: Array<CompiledPathRule<CompiledRewriteMeta>> = [];
  for (const rule of rules) {
    const pathRule = compilePathRule(rule.source, {
      destination: rule.destination,
    });
    if (pathRule) {
      compiled.push(pathRule);
    }
  }
  return compiled;
}

function compileHeaderRules(
  rules: NeutronHeaderRule[]
): Array<CompiledPathRule<CompiledHeaderMeta>> {
  const compiled: Array<CompiledPathRule<CompiledHeaderMeta>> = [];
  for (const rule of rules) {
    const pathRule = compilePathRule(rule.source, {
      headers: normalizeHeaderValues(rule.headers),
    });
    if (pathRule) {
      compiled.push(pathRule);
    }
  }
  return compiled;
}

function compilePathRule<TMeta>(
  source: string,
  meta: TMeta
): CompiledPathRule<TMeta> | null {
  const normalized = normalizeSourcePattern(source);
  if (!normalized) {
    return null;
  }

  const segments = normalized.split("/").filter(Boolean);
  const catchAllCount = segments.filter(
    (s) => s === "*" || (s.startsWith(":") && s.endsWith("*"))
  ).length;
  if (catchAllCount > 1) {
    throw new Error(
      `Route pattern "${source}" contains ${catchAllCount} catch-all segments. Only one is allowed.`
    );
  }
  const params: string[] = [];
  let regex = "^";

  if (segments.length === 0) {
    regex += "/$";
  } else {
    for (const segment of segments) {
      if (segment === "*") {
        params.push("*");
        regex += "/(.*)";
        break;
      }

      if (segment.startsWith(":")) {
        const { name, catchAll } = parseNamedParam(segment);
        if (!name) {
          return null;
        }
        params.push(name);
        if (catchAll) {
          // Zero or more segments, as in Next: `/builder/:path*` matches
          // `/builder` too. (`*` keeps requiring one.)
          regex += "(?:/(.*))?";
          break;
        }
        regex += "/([^/]+)";
        continue;
      }

      regex += `/${escapeRegExp(segment)}`;
    }
    regex += "$";
  }

  return {
    source: normalized,
    regex: new RegExp(regex),
    params,
    meta,
  };
}

function matchPathRule<TMeta>(
  rule: CompiledPathRule<TMeta>,
  pathname: string
): Record<string, string> | null {
  const match = pathname.match(rule.regex);
  if (!match) {
    return null;
  }

  const params: Record<string, string> = {};
  for (let index = 0; index < rule.params.length; index++) {
    const key = rule.params[index];
    params[key] = match[index + 1] || "";
  }
  return params;
}

function substituteDestination(
  destination: string,
  params: Record<string, string>
): string {
  let output = destination;

  // An empty catch-all takes its leading slash with it, so `/docs/:path*`
  // matched by `/docs` maps `/v2/docs/:path*` to `/v2/docs`, not `/v2/docs/`.
  output = output.replace(/(\/?):([a-zA-Z0-9_]+)\*/g, (_token, slash: string, name: string) => {
    return params[name] ? slash + params[name] : "";
  });

  output = output.replace(/:([a-zA-Z0-9_]+)/g, (_token, name: string) => {
    return params[name] || "";
  });

  if (params["*"]) {
    output = output.replace(/\*/g, params["*"]);
  }

  if (output === "" || output.startsWith("?") || output.startsWith("#")) {
    output = "/" + output;
  }
  return output;
}

function normalizeSourcePattern(source: string): string | null {
  if (!source || typeof source !== "string") {
    return null;
  }

  const trimmed = source.trim();
  if (!trimmed.startsWith("/")) {
    return null;
  }

  if (trimmed.length > 1 && trimmed.endsWith("/")) {
    return trimmed.slice(0, -1);
  }

  return trimmed;
}

function parseNamedParam(segment: string): { name: string | null; catchAll: boolean } {
  let name = segment.slice(1);
  let catchAll = false;

  if (name.endsWith("*")) {
    catchAll = true;
    name = name.slice(0, -1);
  }

  if (!name || !/^[a-zA-Z0-9_]+$/.test(name)) {
    return { name: null, catchAll: false };
  }

  return { name, catchAll };
}

function destinationToPathname(destination: string): string {
  if (!destination) {
    return "/";
  }

  try {
    const parsed = new URL(destination, "http://neutron.local");
    const pathname = parsed.pathname || "/";
    if (pathname.length > 1 && pathname.endsWith("/")) {
      return pathname.slice(0, -1);
    }
    return pathname;
  } catch {
    return destination;
  }
}

function normalizeHeaderValues(
  headers: Record<string, string | null>
): Record<string, string | null> {
  const output: Record<string, string | null> = {};
  for (const [name, value] of Object.entries(headers || {})) {
    output[name] = value === null ? null : String(value);
  }
  return output;
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
