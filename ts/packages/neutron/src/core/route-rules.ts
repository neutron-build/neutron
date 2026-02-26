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
  headers: Record<string, string>;
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
  headers: Record<string, string>;
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
    const hasQuery = destination.includes("?");
    const location = !hasQuery && search ? `${destination}${search}` : destination;
    return {
      location,
      status: rule.meta.status,
    };
  }

  return null;
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
          regex += "/(.*)";
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

  output = output.replace(/:([a-zA-Z0-9_]+)\*/g, (_token, name: string) => {
    return params[name] || "";
  });

  output = output.replace(/:([a-zA-Z0-9_]+)/g, (_token, name: string) => {
    return params[name] || "";
  });

  if (params["*"]) {
    output = output.replace(/\*/g, params["*"]);
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

function normalizeHeaderValues(headers: Record<string, string>): Record<string, string> {
  const output: Record<string, string> = {};
  for (const [name, value] of Object.entries(headers || {})) {
    output[name] = String(value);
  }
  return output;
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
