export interface CorsOptions {
  origin?: string | string[];
  methods?: string[];
  allowedHeaders?: string[];
  exposedHeaders?: string[];
  credentials?: boolean;
  maxAge?: number;
}

export interface ResolvedCorsOptions {
  origin: string | string[];
  methods: string[];
  allowedHeaders: string[];
  exposedHeaders: string[];
  credentials: boolean;
  maxAge: number;
}

export interface SecurityHeadersConfig {
  defaults: Record<string, string>;
}

const DEFAULT_CORS_METHODS = ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"];
const DEFAULT_ALLOWED_HEADERS = ["Content-Type", "Authorization", "Accept"];

const DEFAULT_SECURITY_HEADERS: Record<string, string> = {
  "X-Content-Type-Options": "nosniff",
  "X-Frame-Options": "DENY",
  "Referrer-Policy": "strict-origin-when-cross-origin",
  "Cross-Origin-Opener-Policy": "same-origin",
  "Cross-Origin-Resource-Policy": "same-origin",
};

export function resolveCorsOptions(options: false | CorsOptions | undefined): ResolvedCorsOptions | null {
  if (options === false) {
    return null;
  }

  const origin = options?.origin ?? "*";
  const credentials = Boolean(options?.credentials);

  if (credentials && origin === "*") {
    throw new Error(
      'CORS misconfiguration: credentials cannot be used with origin "*". ' +
      "Specify explicit allowed origins instead."
    );
  }

  return {
    origin,
    methods: normalizeList(options?.methods, DEFAULT_CORS_METHODS),
    allowedHeaders: normalizeList(options?.allowedHeaders, DEFAULT_ALLOWED_HEADERS),
    exposedHeaders: normalizeList(options?.exposedHeaders, []),
    credentials,
    maxAge: Number.isFinite(options?.maxAge) && (options?.maxAge || 0) > 0 ? options!.maxAge! : 600,
  };
}

export function resolveSecurityHeadersConfig(
  options: false | { headers?: Record<string, string> } | undefined
): SecurityHeadersConfig | null {
  if (options === false) {
    return null;
  }

  const merged: Record<string, string> = { ...DEFAULT_SECURITY_HEADERS };
  for (const [name, value] of Object.entries(options?.headers || {})) {
    merged[name] = String(value);
  }

  return {
    defaults: merged,
  };
}

export function createCorsPreflightResponse(
  request: Request,
  options: ResolvedCorsOptions
): Response | null {
  if (request.method.toUpperCase() !== "OPTIONS") {
    return null;
  }

  const hasPreflightMethod = request.headers.has("Access-Control-Request-Method");
  if (!hasPreflightMethod) {
    return null;
  }

  // The response varies on Origin whether the origin is allowed or not
  // (TS-30): a disallowed-origin 403 without `Vary: Origin` lets a shared
  // cache serve one origin's rejection (or reuse a cached response with the
  // wrong header state) for another.
  const resolvedOrigin = resolveResponseOrigin(request, options);
  if (!resolvedOrigin) {
    return new Response(null, { status: 403, headers: makeVaryOriginHeaders() });
  }

  const headers = new Headers();
  headers.set("Access-Control-Allow-Origin", resolvedOrigin);
  headers.set("Access-Control-Allow-Methods", options.methods.join(", "));
  headers.set("Access-Control-Allow-Headers", options.allowedHeaders.join(", "));
  headers.set("Access-Control-Max-Age", String(options.maxAge));
  appendVary(headers, "Origin");

  if (options.credentials) {
    headers.set("Access-Control-Allow-Credentials", "true");
  }

  return new Response(null, { status: 204, headers });
}

/** Minimal headers declaring Origin variation for rejected/no-origin paths. */
function makeVaryOriginHeaders(): Headers {
  const headers = new Headers();
  appendVary(headers, "Origin");
  return headers;
}

export function applyCorsHeaders(
  request: Request,
  response: Response,
  options: ResolvedCorsOptions
): void {
  // Origin-dependent responses must declare that variation on every path —
  // including absent and disallowed origins (TS-30). Without it a shared
  // intermediary can store the no-CORS-header variant and reuse it for an
  // origin that would have received different header state.
  appendVary(response.headers, "Origin");
  const resolvedOrigin = resolveResponseOrigin(request, options);
  if (!resolvedOrigin) {
    return;
  }

  response.headers.set("Access-Control-Allow-Origin", resolvedOrigin);

  if (options.credentials) {
    response.headers.set("Access-Control-Allow-Credentials", "true");
  }

  if (options.exposedHeaders.length > 0) {
    response.headers.set("Access-Control-Expose-Headers", options.exposedHeaders.join(", "));
  }
}

export function applySecurityHeaders(response: Response, config: SecurityHeadersConfig): void {
  for (const [name, value] of Object.entries(config.defaults)) {
    if (!response.headers.has(name)) {
      response.headers.set(name, value);
    }
  }
}

function normalizeList(value: string[] | undefined, fallback: string[]): string[] {
  if (!value || value.length === 0) {
    return [...fallback];
  }

  const normalized = value
    .map((item) => String(item).trim())
    .filter(Boolean);
  return normalized.length > 0 ? normalized : [...fallback];
}

function resolveResponseOrigin(request: Request, options: ResolvedCorsOptions): string | null {
  const requestOrigin = request.headers.get("Origin");
  if (!requestOrigin) {
    return null;
  }

  if (typeof options.origin === "string") {
    if (options.origin === "*") {
      if (options.credentials) {
        throw new Error(
          'CORS security violation: credentials cannot be used with origin "*". ' +
          "This should have been caught during configuration."
        );
      }
      return "*";
    }
    return options.origin === requestOrigin ? requestOrigin : null;
  }

  return options.origin.includes(requestOrigin) ? requestOrigin : null;
}

function appendVary(headers: Headers, token: string): void {
  const existing = headers.get("Vary");
  if (!existing) {
    headers.set("Vary", token);
    return;
  }

  const parts = existing
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean);
  if (!parts.includes(token)) {
    parts.push(token);
  }
  headers.set("Vary", parts.join(", "));
}
