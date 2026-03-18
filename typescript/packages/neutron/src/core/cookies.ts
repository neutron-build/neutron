export interface CookieSerializeOptions {
  path?: string;
  domain?: string;
  httpOnly?: boolean;
  secure?: boolean;
  sameSite?: "Strict" | "Lax" | "None";
  expires?: Date;
  maxAge?: number;
}

export function parseCookieHeader(header: string | null | undefined): Record<string, string> {
  if (!header) {
    return {};
  }

  // SECURITY: Limit cookie header size to prevent DoS (standard browser limit is ~4KB per cookie, ~8KB total)
  const MAX_COOKIE_HEADER_SIZE = 16384; // 16KB (generous limit)
  if (header.length > MAX_COOKIE_HEADER_SIZE) {
    console.warn(`[Neutron] Cookie header exceeds ${MAX_COOKIE_HEADER_SIZE} bytes, truncating`);
    header = header.slice(0, MAX_COOKIE_HEADER_SIZE);
  }

  const output: Record<string, string> = {};
  const pairs = header.split(";");
  for (const pair of pairs) {
    const trimmed = pair.trim();
    if (!trimmed) {
      continue;
    }

    const separator = trimmed.indexOf("=");
    if (separator <= 0) {
      continue;
    }

    const name = trimmed.slice(0, separator).trim();
    let rawValue = trimmed.slice(separator + 1).trim();
    if (!name) {
      continue;
    }

    // Strip surrounding quotes per RFC 6265
    if (rawValue.length >= 2 && rawValue[0] === '"' && rawValue[rawValue.length - 1] === '"') {
      rawValue = rawValue.slice(1, -1);
    }

    try {
      output[name] = decodeURIComponent(rawValue);
    } catch {
      output[name] = rawValue;
    }
  }

  return output;
}

export function getCookie(request: Request, name: string): string | undefined {
  const cookies = parseCookieHeader(request.headers.get("Cookie"));
  return cookies[name];
}

const VALID_COOKIE_NAME = /^[a-zA-Z0-9!#$%&'*+\-.^_`|~]+$/;
const VALID_COOKIE_DOMAIN = /^[a-zA-Z0-9.\-]+$/;
const VALID_COOKIE_PATH = /^[\x21-\x7E]*$/;

function validateNoControlChars(value: string, fieldName: string): void {
  if (/[\x00-\x1F\x7F]/.test(value)) {
    throw new Error(`${fieldName} contains invalid control characters`);
  }
}

export function serializeCookie(
  name: string,
  value: string,
  options: CookieSerializeOptions = {}
): string {
  if (!name || !VALID_COOKIE_NAME.test(name)) {
    throw new Error(`Invalid cookie name: "${name}"`);
  }
  validateNoControlChars(value, "Cookie value");

  if (options.domain && !VALID_COOKIE_DOMAIN.test(options.domain)) {
    throw new Error(`Invalid cookie domain: "${options.domain}"`);
  }

  const path = options.path || "/";
  if (!VALID_COOKIE_PATH.test(path) || /[\r\n]/.test(path)) {
    throw new Error(`Invalid cookie path: "${path}"`);
  }

  const segments = [`${name}=${encodeURIComponent(value)}`];

  if (Number.isFinite(options.maxAge)) {
    segments.push(`Max-Age=${Math.max(0, Math.floor(options.maxAge || 0))}`);
  }

  if (options.domain) {
    segments.push(`Domain=${options.domain}`);
  }

  segments.push(`Path=${path}`);

  if (options.expires) {
    segments.push(`Expires=${options.expires.toUTCString()}`);
  }

  if (options.httpOnly !== false) {
    segments.push("HttpOnly");
  }

  if (options.secure) {
    segments.push("Secure");
  }

  if (options.sameSite) {
    const validSameSite = ["Strict", "Lax", "None"];
    if (!validSameSite.includes(options.sameSite)) {
      throw new Error(`Invalid SameSite value: "${options.sameSite}"`);
    }
    segments.push(`SameSite=${options.sameSite}`);
  }

  return segments.join("; ");
}
