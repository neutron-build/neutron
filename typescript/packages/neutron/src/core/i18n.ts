import type { AppContext, MiddlewareFn } from "./types.js";

export interface I18nOptions {
  locales: string[];
  defaultLocale: string;
  strategy?: "prefix" | "prefix-except-default";
}

export interface ResolvedLocalePath {
  locale: string;
  pathname: string;
  pathWithoutLocale: string;
  hasLocalePrefix: boolean;
  redirectTo?: string;
}

export function resolveLocalePath(pathname: string, options: I18nOptions): ResolvedLocalePath {
  const normalized = normalizePath(pathname);
  const strategy = options.strategy || "prefix-except-default";
  const defaultLocale = options.defaultLocale;
  const locales = new Set(options.locales);

  if (!locales.has(defaultLocale)) {
    throw new Error(`defaultLocale "${defaultLocale}" must exist in locales.`);
  }

  const segments = normalized.split("/").filter(Boolean);
  const firstSegment = segments[0];
  const hasLocalePrefix = !!firstSegment && locales.has(firstSegment);

  if (hasLocalePrefix) {
    const locale = firstSegment as string;
    const withoutPrefix = "/" + segments.slice(1).join("/");
    const pathWithoutLocale = withoutPrefix === "/" ? "/" : withoutPrefix.replace(/\/+$/, "");

    if (strategy === "prefix-except-default" && locale === defaultLocale) {
      return {
        locale,
        pathname: normalized,
        pathWithoutLocale,
        hasLocalePrefix,
        redirectTo: pathWithoutLocale,
      };
    }

    return {
      locale,
      pathname: normalized,
      pathWithoutLocale,
      hasLocalePrefix,
    };
  }

  const redirectTo =
    strategy === "prefix"
      ? withLocalePath(normalized, defaultLocale, options)
      : undefined;

  return {
    locale: defaultLocale,
    pathname: normalized,
    pathWithoutLocale: normalized,
    hasLocalePrefix: false,
    redirectTo,
  };
}

export function stripLocalePrefix(pathname: string, options: I18nOptions): string {
  return resolveLocalePath(pathname, options).pathWithoutLocale;
}

export function withLocalePath(
  pathname: string,
  locale: string,
  options: I18nOptions
): string {
  const normalized = normalizePath(pathname);
  const strategy = options.strategy || "prefix-except-default";
  const defaultLocale = options.defaultLocale;

  if (!options.locales.includes(defaultLocale)) {
    // The same config check `resolveLocalePath` makes. Without it the identical
    // options object is rejected by two of these three functions and silently
    // accepted by the third, which returned a path computed against a default
    // locale the app does not serve. Validate the config before the argument:
    // a broken config is the more fundamental error, and reporting the argument
    // first sends the caller looking at the wrong value.
    throw new Error(`defaultLocale "${defaultLocale}" must exist in locales.`);
  }

  if (!options.locales.includes(locale)) {
    throw new Error(`Locale "${locale}" is not configured.`);
  }

  if (strategy === "prefix-except-default" && locale === defaultLocale) {
    return normalized;
  }

  if (normalized === "/") {
    return `/${locale}`;
  }

  return `/${locale}${normalized}`;
}

export function createI18nMiddleware(options: I18nOptions): MiddlewareFn {
  return async (request, context, next) => {
    const url = new URL(request.url);
    const resolved = resolveLocalePath(url.pathname, options);

    assignLocaleContext(context, resolved);

    if (
      resolved.redirectTo &&
      (request.method === "GET" || request.method === "HEAD")
    ) {
      // Deliberately NOT `Response.redirect()`. That returns a response whose
      // header guard is "immutable", so the very next middleware to set a
      // header on the way out -- request-id, CORS and compression are all in
      // the standard stack, and all of them do -- throws `TypeError: immutable`
      // and the redirect surfaces as an empty-body 500. Found by writing the
      // i18n docs and testing the samples against a live server.
      //
      // The Location is a path rather than an absolute URL for a second
      // reason: it is built from `request.url`, whose host behind a reverse
      // proxy is the internal one, so an absolute redirect can send a browser
      // to a hostname it cannot reach. A relative Location is valid per
      // RFC 7231 and resolves against the request the client actually made.
      const location = resolved.redirectTo + url.search;
      return new Response(null, { status: 307, headers: { location } });
    }

    return next();
  };
}

function assignLocaleContext(context: AppContext, resolved: ResolvedLocalePath): void {
  (context as Record<string, unknown>).locale = resolved.locale;
  (context as Record<string, unknown>).pathWithoutLocale = resolved.pathWithoutLocale;
}

function normalizePath(pathname: string): string {
  const value = pathname || "/";
  if (value === "/") {
    return value;
  }
  const prefixed = value.startsWith("/") ? value : `/${value}`;
  return prefixed.replace(/\/+$/, "");
}
