/**
 * Validates that a URL is safe for redirection (relative or same-origin).
 *
 * @param url - The URL to validate
 * @param baseUrl - Optional base URL for validation (defaults to current origin in browser, required in SSR)
 * @returns true if the URL is safe for redirection
 *
 * @example
 * ```ts
 * // Safe redirects
 * isSafeRedirect("/dashboard"); // true - relative path
 * isSafeRedirect("dashboard/settings"); // true - relative path
 * isSafeRedirect("https://example.com/page", "https://example.com"); // true - same origin
 *
 * // Unsafe redirects (open redirect attacks)
 * isSafeRedirect("https://evil.com"); // false - different origin
 * isSafeRedirect("//evil.com"); // false - protocol-relative URL
 * isSafeRedirect("javascript:alert(1)"); // false - javascript protocol
 * ```
 */
export function isSafeRedirect(url: string, baseUrl?: string): boolean {
  // Empty or whitespace-only URLs are not safe
  if (!url || !url.trim()) {
    return false;
  }

  const trimmedUrl = url.trim();

  // SECURITY: Deny control characters to prevent HTTP response splitting
  // Check for \r (CR), \n (LF), \0 (null), and other control chars
  if (/[\r\n\0\x00-\x1F\x7F]/.test(trimmedUrl)) {
    return false;
  }

  // Deny javascript:, data:, vbscript:, file:, and other dangerous protocols
  const dangerousProtocols = /^(javascript|data|vbscript|file|about):/i;
  if (dangerousProtocols.test(trimmedUrl)) {
    return false;
  }

  // Deny protocol-relative URLs (//evil.com)
  if (trimmedUrl.startsWith("//")) {
    return false;
  }

  // Allow relative paths (doesn't start with protocol://)
  if (!trimmedUrl.match(/^[a-z][a-z0-9+.-]*:/i)) {
    return true;
  }

  // For absolute URLs, check if same origin
  if (baseUrl) {
    try {
      const targetUrl = new URL(trimmedUrl);
      const base = new URL(baseUrl);
      return targetUrl.origin === base.origin;
    } catch {
      return false;
    }
  }

  // If no baseUrl provided and it's an absolute URL, reject it
  // (developers should use safeRedirect with baseUrl for absolute URLs)
  return false;
}

/**
 * Creates a redirect response with validation to prevent open redirect attacks.
 *
 * SECURITY: This function validates the URL to ensure it's either:
 * - A relative path (e.g., "/dashboard", "settings")
 * - A same-origin absolute URL
 *
 * For absolute URLs, you must provide a baseUrl to validate same-origin.
 *
 * @param url - The URL to redirect to
 * @param options - Optional configuration
 * @param options.status - HTTP status code (default: 302)
 * @param options.baseUrl - Base URL for same-origin validation (required for absolute URLs)
 * @param options.fallback - Fallback URL if validation fails (default: "/")
 * @returns Response object with Location header
 *
 * @example
 * ```ts
 * // In a loader/action
 * export async function action({ request }: ActionArgs) {
 *   const formData = await request.formData();
 *   const next = formData.get("next") as string;
 *
 *   // GOOD: Validates the redirect URL
 *   return safeRedirect(next, {
 *     baseUrl: request.url,
 *     fallback: "/dashboard"
 *   });
 *
 *   // BAD: Direct redirect without validation
 *   // return redirect(next); // Open redirect vulnerability!
 * }
 * ```
 */
export function safeRedirect(
  url: string,
  options: {
    status?: number;
    baseUrl?: string;
    fallback?: string;
  } = {}
): Response {
  const { status = 302, baseUrl, fallback = "/" } = options;

  if (isSafeRedirect(url, baseUrl)) {
    return redirect(url, status);
  }

  // URL failed validation, use fallback
  console.warn(
    `[neutron] Blocked potentially unsafe redirect to "${url}". Using fallback: "${fallback}"`
  );
  return redirect(fallback, status);
}

/**
 * Creates a redirect response.
 *
 * SECURITY WARNING: This function does NOT validate the URL. If you're redirecting
 * to user-controlled URLs (e.g., from query parameters or form data), use safeRedirect()
 * instead to prevent open redirect attacks.
 *
 * @param url - The URL to redirect to
 * @param status - HTTP status code (default: 302)
 * @returns Response object with Location header
 *
 * @example
 * ```ts
 * // SAFE: Hardcoded redirect
 * return redirect("/dashboard");
 *
 * // UNSAFE: User-controlled redirect
 * const next = new URL(request.url).searchParams.get("next");
 * return redirect(next); // Open redirect vulnerability!
 *
 * // SAFE: Use safeRedirect for user input
 * return safeRedirect(next, { baseUrl: request.url, fallback: "/dashboard" });
 * ```
 */
export function redirect(url: string, status: number = 302): Response {
  // SECURITY: Validate against control characters to prevent HTTP response splitting
  if (/[\r\n\0\x00-\x1F\x7F]/.test(url)) {
    throw new Error(
      `[Neutron] Invalid redirect URL: contains control characters. ` +
      `This could enable HTTP response splitting attacks. URL: ${JSON.stringify(url.substring(0, 100))}`
    );
  }

  return new Response(null, {
    status,
    headers: {
      Location: url,
    },
  });
}

export function json(data: unknown, status: number = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json",
    },
  });
}

export function notFound(body?: string): Response {
  return new Response(body ?? "Not Found", { status: 404 });
}

export function isResponse(value: unknown): value is Response {
  return value instanceof Response;
}

export class DeferredData {
  private data: Record<string, unknown>;
  private pendingKeys: Set<string>;
  private resolvedData: Record<string, unknown>;
  public readonly subscribers: Array<(key: string, value: unknown) => void>;

  constructor(data: Record<string, unknown>) {
    this.data = data;
    this.pendingKeys = new Set();
    this.resolvedData = {};
    this.subscribers = [];

    // Identify which values are promises
    for (const [key, value] of Object.entries(data)) {
      if (value instanceof Promise) {
        this.pendingKeys.add(key);
        // Start resolving the promise
        value.then(
          (resolved) => {
            this.resolvedData[key] = resolved;
            this.pendingKeys.delete(key);
            this.notifySubscribers(key, resolved);
          },
          (error) => {
            this.resolvedData[key] = { __error: error.message || String(error) };
            this.pendingKeys.delete(key);
            this.notifySubscribers(key, this.resolvedData[key]);
          }
        );
      } else {
        // Not a promise, immediately available
        this.resolvedData[key] = value;
      }
    }
  }

  private notifySubscribers(key: string, value: unknown): void {
    for (const subscriber of this.subscribers) {
      subscriber(key, value);
    }
  }

  public subscribe(callback: (key: string, value: unknown) => void): () => void {
    this.subscribers.push(callback);
    return () => {
      const index = this.subscribers.indexOf(callback);
      if (index > -1) {
        this.subscribers.splice(index, 1);
      }
    };
  }

  public get(key: string): unknown {
    return this.resolvedData[key] ?? this.data[key];
  }

  public isPending(key: string): boolean {
    return this.pendingKeys.has(key);
  }

  public get done(): boolean {
    return this.pendingKeys.size === 0;
  }

  public get keys(): string[] {
    return Object.keys(this.data);
  }

  public toJSON(): Record<string, unknown> {
    return this.resolvedData;
  }
}

export function defer<T extends Record<string, unknown>>(data: T): DeferredData {
  return new DeferredData(data);
}

export function isDeferredData(value: unknown): value is DeferredData {
  return value instanceof DeferredData;
}
