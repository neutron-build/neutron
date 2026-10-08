/**
 * Neutron Desktop bridge — routes fetch() through the `neutron://` protocol.
 *
 * On desktop: `fetch("/api/users")` → `neutron://localhost/api/users`
 * On web: standard HTTP fetch (no transformation)
 *
 * Dev mode: When `NEUTRON_DESKTOP_DEV=true`, the Rust backend injects
 * `window.__NEUTRON_DEV_MODE__ = true` and opens a TCP server on :3001.
 * The bridge then routes through standard HTTP so curl/Postman/DevTools
 * can inspect traffic normally.
 *
 * Zero code changes between platforms.
 */

/** Check if running inside a Tauri desktop app. */
export function isDesktop(): boolean {
  return typeof window !== 'undefined' && '__TAURI__' in window;
}

/** Check if desktop dev mode is active (TCP server instead of neutron://). */
export function isDevMode(): boolean {
  return typeof window !== 'undefined' && (window as any).__NEUTRON_DEV_MODE__ === true;
}

/** Get the base URL for API requests. */
export function getBaseUrl(): string {
  // Dev mode: use the TCP server for standard HTTP debugging
  if (isDevMode()) {
    const port = (window as any).__NEUTRON_DEV_PORT__ || 3001;
    return `http://127.0.0.1:${port}`;
  }

  // Production: use neutron:// protocol (no TCP port)
  if (isDesktop()) {
    return 'neutron://localhost';
  }

  // Web: relative URL
  return '';
}

/**
 * Re-issue a Request under a different URL WITHOUT losing its method, body,
 * headers or abort signal (NF-DESK-04). `new Request(url, existingRequest)`
 * treats the Request object as a RequestInit dictionary — every field it
 * doesn't know is silently ignored, flattening the request to a bodyless GET.
 * When the URL is unchanged the original Request is forwarded untouched
 * (its body stream can only be handed off once).
 */
function rebuildRequest(url: string, request: Request): Request {
  if (url === request.url) return request;
  const hasBody = request.body != null && request.method !== 'GET' && request.method !== 'HEAD';
  const init: RequestInit & { duplex?: 'half' } = {
    method: request.method,
    headers: new Headers(request.headers),
    body: hasBody ? request.body : undefined,
    signal: request.signal,
    ...(hasBody ? { duplex: 'half' } : {}),
  };
  return new Request(url, init);
}

/**
 * Fetch wrapper that automatically routes through `neutron://` on desktop,
 * or through the dev TCP server when dev mode is active.
 * Identical API to standard `fetch()`.
 */
export async function neutronFetch(
  input: string | URL | Request,
  init?: RequestInit,
): Promise<Response> {
  if (!isDesktop()) {
    return fetch(input, init);
  }

  const base = getBaseUrl();

  let url: string;
  if (typeof input === 'string') {
    // Relative URLs get prefixed with the appropriate base
    url = input.startsWith('/') ? `${base}${input}` : input;
  } else if (input instanceof URL) {
    url = input.toString();
  } else {
    url = input.url;
  }

  const requestInput = input instanceof Request ? rebuildRequest(url, input) : url;
  if (isDevMode() && url.startsWith(`${base}/`)) {
    const request = new Request(requestInput, init);
    const token = (window as any).__NEUTRON_DEV_TOKEN__;
    if (typeof token !== 'string' || !token) throw new Error('Desktop dev capability unavailable');
    request.headers.set('X-Neutron-Dev-Token', token);
    return fetch(request);
  }
  return fetch(requestInput, init);
}

/**
 * Install the fetch interceptor globally.
 * After calling this, all `fetch("/api/...")` calls automatically route
 * through the protocol bridge (or dev TCP server) on desktop.
 */
export function installFetchInterceptor(): void {
  if (!isDesktop()) return;

  const originalFetch = window.fetch.bind(window);
  window.fetch = (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    if (typeof input === 'string' && input.startsWith('/')) {
      const base = getBaseUrl();
      if (isDevMode()) {
        const request = new Request(`${base}${input}`, init);
        const token = (window as any).__NEUTRON_DEV_TOKEN__;
        if (typeof token !== 'string' || !token) return Promise.reject(new Error('Desktop dev capability unavailable'));
        request.headers.set('X-Neutron-Dev-Token', token);
        return originalFetch(request);
      }
      return originalFetch(`${base}${input}`, init);
    }
    return originalFetch(input, init);
  };
}
