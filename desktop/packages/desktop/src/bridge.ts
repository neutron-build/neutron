/**
 * Neutron Desktop bridge — routes fetch() through the `neutron://` protocol.
 *
 * On desktop: `fetch("/api/users")` → `neutron://localhost/api/users`
 * On web: standard HTTP fetch (no transformation)
 *
 * Zero code changes between platforms.
 */

/** Check if running inside a Tauri desktop app. */
export function isDesktop(): boolean {
  return typeof window !== 'undefined' && '__TAURI__' in window;
}

/** Get the base URL for API requests. */
export function getBaseUrl(): string {
  if (isDesktop()) {
    return 'neutron://localhost';
  }
  return '';
}

/**
 * Fetch wrapper that automatically routes through `neutron://` on desktop.
 * Identical API to standard `fetch()`.
 */
export async function neutronFetch(
  input: string | URL | Request,
  init?: RequestInit,
): Promise<Response> {
  if (!isDesktop()) {
    return fetch(input, init);
  }

  let url: string;
  if (typeof input === 'string') {
    // Relative URLs get prefixed with neutron://localhost
    url = input.startsWith('/') ? `neutron://localhost${input}` : input;
  } else if (input instanceof URL) {
    url = input.toString();
  } else {
    url = input.url;
  }

  return fetch(url, init);
}

/**
 * Install the neutron:// fetch interceptor globally.
 * After calling this, all `fetch("/api/...")` calls automatically route
 * through the protocol bridge on desktop.
 */
export function installFetchInterceptor(): void {
  if (!isDesktop()) return;

  const originalFetch = window.fetch.bind(window);
  window.fetch = (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    if (typeof input === 'string' && input.startsWith('/')) {
      return originalFetch(`neutron://localhost${input}`, init);
    }
    return originalFetch(input, init);
  };
}
