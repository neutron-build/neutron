/**
 * Shallow routing - Update URL without navigation
 * Inspired by SvelteKit's pushState/replaceState
 *
 * Useful for modals, tabs, filters, and multi-step forms where you want
 * the URL to change but don't want to trigger a full navigation.
 */

export interface ShallowRouteState {
  [key: string]: unknown;
}

export interface PushStateOptions {
  /**
   * Whether to trigger a popstate event (default: false)
   */
  triggerPopstate?: boolean;
}

/**
 * Push a new URL to browser history without navigation
 *
 * @example
 * ```typescript
 * // Open modal with URL state
 * pushState('/photos/123', { modalOpen: true });
 *
 * // User clicks back -> modal closes and URL reverts
 * ```
 */
export function pushState(
  url: string,
  state: ShallowRouteState = {},
  options: PushStateOptions = {}
): void {
  if (typeof window === 'undefined') return;

  const resolvedUrl = new URL(url, window.location.href);

  // Only allow same-origin URLs
  if (resolvedUrl.origin !== window.location.origin) {
    console.warn('[Neutron] pushState only works with same-origin URLs');
    return;
  }

  const targetUrl = resolvedUrl.pathname + resolvedUrl.search + resolvedUrl.hash;

  window.history.pushState(state, '', targetUrl);

  if (options.triggerPopstate) {
    window.dispatchEvent(new PopStateEvent('popstate', { state }));
  }
}

/**
 * Replace current URL in browser history without navigation
 *
 * @example
 * ```typescript
 * // Update URL params without adding to history
 * replaceState('/search?q=new-query', { query: 'new-query' });
 * ```
 */
export function replaceState(
  url: string,
  state: ShallowRouteState = {},
  options: PushStateOptions = {}
): void {
  if (typeof window === 'undefined') return;

  const resolvedUrl = new URL(url, window.location.href);

  if (resolvedUrl.origin !== window.location.origin) {
    console.warn('[Neutron] replaceState only works with same-origin URLs');
    return;
  }

  const targetUrl = resolvedUrl.pathname + resolvedUrl.search + resolvedUrl.hash;

  window.history.replaceState(state, '', targetUrl);

  if (options.triggerPopstate) {
    window.dispatchEvent(new PopStateEvent('popstate', { state }));
  }
}

/**
 * Get current history state
 */
export function getState<T extends ShallowRouteState = ShallowRouteState>(): T | null {
  if (typeof window === 'undefined') return null;
  return window.history.state as T | null;
}
