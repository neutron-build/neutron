import type { RouteHref } from "../core/typed-routes.js";

let currentPath = typeof window !== "undefined" ? window.location.pathname : "/";
let currentSearch = typeof window !== "undefined" ? window.location.search : "";
export interface NavigationListenerEvent {
  forceRevalidate?: boolean;
}

const listeners = new Set<(event: NavigationListenerEvent) => void | Promise<void>>();

export interface BlockerArgs {
  currentLocation: { pathname: string; search: string; hash: string; href: string };
  nextLocation: { pathname: string; search: string; hash: string; href: string };
}

export interface BlockerFunction {
  (args: BlockerArgs): boolean;
}

interface Blocker {
  id: number;
  shouldBlock: boolean | BlockerFunction;
}

const blockers = new Map<number, Blocker>();
let blockerIdCounter = 0;

type DocumentWithViewTransition = Document & {
  startViewTransition?: (update: () => void | Promise<void>) => {
    finished?: Promise<void>;
  };
};

export function getCurrentPath(): string {
  return currentPath;
}

export function getCurrentSearch(): string {
  return currentSearch;
}

export function subscribe(
  listener: (event: NavigationListenerEvent) => void | Promise<void>
): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function registerBlocker(shouldBlock: boolean | BlockerFunction): number {
  const id = ++blockerIdCounter;
  blockers.set(id, { id, shouldBlock });
  return id;
}

export function unregisterBlocker(id: number): void {
  blockers.delete(id);
}

function isNavigationBlocked(nextUrl: string): boolean {
  if (blockers.size === 0) return false;

  const currentLoc = window.location;
  const nextLoc = new URL(nextUrl, window.location.href);

  const currentLocation = {
    pathname: currentLoc.pathname,
    search: currentLoc.search,
    hash: currentLoc.hash,
    href: currentLoc.href,
  };

  const nextLocation = {
    pathname: nextLoc.pathname,
    search: nextLoc.search,
    hash: nextLoc.hash,
    href: nextLoc.href,
  };

  for (const blocker of blockers.values()) {
    const shouldBlock = typeof blocker.shouldBlock === 'function'
      ? blocker.shouldBlock({ currentLocation, nextLocation })
      : blocker.shouldBlock;

    if (shouldBlock) return true;
  }

  return false;
}

export function navigate(to: RouteHref): void {
  const resolved = new URL(to, window.location.href);
  if (resolved.origin !== window.location.origin) {
    window.location.href = resolved.toString();
    return;
  }
  const target = resolved.pathname + resolved.search;
  // Preserve the hash in the pushed URL so anchor links survive SPA nav.
  const targetWithHash = target + resolved.hash;
  const current = window.location.pathname + window.location.search;

  if (target === current) {
    // Same route: if the hash changed, update it and scroll to the target;
    // otherwise revalidate the current route in place.
    if (resolved.hash && resolved.hash !== window.location.hash) {
      window.history.pushState(null, "", targetWithHash);
      scrollToHash(resolved.hash);
    } else {
      handlePopState(true, { forceRevalidate: true });
    }
    return;
  }

  // Check if navigation is blocked
  if (isNavigationBlocked(target)) {
    window.dispatchEvent(new CustomEvent('neutron:navigation-blocked', {
      detail: { from: current, to: target }
    }));
    return;
  }

  window.history.pushState(null, "", targetWithHash);
  handlePopState(true);
  if (resolved.hash) {
    scrollToHash(resolved.hash);
  }
}

// Scroll to a hash target after the route renders. Two rAFs let the new DOM
// paint first; relies on CSS scroll-margin-top for fixed-header offset (the
// web-standard approach) rather than hardcoding an offset.
function scrollToHash(hash: string): void {
  if (typeof document === "undefined") return;
  const id = decodeURIComponent(hash.replace(/^#/, ""));
  if (!id) return;
  requestAnimationFrame(() =>
    requestAnimationFrame(() => {
      const el = document.getElementById(id);
      if (el) el.scrollIntoView();
    })
  );
}

export function go(delta: number): void {
  window.history.go(delta);
}

// Debounce view transitions to prevent the flash artifact caused by
// the browser cancelling a transition mid-flight when a new one starts.
// If navigations happen faster than this threshold, skip the transition
// API and do an instant DOM swap instead.
const VT_DEBOUNCE_MS = 200;
let lastTransitionTime = 0;

function handlePopState(
  withTransition: boolean = true,
  event: NavigationListenerEvent = {}
) {
  const apply = (): Promise<void> => {
    currentPath = window.location.pathname;
    currentSearch = window.location.search;
    const results = Array.from(listeners).map((listener) => listener(event));
    return Promise.all(results).then(() => {});
  };

  if (withTransition && shouldUseViewTransitions()) {
    const now = performance.now();
    const elapsed = now - lastTransitionTime;
    lastTransitionTime = now;

    // Skip View Transitions if navigating too rapidly — prevents the
    // browser's snapshot cancellation flash artifact.
    if (elapsed < VT_DEBOUNCE_MS) {
      apply();
      return;
    }

    const doc = document as DocumentWithViewTransition;
    doc.startViewTransition?.(() => apply());
    return;
  }

  apply();
}

if (typeof window !== "undefined") {
  window.addEventListener("popstate", () => handlePopState(true));
}

export function matchRoute(pathname: string, routes: string[]): string | null {
  for (const route of routes) {
    if (route === pathname) return route;
    
    const routeSegments = route.split("/").filter(Boolean);
    const pathSegments = pathname.split("/").filter(Boolean);
    
    const hasWildcard = routeSegments.includes("*");
    if (!hasWildcard && routeSegments.length !== pathSegments.length) continue;
    // The server trie requires at least one segment after the wildcard
    // (router.ts skips empty wildcard values); zero-remainder paths must
    // not match here either.
    if (hasWildcard && pathSegments.length < routeSegments.length) continue;

    let matches = true;
    for (let i = 0; i < routeSegments.length; i++) {
      const routeSeg = routeSegments[i];
      const pathSeg = pathSegments[i];

      if (routeSeg === "*") {
        break;
      }
      if (routeSeg.startsWith(":")) {
        continue;
      }

      if (routeSeg !== pathSeg) {
        matches = false;
        break;
      }
    }
    
    if (matches) return route;
  }
  
  return null;
}

function safeDecodeSegment(segment: string | undefined): string {
  if (segment === undefined) return "";
  try {
    return decodeURIComponent(segment);
  } catch {
    return segment;
  }
}

export function extractParams(routePattern: string, pathname: string): Record<string, string> {
  const params: Record<string, string> = {};
  const routeSegments = routePattern.split("/").filter(Boolean);
  const pathSegments = pathname.split("/").filter(Boolean);

  for (let i = 0; i < routeSegments.length; i++) {
    const routeSeg = routeSegments[i];
    const pathSeg = pathSegments[i];

    if (routeSeg.startsWith(":")) {
      // The server decodes the request path before routing, so params arrive
      // decoded; the client must decode too or useParams differs pre/post
      // hydration for percent-encoded segments.
      params[routeSeg.slice(1)] = safeDecodeSegment(pathSeg);
    } else if (routeSeg === "*") {
      params["*"] = pathSegments.slice(i).map(safeDecodeSegment).join("/");
      break;
    }
  }

  return params;
}

function shouldUseViewTransitions(): boolean {
  if (typeof window === "undefined") {
    return false;
  }
  const doc = document as DocumentWithViewTransition;
  return Boolean(window.__NEUTRON_VIEW_TRANSITIONS__ && doc.startViewTransition);
}

declare global {
  interface Window {
    __NEUTRON_VIEW_TRANSITIONS__?: boolean;
    __NEUTRON_ROUTER_ACTIVE__?: boolean;
  }
}
