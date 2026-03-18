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
  const current = window.location.pathname + window.location.search;

  if (target === current) {
    handlePopState(true, { forceRevalidate: true });
    return;
  }

  // Check if navigation is blocked
  if (isNavigationBlocked(target)) {
    window.dispatchEvent(new CustomEvent('neutron:navigation-blocked', {
      detail: { from: current, to: target }
    }));
    return;
  }

  window.history.pushState(null, "", target);
  handlePopState(true);
}

export function go(delta: number): void {
  window.history.go(delta);
}

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
    if (hasWildcard && pathSegments.length < routeSegments.length - 1) continue;

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

export function extractParams(routePattern: string, pathname: string): Record<string, string> {
  const params: Record<string, string> = {};
  const routeSegments = routePattern.split("/").filter(Boolean);
  const pathSegments = pathname.split("/").filter(Boolean);
  
  for (let i = 0; i < routeSegments.length; i++) {
    const routeSeg = routeSegments[i];
    const pathSeg = pathSegments[i];
    
    if (routeSeg.startsWith(":")) {
      params[routeSeg.slice(1)] = pathSeg;
    } else if (routeSeg === "*") {
      params["*"] = pathSegments.slice(i).join("/");
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
