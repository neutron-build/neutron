import { hydrate, render, h, ComponentType } from "preact";
import {
  LoaderContext,
  ActionDataContext,
  NavigationContext,
  RouterContext,
  LoaderData,
  setNavigationState,
} from "./hooks.js";
import { getCurrentPath, getCurrentSearch, subscribe, navigate } from "./navigate.js";
import type { RouteHref } from "../core/typed-routes.js";
import { initIslands } from "./island-runtime.js";
import { decodeLoaderDataPayload, readInitialLoaderData } from "./serialization.js";
import { ClientErrorBoundary } from "./error-boundary.js";

interface RouteModule {
  id?: string;
  path?: string;
  parentId?: string | null;
  default: ComponentType<{ data?: unknown; params?: Record<string, string>; actionData?: unknown }>;
  loader?: () => Promise<unknown>;
  ErrorBoundary?: ComponentType<{ error: Error; reset?: () => void }>;
}

interface RouteRegistration {
  id?: string;
  path?: string;
  parentId?: string | null;
  isLayout?: boolean;
  load?: () => Promise<unknown>;
  default?: ComponentType<{ data?: unknown; params?: Record<string, string>; actionData?: unknown }>;
  loader?: () => Promise<unknown>;
  ErrorBoundary?: ComponentType<{ error: Error; reset?: () => void }>;
}

interface RouteInfo {
  id: string;
  path: string;
  module: RouteModule | null;
  load: () => Promise<RouteModule>;
  parentId: string | null;
  isLayout: boolean;
}

let routes: RouteInfo[] = [];
let currentRoute: RouteInfo | null = null;
let layouts: RouteInfo[] = [];
let currentUrl = "";
let initialized = false;
let hydrated = false;
let activeNavigationController: AbortController | null = null;
let latestNavigationRequestId = 0;

// ── CSS loading for SPA navigation ──
const loadedStylesheets = new Set<string>();
const CSS_LOAD_TIMEOUT_MS = 5000;

function initLoadedStylesheets(): void {
  document.querySelectorAll<HTMLLinkElement>('link[rel="stylesheet"]').forEach((link) => {
    if (link.href) {
      try {
        // Normalize to pathname for stable comparison across dev query params
        loadedStylesheets.add(new URL(link.href).pathname);
      } catch {
        loadedStylesheets.add(link.href);
      }
    }
  });
}

function normalizeStylesheetUrl(url: string): string {
  try {
    return new URL(url, window.location.origin).pathname;
  } catch {
    return url;
  }
}

async function loadMissingStylesheets(urls: string[]): Promise<void> {
  const missing = urls.filter((url) => !loadedStylesheets.has(normalizeStylesheetUrl(url)));
  if (missing.length === 0) return;

  await Promise.race([
    Promise.all(
      missing.map(
        (url) =>
          new Promise<void>((resolve) => {
            const link = document.createElement("link");
            link.rel = "stylesheet";
            link.href = url;
            link.setAttribute("data-neutron-nav", "true");
            link.onload = () => {
              loadedStylesheets.add(normalizeStylesheetUrl(url));
              resolve();
            };
            link.onerror = () => {
              // Don't block navigation on CSS error
              resolve();
            };
            document.head.appendChild(link);
          })
      )
    ),
    // Safety valve: full-page reload if CSS takes too long
    new Promise<void>((_, reject) =>
      setTimeout(() => reject(new Error("css-timeout")), CSS_LOAD_TIMEOUT_MS)
    ),
  ]);
}

export async function init() {
  if (initialized) return;
  initialized = true;

  if (import.meta.env.DEV) {
    await import("preact/debug");
  }

  window.__NEUTRON_ROUTER_ACTIVE__ = true;
  initLoadedStylesheets();

  const data = readInitialLoaderData();
  const pathname = getCurrentPath();
  currentUrl = pathname + getCurrentSearch();
  
  const route = findRoute(pathname);
  if (!route) {
    console.warn("No route found for", pathname);
    return;
  }
  
  currentRoute = route;
  layouts = getLayoutChain(route);
  await ensureRouteChainModules([...layouts, route]);
  
  hydrateApp(data);
  setNavigationState({ state: "idle" });

  window.addEventListener("neutron:data-updated", (event: Event) => {
    const data = (event as CustomEvent<LoaderData>).detail;
    if (!data) return;

    void handleIncomingDataUpdate(data);
  });
  
  subscribe((event) => {
    return handleNavigation(event.forceRevalidate === true);
  });

  // Global click interceptor — makes all same-origin <a> tags do SPA navigation.
  // Without this, only <Link> components would trigger SPA nav; raw <a> tags
  // would cause full page reloads (white flash, lost JS state).
  document.addEventListener("click", (event: MouseEvent) => {
    if (event.defaultPrevented) return;
    if (event.button !== 0) return;
    if (event.metaKey || event.altKey || event.ctrlKey || event.shiftKey) return;

    const target = event.target;
    const anchor = target instanceof Element ? target.closest("a") : null;
    if (!anchor) return;
    if (anchor.target && anchor.target !== "_self") return;
    if (anchor.hasAttribute("download")) return;
    if (anchor.origin !== window.location.origin) return;

    const href = anchor.pathname + anchor.search;
    event.preventDefault();
    navigate(href as RouteHref);
  });
}

async function handleIncomingDataUpdate(data: LoaderData): Promise<void> {
  const pathname = getCurrentPath();
  const route = findRoute(pathname);
  if (route) {
    currentRoute = route;
    layouts = getLayoutChain(route);
    await ensureRouteChainModules([...layouts, route]);
  }

  applyData(data);
}

function findRoute(pathname: string): RouteInfo | null {
  for (const route of routes) {
    if (route.isLayout) continue;
    if (matchPath(route.path, pathname)) {
      return route;
    }
  }
  return null;
}

function matchPath(pattern: string, pathname: string): boolean {
  if (pattern === pathname) return true;
  
  const patternParts = pattern.split("/").filter(Boolean);
  const pathParts = pathname.split("/").filter(Boolean);
  
  if (patternParts.length !== pathParts.length) return false;
  
  for (let i = 0; i < patternParts.length; i++) {
    const p = patternParts[i];
    if (p.startsWith(":") || p === "*") continue;
    if (p !== pathParts[i]) return false;
  }
  
  return true;
}

function getLayoutChain(route: RouteInfo): RouteInfo[] {
  const chain: RouteInfo[] = [];
  let currentId: string | null = route.parentId;
  
  while (currentId) {
    const layout = routes.find((r) => r.id === currentId);
    if (layout) {
      chain.unshift(layout);
      currentId = layout.parentId;
    } else {
      break;
    }
  }
  
  return chain;
}

function hydrateApp(data: LoaderData) {
  if (!currentRoute) return;
  
  const pathname = getCurrentPath();
  const search = getCurrentSearch();
  const params = extractParams(currentRoute.path, pathname);
  
  const actionData = data.__action__;
  const loaderData = { ...data };
  delete loaderData.__action__;
  
  // Build the element tree
  const allRoutes = [...layouts, currentRoute];
  window.__NEUTRON_ACTIVE_ROUTE_IDS__ = allRoutes.map((route) => route.id);

  interface RouteElement {
    Component: ComponentType;
    data: unknown;
    params: Record<string, string>;
    ErrorBoundary?: ComponentType<{ error: Error; reset?: () => void }>;
  }

  const elements: RouteElement[] = [];

  for (const r of allRoutes) {
    if (!r.module) {
      return;
    }
    const routeData = loaderData[r.id];
    elements.push({
      Component: r.module.default,
      data: routeData,
      params,
      ErrorBoundary: r.module.ErrorBoundary,
    });
  }

  // Build nested element with per-route error boundaries
  let element: preact.VNode | null = null;

  for (let i = elements.length - 1; i >= 0; i--) {
    const { Component, data: routeData, params: routeParams, ErrorBoundary } = elements[i];

    if (element === null) {
      element = h(Component, { data: routeData, params: routeParams, actionData } as preact.ComponentProps<typeof Component>);
    } else {
      element = h(Component, { data: routeData } as preact.ComponentProps<typeof Component>, element);
    }

    // Wrap with route's ErrorBoundary if exported
    if (ErrorBoundary) {
      element = h(ClientErrorBoundary, { fallback: ErrorBoundary }, element);
    }
  }

  if (!element) return;

  // Root-level error boundary catches anything not caught by route boundaries
  element = h(ClientErrorBoundary, null, element);

  // Wrap with contexts
  const app = h(
    RouterContext.Provider,
    { value: { routeId: currentRoute.id, pathname, search, params } },
    h(
      LoaderContext.Provider,
      { value: loaderData },
      h(
        ActionDataContext.Provider,
        { value: actionData },
        h(
          NavigationContext.Provider,
          { value: { state: "idle" } },
          element
        )
      )
    )
  );
  
  const appElement = document.getElementById("app");
  if (appElement) {
    try {
      if (!hydrated) {
        hydrate(app, appElement);
        hydrated = true;
      } else {
        render(app, appElement);
      }
    } catch (err) {
      console.error("[neutron] Hydration failed, keeping SSR HTML:", err);
      // Don't dispatch neutron:hydrated — the init style stays in place
      // and the SSR HTML remains intact.
      return;
    }
    initIslands();

    // Notify ScrollReveal (and other components) that hydration is complete.
    // New DOM nodes may have been created — they need reveal processing.
    document.dispatchEvent(new CustomEvent("neutron:hydrated"));
  }
}

function applyData(data: LoaderData): void {
  const decoded = decodeLoaderDataPayload(data);

  // Extract and apply head HTML if present (from SPA navigation JSON response)
  const headHtml = (decoded as Record<string, unknown>).__head__;
  if (typeof headHtml === "string") {
    applyHeadHtml(headHtml);
    delete (decoded as Record<string, unknown>).__head__;
  }

  window.__NEUTRON_DATA__ = decoded;
  hydrateApp(decoded);
}

/**
 * Apply server-rendered head HTML to the live document.
 * Updates <title>, <meta>, and <link rel="canonical"> tags.
 */
function applyHeadHtml(headHtml: string): void {
  // Parse title
  const titleMatch = headHtml.match(/<title>([^<]*)<\/title>/);
  if (titleMatch) {
    document.title = titleMatch[1];
  }

  // Collect new meta tags from the head HTML
  const metaPattern = /<meta\s+([^>]+)>/g;
  const newMetas: Array<{ attrs: Record<string, string> }> = [];
  let match: RegExpExecArray | null;
  while ((match = metaPattern.exec(headHtml)) !== null) {
    const attrs = parseHtmlAttributes(match[1]);
    newMetas.push({ attrs });
  }

  // Remove existing managed meta tags (name=, property=) that we'll replace
  const managedSelectors = new Set<string>();
  for (const meta of newMetas) {
    if (meta.attrs.name) {
      managedSelectors.add(`meta[name="${meta.attrs.name}"]`);
    } else if (meta.attrs.property) {
      managedSelectors.add(`meta[property="${meta.attrs.property}"]`);
    }
  }
  for (const selector of managedSelectors) {
    const existing = document.head.querySelector(selector);
    if (existing) {
      existing.remove();
    }
  }

  // Insert new meta tags
  for (const meta of newMetas) {
    // Skip charset and viewport — these don't change per-page
    if (meta.attrs.charset || meta.attrs.name === "viewport") continue;
    const el = document.createElement("meta");
    for (const [key, value] of Object.entries(meta.attrs)) {
      el.setAttribute(key, value);
    }
    document.head.appendChild(el);
  }

  // Update canonical link (validate same-origin to prevent open redirects)
  const canonicalMatch = headHtml.match(/<link\s+rel="canonical"\s+href="([^"]*)"[^>]*>/);
  let canonical = document.head.querySelector('link[rel="canonical"]');
  if (canonicalMatch) {
    let isSameOrigin = false;
    try {
      const canonUrl = new URL(canonicalMatch[1], window.location.origin);
      isSameOrigin = canonUrl.origin === window.location.origin;
    } catch {
      // ignore invalid URLs
    }
    if (isSameOrigin) {
      if (!canonical) {
        canonical = document.createElement("link");
        canonical.setAttribute("rel", "canonical");
        document.head.appendChild(canonical);
      }
      canonical.setAttribute("href", canonicalMatch[1]);
    }
  } else if (canonical) {
    canonical.remove();
  }
}

function parseHtmlAttributes(attrString: string): Record<string, string> {
  const attrs: Record<string, string> = {};
  const pattern = /(\w[\w-]*)="([^"]*)"/g;
  let m: RegExpExecArray | null;
  while ((m = pattern.exec(attrString)) !== null) {
    attrs[m[1]] = m[2];
  }
  return attrs;
}

function extractParams(pattern: string, pathname: string): Record<string, string> {
  const params: Record<string, string> = {};
  const patternParts = pattern.split("/").filter(Boolean);
  const pathParts = pathname.split("/").filter(Boolean);
  
  for (let i = 0; i < patternParts.length; i++) {
    const p = patternParts[i];
    if (p.startsWith(":")) {
      params[p.slice(1)] = pathParts[i] || "";
    } else if (p === "*") {
      params["*"] = pathParts.slice(i).join("/");
      break;
    }
  }
  
  return params;
}

async function handleNavigation(forceRevalidate: boolean = false) {
  const pathname = getCurrentPath();
  const search = getCurrentSearch();
  const nextUrl = pathname + search;

  if (!forceRevalidate && nextUrl === currentUrl) {
    return;
  }

  const previousUrl = currentUrl;
  const previousPathname = toPathname(previousUrl) || pathname;
  const previousSearch = toSearch(previousUrl);

  const route = findRoute(pathname);
  if (!route) {
    window.location.reload();
    return;
  }

  const previousRoute = currentRoute;
  const previousLayouts = [...layouts];
  const nextLayouts = getLayoutChain(route);

  const previousSnapshots = previousRoute
    ? buildRouteSnapshots([...previousLayouts, previousRoute], previousPathname)
    : [];
  const nextSnapshots = buildRouteSnapshots([...nextLayouts, route], pathname);

  const searchChanged = previousSearch !== search;
  const requestedRouteIds = forceRevalidate || searchChanged
    ? nextSnapshots.map((snapshot) => snapshot.id)
    : diffRequestedRouteIds(previousSnapshots, nextSnapshots);

  currentRoute = route;
  layouts = nextLayouts;
  currentUrl = nextUrl;
  await ensureRouteChainModules([...nextLayouts, route]);

  const prefetched = window.__NEUTRON_PREFETCH_CACHE__?.[nextUrl];
  if (prefetched && !forceRevalidate) {
    // CSS should already be loaded during prefetch, but ensure it
    const prefetchCss = (prefetched as Record<string, unknown>).__css__;
    if (Array.isArray(prefetchCss)) {
      try {
        await loadMissingStylesheets(prefetchCss as string[]);
      } catch {
        // CSS timeout — fall back to full page load
        window.location.href = nextUrl;
        return;
      }
      delete (prefetched as Record<string, unknown>).__css__;
    }
    const merged = mergeLoaderData(window.__NEUTRON_DATA__ || {}, prefetched);
    applyData(merged);
    return;
  }

  if (activeNavigationController) {
    activeNavigationController.abort();
  }

  const controller = new AbortController();
  activeNavigationController = controller;
  const requestId = ++latestNavigationRequestId;

  setNavigationState({
    state: "loading",
    location: nextUrl,
  });

  const requestHeaders: Record<string, string> = {
    Accept: "application/json",
    "X-Neutron-Data": "true",
  };
  if (requestedRouteIds.length > 0) {
    requestHeaders["X-Neutron-Routes"] = requestedRouteIds.join(",");
  }

  try {
    const response = await fetch(pathname + search, {
      headers: requestHeaders,
      signal: controller.signal,
    });

    if (controller.signal.aborted || requestId !== latestNavigationRequestId) {
      return;
    }

    if (response.ok) {
      const payload = await response.json();
      const data = decodeLoaderDataPayload(payload);

      // Load CSS for the new route before DOM swap
      const cssUrls = (data as Record<string, unknown>).__css__;
      if (Array.isArray(cssUrls)) {
        try {
          await loadMissingStylesheets(cssUrls as string[]);
        } catch {
          // CSS timeout — fall back to full page load (guaranteed no FOUC)
          window.location.href = nextUrl;
          return;
        }
        delete (data as Record<string, unknown>).__css__;

        // Re-check: another navigation may have started while CSS was loading
        if (requestId !== latestNavigationRequestId) {
          return;
        }
      }

      const merged = mergeLoaderData(window.__NEUTRON_DATA__ || {}, data);
      applyData(merged);
      if (previousPathname !== pathname) {
        window.scrollTo({ top: 0, left: 0, behavior: "auto" });
      }
    } else {
      window.location.reload();
    }
  } catch (error) {
    if (controller.signal.aborted || requestId !== latestNavigationRequestId) {
      return;
    }
    window.location.reload();
  } finally {
    if (requestId === latestNavigationRequestId) {
      setNavigationState({ state: "idle" });
      if (activeNavigationController === controller) {
        activeNavigationController = null;
      }
    }
  }
}

interface RouteSnapshot {
  id: string;
  params: Record<string, string>;
}

function buildRouteSnapshots(
  chain: RouteInfo[],
  pathname: string
): RouteSnapshot[] {
  return chain.map((route) => ({
    id: route.id,
    params: extractParams(route.path, pathname),
  }));
}

function diffRequestedRouteIds(
  previous: RouteSnapshot[],
  next: RouteSnapshot[]
): string[] {
  const previousById = new Map(previous.map((entry) => [entry.id, entry.params]));
  const requested: string[] = [];
  for (const nextEntry of next) {
    const previousParams = previousById.get(nextEntry.id);
    if (!previousParams) {
      requested.push(nextEntry.id);
      continue;
    }
    if (!areParamsEqual(previousParams, nextEntry.params)) {
      requested.push(nextEntry.id);
    }
  }
  return requested;
}

function areParamsEqual(
  left: Record<string, string>,
  right: Record<string, string>
): boolean {
  const leftKeys = Object.keys(left);
  const rightKeys = Object.keys(right);
  if (leftKeys.length !== rightKeys.length) {
    return false;
  }
  for (const key of leftKeys) {
    if (left[key] !== right[key]) {
      return false;
    }
  }
  return true;
}

function mergeLoaderData(existing: LoaderData, incoming: LoaderData): LoaderData {
  const merged: LoaderData = { ...existing };
  delete (merged as Record<string, unknown>).__action__;

  for (const [key, value] of Object.entries(incoming)) {
    merged[key] = value;
  }

  return merged;
}

function toPathname(url: string): string {
  const [pathname] = url.split("?");
  return pathname || "/";
}

function toSearch(url: string): string {
  const index = url.indexOf("?");
  return index >= 0 ? url.slice(index) : "";
}

// Register routes from virtual module
export function registerRoutes(routeMap: Record<string, RouteRegistration | RouteModule>) {
  routes = Object.entries(routeMap as Record<string, RouteRegistration>).map(([id, registration]) => ({
    id,
    path:
      registration.path || id.replace(/^route:/, "").replace(/\.(tsx|ts|jsx|js|mdx)$/, ""),
    module:
      typeof registration.load === "function"
        ? null
        : normalizeRouteModule(registration),
    load:
      typeof registration.load === "function"
        ? async () => normalizeRouteModule(await registration.load!())
        : async () => normalizeRouteModule(registration),
    parentId: registration.parentId ?? null,
    isLayout: registration.isLayout === true,
  }));
}

async function ensureRouteChainModules(chain: RouteInfo[]): Promise<void> {
  await Promise.all(chain.map((route) => ensureRouteModule(route)));
}

async function ensureRouteModule(route: RouteInfo): Promise<RouteModule> {
  if (route.module) {
    return route.module;
  }

  const loaded = await route.load();
  route.module = loaded;
  return loaded;
}

function normalizeRouteModule(value: unknown): RouteModule {
  const module = value as RouteModule;
  if (!module || typeof module.default !== "function") {
    throw new Error("Invalid route module: missing default export.");
  }
  return module;
}
