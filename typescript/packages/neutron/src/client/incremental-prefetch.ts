/**
 * Link prefetching: warm a navigation's payload before the click lands.
 *
 * A same-origin link is prefetched when it enters the viewport or on pointer
 * intent (hover / touch-start / keyboard focus). The payload is stored in the
 * shared prefetch cache, which is the one `handleNavigation` reads, so a click
 * on a warmed link renders with **zero network**.
 *
 * ## What this replaced, and why none of it worked
 *
 * The previous implementation was built on three server features that do not
 * exist. It sent `X-Neutron-Prefetch-Metadata` on a HEAD request and read
 * `X-Neutron-Layout-Id` / `X-Neutron-Route-Id` off the response — the server
 * sets neither, so every link resolved to the same `'default'` layout id. That
 * made its `hasLayout` check true for every link after the first, which routed
 * every later prefetch through an `X-Neutron-Skip-Layout` request the server
 * also does not implement, and stored the result under a *different shape*
 * from the first entry. Two requests per link, to produce data in an
 * inconsistent format.
 *
 * None of which mattered, because it wrote to module-local Maps that nothing
 * read, and nothing imported this module into the browser bundle in the first
 * place — the auto-setup at the bottom of the file never ran.
 *
 * This version fetches exactly what a navigation fetches, in one request,
 * stores it where a navigation looks, and is imported by the client runtime.
 */

import { decodeLoaderDataPayload } from "./serialization.js";
import { hasFreshPrefetch, storePrefetch } from "./prefetch-cache.js";

export { clearPrefetchCache, clearPrefetchCacheForUrl } from "./prefetch-cache.js";

/** Links currently being warmed, so intent and viewport do not double-fetch. */
const inFlight = new Map<string, AbortController>();

/** How long pointer intent waits before spending a request. */
const HOVER_DELAY_MS = 65;

const observed = new WeakSet<Element>();
let intersectionObserver: IntersectionObserver | null = null;
let installed = false;
let hoverTimer: ReturnType<typeof setTimeout> | null = null;

function currentUrl(): string {
  return window.location.pathname + window.location.search;
}

/**
 * Whether prefetching is appropriate at all right now.
 *
 * Speculative requests are a cost someone else pays: on a metered or slow
 * connection, warming links the user never clicks is worse than the latency it
 * saves. Both signals are advisory and absent in some browsers, so the default
 * when nothing is known is to prefetch.
 */
function prefetchAllowed(): boolean {
  const connection = (
    navigator as Navigator & {
      connection?: { saveData?: boolean; effectiveType?: string };
    }
  ).connection;
  if (!connection) return true;
  if (connection.saveData) return false;
  const type = connection.effectiveType;
  return type !== "slow-2g" && type !== "2g";
}

/**
 * The prefetchable href for an anchor, or null.
 *
 * Deliberately conservative: anything that is not a plain same-origin document
 * navigation is left alone. `data-neutron-prefetch="false"` opts a link out —
 * the escape hatch for a link whose GET is not side-effect free.
 */
function prefetchableHref(anchor: HTMLAnchorElement): string | null {
  if (anchor.dataset.neutronPrefetch === "false") return null;
  if (anchor.target && anchor.target !== "_self") return null;
  if (anchor.hasAttribute("download")) return null;
  if (anchor.origin !== window.location.origin) return null;

  const protocol = anchor.protocol;
  if (protocol !== "http:" && protocol !== "https:") return null;

  const href = anchor.pathname + anchor.search;
  // The page we are on needs no warming.
  if (href === currentUrl()) return null;
  return href;
}

/**
 * Warm `url` unless it is already warm or in flight.
 *
 * Failures are silent by design. A prefetch is speculative: if it does not
 * arrive, the click falls back to the normal fetch, which is exactly the
 * behavior without any prefetching. Logging per failed speculative request
 * turns a flaky network into console noise.
 */
export async function incrementalPrefetch(url: string): Promise<void> {
  if (typeof window === "undefined") return;
  if (!prefetchAllowed()) return;
  if (hasFreshPrefetch(url) || inFlight.has(url)) return;

  const controller = new AbortController();
  inFlight.set(url, controller);
  try {
    const response = await fetch(url, {
      headers: {
        Accept: "application/json",
        "X-Neutron-Data": "true",
        // Marks the request speculative so a server or proxy can treat it
        // differently from a user-initiated navigation. Advisory only.
        Purpose: "prefetch",
      },
      signal: controller.signal,
    });
    if (!response.ok) return;
    // A redirect means the destination is not what was linked. Warming the
    // wrong URL would make the click render a page the server would not serve
    // for it — including, behind auth middleware, one it would have refused.
    if (response.redirected) return;
    const contentType = response.headers.get("content-type") || "";
    if (!contentType.includes("application/json")) return;

    storePrefetch(url, decodeLoaderDataPayload(await response.json()));
  } catch {
    // Aborted or offline: leave the URL unwarmed.
  } finally {
    inFlight.delete(url);
  }
}

function handleIntersection(list: IntersectionObserverEntry[]): void {
  for (const entry of list) {
    if (!entry.isIntersecting) continue;
    const anchor = entry.target as HTMLAnchorElement;
    const href = prefetchableHref(anchor);
    if (href) void incrementalPrefetch(href);
    // One warm per link per page-view; the cache decides the rest.
    intersectionObserver?.unobserve(anchor);
  }
}

function onPointerIntent(event: Event): void {
  const target = event.target;
  const anchor = target instanceof Element ? target.closest("a") : null;
  if (!anchor) return;
  const href = prefetchableHref(anchor as HTMLAnchorElement);
  if (!href) return;

  if (hoverTimer) clearTimeout(hoverTimer);
  // A short delay keeps a pointer crossing a nav bar from warming every link
  // in it; real intent outlasts the sweep.
  hoverTimer = setTimeout(() => void incrementalPrefetch(href), HOVER_DELAY_MS);
}

function cancelPointerIntent(): void {
  if (hoverTimer) {
    clearTimeout(hoverTimer);
    hoverTimer = null;
  }
}

/**
 * Install the prefetch triggers. Idempotent. Called by the client runtime, so
 * apps do not need to call it.
 */
export function setupIncrementalPrefetch(): void {
  if (typeof window === "undefined" || installed) return;
  installed = true;

  // Capture phase, because a link inside a component that stops propagation
  // would otherwise never be warmed.
  document.addEventListener("pointerenter", onPointerIntent, true);
  document.addEventListener("focusin", onPointerIntent, true);
  document.addEventListener("pointerleave", cancelPointerIntent, true);
  document.addEventListener("touchstart", onPointerIntent, {
    capture: true,
    passive: true,
  });

  if ("IntersectionObserver" in window) {
    intersectionObserver = new IntersectionObserver(handleIntersection, {
      rootMargin: "200px",
    });
    observeVisibleLinks();
  }
}

/**
 * Observe same-origin links currently in the document.
 *
 * Called again after each navigation: a client-rendered page brings its own
 * links, and an observer that only ever saw the first page's DOM would warm
 * nothing from the second one onward.
 */
export function observeVisibleLinks(): void {
  if (!intersectionObserver) return;
  document.querySelectorAll("a[href]").forEach((element) => {
    if (observed.has(element)) return;
    const anchor = element as HTMLAnchorElement;
    if (!prefetchableHref(anchor)) return;
    observed.add(element);
    intersectionObserver?.observe(anchor);
  });
}

export function cleanupIncrementalPrefetch(): void {
  document.removeEventListener("pointerenter", onPointerIntent, true);
  document.removeEventListener("focusin", onPointerIntent, true);
  document.removeEventListener("pointerleave", cancelPointerIntent, true);
  intersectionObserver?.disconnect();
  intersectionObserver = null;
  cancelPointerIntent();
  for (const controller of inFlight.values()) controller.abort();
  inFlight.clear();
  installed = false;
}

/** The payload warmed for `url`, without consuming it. */
export function getCachedPage(url: string): unknown {
  if (!hasFreshPrefetch(url)) return null;
  return window.__NEUTRON_PREFETCH_CACHE__?.[url] ?? null;
}
