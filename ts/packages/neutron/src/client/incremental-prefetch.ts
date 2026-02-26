/**
 * Incremental Prefetch with Layout Deduplication
 * Inspired by Next.js 16's smart prefetching
 *
 * - Only downloads missing parts not in cache
 * - Shared layouts download once
 * - Viewport-aware (cancels if link leaves viewport)
 * - Cache-aware (re-prefetches only uncached parts)
 */

interface PrefetchMetadata {
  layoutId: string;
  routeId: string;
  timestamp: number;
}

interface PrefetchCache {
  layouts: Map<string, any>;     // layoutId -> layout data
  pages: Map<string, any>;       // url -> page data
  metadata: Map<string, PrefetchMetadata>; // url -> metadata
}

const prefetchCache: PrefetchCache = {
  layouts: new Map(),
  pages: new Map(),
  metadata: new Map(),
};

const CACHE_TTL = 5 * 60 * 1000; // 5 minutes
const intersectionObserver = typeof window !== 'undefined'
  ? new IntersectionObserver(handleIntersection, { rootMargin: '50px' })
  : null;

const pendingPrefetches = new Map<string, AbortController>();

/**
 * Fetches page metadata to determine layout fingerprint
 */
async function fetchPageMetadata(url: string): Promise<PrefetchMetadata | null> {
  try {
    const response = await fetch(url, {
      method: 'HEAD',
      headers: {
        'X-Neutron-Prefetch-Metadata': 'true',
      },
    });

    const layoutId = response.headers.get('X-Neutron-Layout-Id') || 'default';
    const routeId = response.headers.get('X-Neutron-Route-Id') || url;

    return {
      layoutId,
      routeId,
      timestamp: Date.now(),
    };
  } catch {
    return null;
  }
}

/**
 * Incremental prefetch - only fetches what's not cached
 */
export async function incrementalPrefetch(url: string, signal?: AbortSignal): Promise<void> {
  // Check if we already have this page cached
  const cached = prefetchCache.pages.get(url);
  if (cached) {
    const metadata = prefetchCache.metadata.get(url);
    if (metadata && Date.now() - metadata.timestamp < CACHE_TTL) {
      // Cache still valid
      return;
    }
  }

  // Fetch page metadata
  const metadata = await fetchPageMetadata(url);
  if (!metadata || signal?.aborted) return;

  const { layoutId } = metadata;

  // Check if we have this layout cached
  const hasLayout = prefetchCache.layouts.has(layoutId);

  try {
    if (hasLayout) {
      // Only fetch page data (layout already cached)
      const response = await fetch(url, {
        headers: {
          'Accept': 'application/json',
          'X-Neutron-Data': 'true',
          'X-Neutron-Skip-Layout': 'true', // Request only page data
        },
        signal,
      });

      if (signal?.aborted) return;

      const pageData = await response.json();
      prefetchCache.pages.set(url, { layout: layoutId, data: pageData });
      prefetchCache.metadata.set(url, metadata);
    } else {
      // Fetch both layout and page data
      const response = await fetch(url, {
        headers: {
          'Accept': 'application/json',
          'X-Neutron-Data': 'true',
        },
        signal,
      });

      if (signal?.aborted) return;

      const fullData = await response.json();

      // Cache layout separately for reuse
      if (fullData.layout) {
        prefetchCache.layouts.set(layoutId, fullData.layout);
      }

      prefetchCache.pages.set(url, fullData);
      prefetchCache.metadata.set(url, metadata);
    }
  } catch (error) {
    if (!signal?.aborted) {
      console.warn('[Neutron] Prefetch failed:', url, error);
    }
  }
}

/**
 * Get cached page data if available
 */
export function getCachedPage(url: string): any {
  const cached = prefetchCache.pages.get(url);
  if (!cached) return null;

  const metadata = prefetchCache.metadata.get(url);
  if (metadata && Date.now() - metadata.timestamp < CACHE_TTL) {
    return cached;
  }

  // Cache expired, clean up
  prefetchCache.pages.delete(url);
  prefetchCache.metadata.delete(url);
  return null;
}

/**
 * Clear prefetch cache
 */
export function clearPrefetchCache(): void {
  prefetchCache.layouts.clear();
  prefetchCache.pages.clear();
  prefetchCache.metadata.clear();
}

/**
 * Clear prefetch cache for specific URL
 */
export function clearPrefetchCacheForUrl(url: string): void {
  prefetchCache.pages.delete(url);
  prefetchCache.metadata.delete(url);
}

/**
 * Intersection observer callback
 */
function handleIntersection(entries: IntersectionObserverEntry[]) {
  for (const entry of entries) {
    const link = entry.target as HTMLAnchorElement;
    const href = link.getAttribute('href');
    if (!href) continue;

    if (entry.isIntersecting) {
      // Link entered viewport - start prefetch
      startPrefetch(href);
    } else {
      // Link left viewport - cancel prefetch
      cancelPrefetch(href);
    }
  }
}

/**
 * Start prefetching a URL
 */
function startPrefetch(url: string): void {
  // Don't prefetch if already in progress
  if (pendingPrefetches.has(url)) return;

  const controller = new AbortController();
  pendingPrefetches.set(url, controller);

  incrementalPrefetch(url, controller.signal)
    .finally(() => {
      pendingPrefetches.delete(url);
    });
}

/**
 * Cancel prefetching a URL
 */
function cancelPrefetch(url: string): void {
  const controller = pendingPrefetches.get(url);
  if (controller) {
    controller.abort();
    pendingPrefetches.delete(url);
  }
}

/**
 * Setup incremental prefetch for all links with data-neutron-prefetch attribute
 */
export function setupIncrementalPrefetch(): void {
  if (typeof window === 'undefined' || !intersectionObserver) return;

  // Observe all prefetch links
  const links = document.querySelectorAll('a[data-neutron-prefetch="viewport"]');
  links.forEach(link => {
    intersectionObserver.observe(link);
  });

  // Handle hover prefetch
  const hoverLinks = document.querySelectorAll('a[data-neutron-prefetch="hover"]');
  hoverLinks.forEach(link => {
    link.addEventListener('mouseenter', () => {
      const href = link.getAttribute('href');
      if (href) startPrefetch(href);
    });
  });

  // Handle immediate prefetch
  const immediateLinks = document.querySelectorAll('a[data-neutron-prefetch="immediate"]');
  immediateLinks.forEach(link => {
    const href = link.getAttribute('href');
    if (href) startPrefetch(href);
  });
}

/**
 * Cleanup - disconnect observer
 */
export function cleanupIncrementalPrefetch(): void {
  intersectionObserver?.disconnect();

  // Cancel all pending prefetches
  for (const controller of pendingPrefetches.values()) {
    controller.abort();
  }
  pendingPrefetches.clear();
}

// Auto-setup on load
if (typeof window !== 'undefined') {
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', setupIncrementalPrefetch);
  } else {
    setupIncrementalPrefetch();
  }
}
