// Neutron Island Runtime
// Hydrates islands based on client directive

import { h, hydrate, render, type ComponentType } from "preact";

type ClientDirective = "load" | "visible" | "idle" | "media" | "only";

interface IslandElement extends HTMLElement {
  __neutronHydrated?: boolean;
  __neutronHydrationAttempts?: number;
}

type IslandManifest = Record<string, () => Promise<unknown>>;

// Manifest set by initIslands() for standalone (non-SPA) hydration. Maps an
// island module id (data-src) to a dynamic import() of its code-split chunk.
let islandManifest: IslandManifest | undefined;

async function hydrateIsland(island: IslandElement) {
  if (island.__neutronHydrated) return;

  const componentId = island.getAttribute("data-component");
  const propsJson = island.getAttribute("data-props");
  const props = safeParseProps(propsJson);

  // Resolve the component. In SPA mode the full-tree client render populates
  // window.__ISLAND_COMPONENTS__, so use that first. Otherwise (standalone
  // islands on a static page) dynamic-import the component's own chunk via the
  // manifest keyed by the marker's data-src module id.
  const registry = (window as unknown as {
    __ISLAND_COMPONENTS__?: Record<string, ComponentType<any>>;
  }).__ISLAND_COMPONENTS__ || {};
  let Component: ComponentType<any> | null = registry[componentId || ""] ?? null;

  if (!Component) {
    const src = island.dataset.src;
    if (src && islandManifest && Object.prototype.hasOwnProperty.call(islandManifest, src)) {
      try {
        const mod = await islandManifest[src]();
        Component = resolveIslandExport(
          mod as Record<string, unknown> | null | undefined,
          componentId
        );
      } catch (error) {
        console.error(`[Neutron] Failed to load island chunk ${src}:`, error);
      }
    }
  }

  if (!Component) {
    scheduleHydrationRetry(island, componentId);
    return;
  }

  try {
    const element = h(Component, props);
    // Preact's hydrate() walks the existing DOM and attaches event handlers
    // in place — it crashes with "Cannot read properties of null (reading
    // 'length')" if you call it against an empty container, because its diff
    // walker tries to iterate children that don't exist. So:
    //   - If the island already has SSR-rendered children, hydrate over them.
    //   - Otherwise (e.g. client="only" with no SSR), render a fresh tree.
    if (island.firstChild) {
      hydrate(element, island);
    } else {
      render(element, island);
    }
    island.__neutronHydrated = true;
  } catch (error) {
    console.error(`[Neutron] Failed to hydrate island ${componentId}:`, error);
  }
}

/**
 * Pick a component function out of a dynamically imported island chunk.
 *
 * Rollup preserves named exports (`export function ThemeToggle`) without
 * synthesizing `default`. The old `mod.default ?? mod` path then handed the
 * whole module namespace to `h()`, which silently fails to hydrate. Resolve:
 *   1. `default` if it is a function
 *   2. the sole function export (common island shape)
 *   3. export matching `data-component` (exact, PascalCase, or fuzzy)
 *   4. first function export as last resort
 */
export function resolveIslandExport(
  mod: Record<string, unknown> | null | undefined,
  componentId: string | null
): ComponentType<any> | null {
  if (!mod || typeof mod !== "object") {
    return null;
  }

  if (typeof mod.default === "function") {
    return mod.default as ComponentType<any>;
  }

  const fnKeys = Object.keys(mod).filter(
    (key) => key !== "__esModule" && typeof mod[key] === "function"
  );

  if (fnKeys.length === 1) {
    return mod[fnKeys[0]] as ComponentType<any>;
  }

  if (componentId) {
    if (typeof mod[componentId] === "function") {
      return mod[componentId] as ComponentType<any>;
    }

    const pascal = toPascalCase(componentId);
    if (pascal && typeof mod[pascal] === "function") {
      return mod[pascal] as ComponentType<any>;
    }

    const needle = normalizeExportName(componentId);
    for (const key of fnKeys) {
      if (normalizeExportName(key) === needle) {
        return mod[key] as ComponentType<any>;
      }
    }
  }

  if (fnKeys.length > 0) {
    return mod[fnKeys[0]] as ComponentType<any>;
  }

  return null;
}

function toPascalCase(id: string): string {
  return id
    .split(/[-_\s]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join("");
}

function normalizeExportName(name: string): string {
  return name.toLowerCase().replace(/[-_\s]/g, "");
}

function observeVisible(island: IslandElement) {
  const observer = new IntersectionObserver(
    ([entry]) => {
      if (entry.isIntersecting) {
        hydrateIsland(island);
        observer.disconnect();
      }
    },
    { threshold: 0.1 }
  );

  observer.observe(island);
}

function onIdle(callback: () => void) {
  if ("requestIdleCallback" in window) {
    (window as any).requestIdleCallback(callback);
  } else {
    setTimeout(callback, 200);
  }
}

function onMedia(island: IslandElement, query: string) {
  const mql = matchMedia(query);

  if (mql.matches) {
    hydrateIsland(island);
    return;
  }

  const handler = (e: MediaQueryListEvent) => {
    if (e.matches || !island.isConnected) {
      mql.removeEventListener("change", handler);
      if (e.matches && island.isConnected) hydrateIsland(island);
    }
  };

  mql.addEventListener("change", handler);
}

export function initIslands(manifest?: IslandManifest) {
  if (manifest) {
    islandManifest = manifest;
  }
  const islands = document.querySelectorAll<IslandElement>("neutron-island");

  islands.forEach((island) => {
    if (island.__neutronHydrated) return;

    const client = island.getAttribute("data-client") as ClientDirective | null;
    const media = island.getAttribute("data-media");

    switch (client) {
      case "load":
        hydrateIsland(island);
        break;

      case "visible":
        observeVisible(island);
        break;

      case "idle":
        onIdle(() => hydrateIsland(island));
        break;

      case "media":
        if (!media) {
          hydrateIsland(island);
        } else {
          onMedia(island, media);
        }
        break;

      case "only":
        hydrateIsland(island);
        break;
    }
  });
}

function safeParseProps(propsJson: string | null): Record<string, unknown> {
  if (!propsJson) {
    return {};
  }

  try {
    const parsed = JSON.parse(propsJson);
    if (parsed && typeof parsed === "object") {
      // SECURITY: Validate against prototype pollution
      if (hasPrototypePollution(parsed)) {
        console.error("[Neutron] Blocked potentially malicious island props");
        return {};
      }
      return parsed as Record<string, unknown>;
    }
  } catch (error) {
    console.warn("[Neutron] Failed to parse island props JSON.", error);
  }
  return {};
}

function hasPrototypePollution(obj: any, visited = new WeakSet()): boolean {
  if (!obj || typeof obj !== "object") return false;

  // Prevent infinite recursion on circular references
  if (visited.has(obj)) return false;
  visited.add(obj);

  // Check current level
  if (
    obj.hasOwnProperty("__proto__") ||
    obj.hasOwnProperty("constructor") ||
    obj.hasOwnProperty("prototype")
  ) {
    return true;
  }

  // Recursively check nested objects and arrays
  for (const key in obj) {
    if (obj.hasOwnProperty(key)) {
      const value = obj[key];
      if (value && typeof value === "object") {
        if (hasPrototypePollution(value, visited)) {
          return true;
        }
      }
    }
  }

  return false;
}

function scheduleHydrationRetry(island: IslandElement, componentId: string | null): void {
  const attempts = island.__neutronHydrationAttempts || 0;
  if (attempts >= 8) {
    console.warn(`[Neutron] Island component not found after retries: ${componentId}`);
    return;
  }

  island.__neutronHydrationAttempts = attempts + 1;
  const delayMs = Math.min(400, 40 * (attempts + 1));
  window.setTimeout(() => {
    if (island.isConnected) void hydrateIsland(island);
  }, delayMs);
}
