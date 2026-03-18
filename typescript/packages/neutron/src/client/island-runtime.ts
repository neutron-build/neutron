// Neutron Island Runtime
// Hydrates islands based on client directive

import { h, hydrate } from "preact";

type ClientDirective = "load" | "visible" | "idle" | "media" | "only";

interface IslandElement extends HTMLElement {
  __neutronHydrated?: boolean;
  __neutronHydrationAttempts?: number;
}

async function hydrateIsland(island: IslandElement) {
  if (island.__neutronHydrated) return;

  const componentId = island.getAttribute("data-component");
  const propsJson = island.getAttribute("data-props");
  const props = safeParseProps(propsJson);

  // Get component from registry
  const registry = (window as any).__ISLAND_COMPONENTS__ || {};
  const Component = registry[componentId || ""];

  if (!Component) {
    scheduleHydrationRetry(island, componentId);
    return;
  }

  try {
    const element = h(Component, props);
    hydrate(element, island);
    island.__neutronHydrated = true;
  } catch (error) {
    console.error(`[Neutron] Failed to hydrate island ${componentId}:`, error);
  }
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

export function initIslands() {
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
        island.innerHTML = "";
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
