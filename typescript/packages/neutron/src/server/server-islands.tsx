/**
 * Server Islands - Progressive Server-Side Rendering
 * Inspired by Astro 6's server:defer pattern
 *
 * Allows on-demand rendering of dynamic components while static shell loads instantly
 */

import crypto from "node:crypto";
import { Fragment, h, type ComponentChildren } from "preact";
import { renderToString } from "preact-render-to-string";

export interface ServerIslandProps {
  children: ComponentChildren;
  fallback?: ComponentChildren;
  id?: string;
}

interface RegisteredIsland {
  render: () => Promise<ComponentChildren>;
  createdAt: number;
}

const ISLAND_ENTRY_TTL_MS = 5 * 60 * 1000;
const ISLAND_REGISTRY_MAX_SIZE = 10_000;
const islandRegistry = new Map<string, RegisteredIsland>();
let islandCounter = 0;

/**
 * Generates a unique island ID
 */
function generateIslandId(userProvidedId?: string): string {
  if (userProvidedId) return userProvidedId;
  return `island-${++islandCounter}-${crypto.randomBytes(4).toString("hex")}`;
}

/**
 * Server Island Component
 * On server: Returns fallback + script to fetch real content
 * On client: Just renders children
 */
export function ServerIsland({ children, fallback, id }: ServerIslandProps) {
  // Client-side: just render children
  if (typeof window !== "undefined") {
    return <>{children}</>;
  }

  // Server-side: render fallback + fetch script
  const islandId = generateIslandId(id);
  const islandIdJson = JSON.stringify(islandId);
  const islandEndpointJson = JSON.stringify(
    `/__neutron_island/${encodeURIComponent(islandId)}`
  );

  // Register component for later rendering
  registerIsland(islandId, async () => children);

  return (
    <>
      <div
        id={islandId}
        data-server-island
        data-island-id={islandId}
      >
        {fallback || <div>Loading...</div>}
      </div>
      <script
        dangerouslySetInnerHTML={{
          __html: `
(async function() {
  try {
    const el = document.getElementById(${islandIdJson});
    if (!el) return;

    const res = await fetch(${islandEndpointJson}, { cache: 'no-store' });
    if (!res.ok) throw new Error('Failed to load island');

    let html = await res.text();
    // SECURITY: Defense-in-depth — sanitize server-rendered HTML before injection
    html = html.replace(/<script\\b[^<]*(?:(?!<\\/script>)<[^<]*)*<\\/script>/gi, '');
    html = html.replace(/\\s+on\\w+\\s*=\\s*(?:"[^"]*"|'[^']*'|[^\\s>]*)/gi, '');
    el.innerHTML = html;

    // Dispatch event for hydration
    el.dispatchEvent(new CustomEvent('neutron:island-loaded', {
      detail: { islandId: ${islandIdJson} }
    }));
  } catch (err) {
    console.error('[Neutron] Server island load failed:', err);
  }
})();
          `.trim(),
        }}
      />
    </>
  );
}

/**
 * Get registered island component
 */
export function getIslandComponent(islandId: string): (() => Promise<ComponentChildren>) | undefined {
  const entry = islandRegistry.get(islandId);
  return entry?.render;
}

/**
 * Clear island registry (for SSR per-request cleanup)
 */
export function clearIslandRegistry(): void {
  islandRegistry.clear();
  islandCounter = 0;
}

/**
 * Check if an ID is a registered island
 */
export function isRegisteredIsland(islandId: string): boolean {
  return islandRegistry.has(islandId);
}

/**
 * Server Island route handler
 * Should be mounted at /__neutron_island/:id
 */
export async function handleIslandRequest(islandId: string): Promise<string | null> {
  pruneIslandRegistry();

  const entry = islandRegistry.get(islandId);
  if (!entry) {
    return null;
  }

  try {
    const content = await entry.render();
    islandRegistry.delete(islandId);
    return renderToString(h(Fragment, null, content));
  } catch (error) {
    islandRegistry.delete(islandId);
    console.error("[Neutron] Island render error:", error);
    return '<div class="island-error">Failed to load content</div>';
  }
}

function registerIsland(
  islandId: string,
  render: () => Promise<ComponentChildren>
): void {
  pruneIslandRegistry();
  islandRegistry.set(islandId, {
    render,
    createdAt: Date.now(),
  });
}

function pruneIslandRegistry(now: number = Date.now()): void {
  for (const [id, entry] of islandRegistry) {
    if (now - entry.createdAt > ISLAND_ENTRY_TTL_MS) {
      islandRegistry.delete(id);
    }
  }

  if (islandRegistry.size <= ISLAND_REGISTRY_MAX_SIZE) {
    return;
  }

  const overflow = islandRegistry.size - ISLAND_REGISTRY_MAX_SIZE;
  const oldestEntries = Array.from(islandRegistry.entries())
    .sort((left, right) => left[1].createdAt - right[1].createdAt)
    .slice(0, overflow);
  for (const [id] of oldestEntries) {
    islandRegistry.delete(id);
  }
}
