import { h } from "preact";

type ClientDirective = "load" | "visible" | "idle" | "media" | "only";

interface IslandProps {
  component: preact.FunctionComponent<any>;
  client: ClientDirective;
  id?: string;
  media?: string;
  [key: string]: unknown;
}

const isServer = typeof window === "undefined";
let islandCounter = 0;
const componentIdMap = new WeakMap<preact.FunctionComponent<any>, string>();
const componentNameCounts = new Map<string, number>();

/**
 * Island component for progressive hydration
 * 
 * Usage:
 * ```tsx
 * import Counter from "./Counter.js";
 * <Island component={Counter} client="load" count={0} />
 * ```
 */
export function Island({ 
  component: Component, 
  client, 
  id,
  media,
  ...props 
}: IslandProps) {
  const islandId = `island-${islandCounter++}`;
  const componentId = resolveComponentId(Component, id);
  
  // Client: register component for hydration
  if (!isServer) {
    window.__ISLAND_COMPONENTS__ = window.__ISLAND_COMPONENTS__ || {};
    const existing = window.__ISLAND_COMPONENTS__[componentId];
    if (existing && existing !== Component) {
      console.warn(
        `[Neutron] Island id collision "${componentId}". Pass a unique id prop to <Island ... />.`
      );
    }
    window.__ISLAND_COMPONENTS__[componentId] = Component;
  }
  
  // Render island marker + content
  return h(
    "neutron-island",
    {
      "data-island-id": islandId,
      "data-component": componentId,
      "data-client": client,
      "data-props": safeSerializeProps(props),
      ...(media ? { "data-media": media } : {}),
    },
    h(Component, props)
  );
}

function resolveComponentId(
  component: preact.FunctionComponent<any>,
  explicitId?: string
): string {
  if (explicitId) {
    return explicitId;
  }

  const existing = componentIdMap.get(component);
  if (existing) {
    return existing;
  }

  const baseName = (component as any).displayName || (component as any).name || "Component";
  const nextIndex = (componentNameCounts.get(baseName) || 0) + 1;
  componentNameCounts.set(baseName, nextIndex);
  const generatedId = nextIndex === 1 ? baseName : `${baseName}#${nextIndex}`;
  componentIdMap.set(component, generatedId);
  return generatedId;
}

function safeSerializeProps(props: Record<string, unknown>): string {
  try {
    return JSON.stringify(props);
  } catch {
    return "{}";
  }
}

export type { ClientDirective };
export type { IslandProps };

// Global registry
declare global {
  interface Window {
    __ISLAND_COMPONENTS__?: Record<string, preact.FunctionComponent<any>>;
  }
}
