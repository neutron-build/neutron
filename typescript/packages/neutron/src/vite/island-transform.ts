import * as fs from "node:fs";
import * as path from "node:path";
import type { Plugin, ResolvedConfig } from "vite";

interface IslandInfo {
  id: string;
  importPath: string;
  clientDirective: string;
  mediaQuery?: string;
  props: Record<string, unknown>;
}

interface TransformResult {
  code: string;
  islands: IslandInfo[];
}

export function islandTransform(
  code: string,
  id: string,
  config: ResolvedConfig
): TransformResult {
  const islands: IslandInfo[] = [];
  
  // Match JSX elements with client: directives
  // e.g., <Counter client:load count={5} />
  // e.g., <Comments client:visible postId="123" />
  const clientDirectivePattern = /<(\w+)([^>]*?)\s+(client:(load|visible|idle|only|media))(?:=['"]([^'"]*)['"])?([^>]*?)\/?>/g;
  
  let transformedCode = code;
  let match;
  
  while ((match = clientDirectivePattern.exec(code)) !== null) {
    const [fullMatch, componentName, beforeDirective, directive, directiveType, mediaQuery, afterDirective] = match;
    
    const islandId = `island-${componentName.toLowerCase()}-${islands.length}`;
    const clientDirective = directiveType;
    
    // Extract props from attributes
    const allAttrs = (beforeDirective + " " + afterDirective).trim();
    const props = extractProps(allAttrs);
    
    // Get import path for the component
    const importPath = resolveComponentImport(code, componentName, id, config);
    
    islands.push({
      id: islandId,
      importPath,
      clientDirective,
      mediaQuery,
      props,
    });
  }
  
  return { code: transformedCode, islands };
}

function extractProps(attrString: string): Record<string, unknown> {
  const props: Record<string, unknown> = {};
  
  // Match prop="value" or prop={value}
  const propPattern = /(\w+)=['"]([^'"]*)['"]/g;
  let match;
  
  while ((match = propPattern.exec(attrString)) !== null) {
    const [, name, value] = match;
    if (!name.startsWith("client")) {
      props[name] = value;
    }
  }
  
  // Match JSX expressions prop={value}
  const jsxPattern = /(\w+)=\{([^}]*)\}/g;
  while ((match = jsxPattern.exec(attrString)) !== null) {
    const [, name, value] = match;
    if (!name.startsWith("client")) {
      props[name] = parseJsxPropExpression(value);
    }
  }
  
  return props;
}

function parseJsxPropExpression(expression: string): unknown {
  const value = expression.trim();
  if (!value) {
    return "";
  }

  if (value === "true") {
    return true;
  }
  if (value === "false") {
    return false;
  }
  if (value === "null") {
    return null;
  }

  if (/^-?\d+(\.\d+)?$/.test(value)) {
    return Number(value);
  }

  if (
    (value.startsWith("\"") && value.endsWith("\"")) ||
    (value.startsWith("'") && value.endsWith("'"))
  ) {
    return value.slice(1, -1);
  }

  if (
    (value.startsWith("{") && value.endsWith("}")) ||
    (value.startsWith("[") && value.endsWith("]"))
  ) {
    try {
      return JSON.parse(value);
    } catch {
      // Fall through to string fallback when expression is not strict JSON.
    }
  }

  return value;
}

function resolveComponentImport(
  code: string,
  componentName: string,
  currentFile: string,
  config: ResolvedConfig
): string {
  // Look for import statement
  const importPattern = new RegExp(
    `import\\s+(?:\\{[^}]*\\}|\\w+)\\s+from\\s+['"]([^'"]+)['"]`
  );
  
  // Try to find import with component name
  const namedImportPattern = new RegExp(
    `import\\s+[^'"]*\\b${componentName}\\b[^'"]*from\\s+['"]([^'"]+)['"]`
  );
  
  const match = code.match(namedImportPattern);
  if (match) {
    return match[1];
  }
  
  // Default import
  const defaultImportPattern = new RegExp(
    `import\\s+${componentName}\\s+from\\s+['"]([^'"]+)['"]`
  );
  const defaultMatch = code.match(defaultImportPattern);
  if (defaultMatch) {
    return defaultMatch[1];
  }
  
  // Assume local component in same directory
  const dir = path.dirname(currentFile);
  return path.join(dir, componentName);
}

export function generateIslandMarker(island: IslandInfo): string {
  const mediaAttr = island.mediaQuery 
    ? ` data-media="${escapeAttr(island.mediaQuery)}"` 
    : "";
  
  return `<neutron-island
  data-island-id="${island.id}"
  data-client="${island.clientDirective}"${mediaAttr}
  data-import="${island.importPath}"
  data-props="${escapeAttr(JSON.stringify(island.props))}"
></neutron-island>`;
}

export function generateIslandRuntime(islands: IslandInfo[]): string {
  return `
// Neutron Island Runtime (~1KB)
(function() {
  const islands = document.querySelectorAll('neutron-island');

  // SECURITY: Validate against prototype pollution (recursive)
  function hasPrototypePollution(obj, visited) {
    if (!obj || typeof obj !== 'object') return false;

    // Prevent infinite recursion on circular references
    visited = visited || new WeakSet();
    if (visited.has(obj)) return false;
    visited.add(obj);

    // Check current level
    if (obj.hasOwnProperty('__proto__') ||
        obj.hasOwnProperty('constructor') ||
        obj.hasOwnProperty('prototype')) {
      return true;
    }

    // Recursively check nested objects and arrays
    for (var key in obj) {
      if (obj.hasOwnProperty(key)) {
        var value = obj[key];
        if (value && typeof value === 'object') {
          if (hasPrototypePollution(value, visited)) {
            return true;
          }
        }
      }
    }

    return false;
  }

  function safeParseJSON(json) {
    try {
      const obj = JSON.parse(json);
      if (hasPrototypePollution(obj)) {
        console.error('[neutron] Blocked potentially malicious island props');
        return {};
      }
      return obj;
    } catch (err) {
      console.error('[neutron] Failed to parse island props:', err);
      return {};
    }
  }

  async function hydrateIsland(island) {
    const importUrl = island.getAttribute('data-import');
    const propsJson = island.getAttribute('data-props');
    const props = propsJson ? safeParseJSON(propsJson) : {};

    try {
      const module = await import(importUrl);
      const Component = module.default;

      if (Component) {
        const { h, hydrate } = await import('preact');
        const element = h(Component, props);
        hydrate(element, island);
      }
    } catch (err) {
      console.error('Failed to hydrate island:', island.id, err);
    }
  }
  
  islands.forEach(island => {
    const client = island.getAttribute('data-client');
    const media = island.getAttribute('data-media');
    
    switch (client) {
      case 'load':
        hydrateIsland(island);
        break;
        
      case 'visible':
        const observer = new IntersectionObserver(([entry]) => {
          if (entry.isIntersecting) {
            hydrateIsland(island);
            observer.disconnect();
          }
        }, { threshold: 0.1 });
        observer.observe(island);
        break;
        
      case 'idle':
        if ('requestIdleCallback' in window) {
          requestIdleCallback(() => hydrateIsland(island));
        } else {
          setTimeout(() => hydrateIsland(island), 200);
        }
        break;
        
      case 'media':
        if (!media) {
          hydrateIsland(island);
          return;
        }
        const mql = matchMedia(media);
        if (mql.matches) {
          hydrateIsland(island);
        } else {
          mql.addEventListener('change', () => {
            if (mql.matches) hydrateIsland(island);
          }, { once: true });
        }
        break;
        
      case 'only':
        // Client-only: render fresh (no SSR HTML to hydrate)
        hydrateIsland(island);
        break;
    }
  });
})();
`;
}

function escapeAttr(str: string): string {
  return str
    .replace(/&/g, "&amp;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}
