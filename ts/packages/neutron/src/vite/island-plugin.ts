import * as path from "node:path";
import type { Plugin, ResolvedConfig } from "vite";

interface IslandComponent {
  name: string;
  importPath: string;
  chunkFile: string;
}

export function islandPlugin(): Plugin {
  let config: ResolvedConfig;
  const islandComponents = new Map<string, IslandComponent>();
  
  return {
    name: "neutron:islands",
    enforce: "post",
    
    configResolved(resolvedConfig) {
      config = resolvedConfig;
    },
    
    transform(code, id) {
      if (id.includes("node_modules")) return null;
      if (!id.includes("/src/routes/")) return null;
      
      // Find Island component usage
      const islandPattern = /<Island\s+[^>]*component=\{(\w+)\}/g;
      let match;
      
      while ((match = islandPattern.exec(code)) !== null) {
        const componentName = match[1];
        
        // Find where this component is imported from
        const importPattern = new RegExp(
          `import\\s+(?:\\{[^}]*\\b${componentName}\\b[^}]*\\}|${componentName})\\s+from\\s+['"]([^'"]+)['"]`
        );
        const importMatch = code.match(importPattern);
        
        if (importMatch) {
          const importPath = importMatch[1];
          const absolutePath = importPath.startsWith(".")
            ? path.resolve(path.dirname(id), importPath)
            : importPath;
          
          if (!islandComponents.has(componentName)) {
            islandComponents.set(componentName, {
              name: componentName,
              importPath: absolutePath,
              chunkFile: `island-${componentName.toLowerCase()}`,
            });
          }
        }
      }
      
      return null;
    },
    
    generateBundle() {
      // For each island component, emit a separate chunk
      for (const [name, info] of islandComponents) {
        this.emitFile({
          type: "chunk",
          id: info.importPath,
          name: info.chunkFile,
        });
      }
    },
  };
}

declare global {
  interface Window {
    __ISLAND_REGISTRY__?: Record<string, string>;
  }
}
