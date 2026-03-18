export interface AdapterRoutesSummary {
  total: number;
  static: number;
  app: number;
}

export interface AdapterRuntimeBundle {
  target: "node" | "worker";
  outDir: string;
  entryPath: string;
  entryRelativePath: string;
}

export interface AdapterBuildContext {
  rootDir: string;
  outDir: string;
  routes: AdapterRoutesSummary;
  log: (message: string) => void;
  clientEntryScriptSrc?: string | null;
  ensureRuntimeBundle?: (
    target: AdapterRuntimeBundle["target"]
  ) => Promise<AdapterRuntimeBundle>;
}

export interface NeutronAdapter {
  name: string;
  adapt(context: AdapterBuildContext): Promise<void> | void;
}
