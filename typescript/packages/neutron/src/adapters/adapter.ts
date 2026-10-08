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
  /** Producer-issued public artifact paths relative to outDir. Only browser
   * bundle output, explicitly public source assets and rendered static routes
   * belong here. Arbitrary output-directory metadata is NOT public. */
  publicArtifacts?: readonly string[];
  clientEntryScriptSrc?: string | null;
  ensureRuntimeBundle?: (
    target: AdapterRuntimeBundle["target"]
  ) => Promise<AdapterRuntimeBundle>;
}

export interface NeutronAdapter {
  name: string;
  adapt(context: AdapterBuildContext): Promise<void> | void;
}
