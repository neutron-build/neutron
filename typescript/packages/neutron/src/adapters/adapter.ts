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

export interface AdapterRuntimeBundleOptions {
  /**
   * Ship what the bundle imports at runtime inside the output, for a deploy
   * target whose only input is `outDir` (the docker image context). Core's
   * own dependencies stay external, as in the `start` bundle, and every
   * external package the bundle imports is copied, with its production
   * dependencies, into `outDir/node_modules` as real directories.
   */
  selfContained?: boolean;
}

export interface AdapterBuildContext {
  rootDir: string;
  outDir: string;
  routes: AdapterRoutesSummary;
  log: (message: string) => void;
  clientEntryScriptSrc?: string | null;
  ensureRuntimeBundle?: (
    target: AdapterRuntimeBundle["target"],
    options?: AdapterRuntimeBundleOptions
  ) => Promise<AdapterRuntimeBundle>;
}

export interface NeutronAdapter {
  name: string;
  adapt(context: AdapterBuildContext): Promise<void> | void;
}
