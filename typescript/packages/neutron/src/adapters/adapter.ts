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
  /** Server actions the build registered (`"use server"` exports client code reaches). */
  serverActions?: number;
  ensureRuntimeBundle?: (
    target: AdapterRuntimeBundle["target"],
    options?: AdapterRuntimeBundleOptions
  ) => Promise<AdapterRuntimeBundle>;
}

export interface NeutronAdapter {
  name: string;
  adapt(context: AdapterBuildContext): Promise<void> | void;
}

/**
 * Refuse a build whose server actions the adapter's output could not serve:
 * a static target has no server, and the runtime bundle the others deploy
 * is built only for apps with app routes. Silently shipping stubs that 404
 * would look like a working deploy.
 */
export function assertServerActionsServable(adapter: string, context: AdapterBuildContext, canServe: boolean): void {
  const count = context.serverActions ?? 0;
  if (count === 0 || canServe) return;
  throw new Error(
    `The ${adapter} adapter cannot serve the ${count} server action${count === 1 ? "" : "s"} this build ` +
      "registered: " +
      (adapter === "static"
        ? "a static deploy has no server. Use a server preset (node, docker, vercel, netlify, cloudflare)."
        : "its server runtime is built only for apps with at least one app route (config mode \"app\").")
  );
}
