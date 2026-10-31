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
  /** `src/server.entry.*`, when the app has one: its mounts must be served by the target. */
  serverEntry?: { file: string; declaresWebSocket: boolean } | null;
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
 * Refuse a build whose server features the adapter's output could not serve:
 * a static target has no server at all, the runtime bundle the others deploy
 * is built only for apps with app routes, and none of them can hold a
 * WebSocket. Silently shipping stubs that 404, or an entry whose mounts or
 * socket never answer, would look like a working deploy.
 */
export function assertServerFeaturesServable(adapter: string, context: AdapterBuildContext, hasServer: boolean): void {
  const actions = context.serverActions ?? 0;
  const entry = context.serverEntry ?? null;
  const unservable: string[] = [];
  if (!hasServer) {
    if (actions > 0) unservable.push(`${actions} server action${actions === 1 ? "" : "s"}`);
    if (entry) unservable.push(`the mounts of ${entry.file}`);
  }
  if (entry?.declaresWebSocket) unservable.push(`the websocket ${entry.file} declares`);
  if (unservable.length === 0) return;
  const why = !hasServer
    ? adapter === "static"
      ? "a static deploy has no server; use a server preset (node, docker, vercel, netlify, cloudflare)"
      : 'its server runtime is built only for apps with at least one app route (config mode "app")'
    : "this target has no WebSocket upgrade; serve the app with `neutron-ts start` (node preset) instead";
  throw new Error(`The ${adapter} adapter cannot serve ${unservable.join(" or ")}: ${why}.`);
}
