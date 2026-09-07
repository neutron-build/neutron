/**
 * Config the CLI composes for every Vite pass, in one place so `dev` and
 * `build` cannot drift apart.
 *
 * The rule both helpers encode: a fact is declared once and derived
 * everywhere else. `runtime` is declared in neutron.config.ts, so the JSX
 * settings come from it rather than from each project's vite.config.ts.
 */
import type { NeutronRuntime } from "@neutron-build/core";
import { resolveRuntimeJsx } from "@neutron-build/core";

/**
 * Plugins the CLI instantiates itself, fully configured. A project copy is
 * necessarily unconfigured (it has no routesDir, rootDir or routeRules), and
 * Vite's mergeConfig concatenates plugin arrays rather than replacing them,
 * so without this filter both instances run: route discovery and island
 * scanning happen twice per pass, and the static-interactivity warning prints
 * twice. `dev` has always filtered; `build` did not, which is how the two
 * commands came to behave differently on the same project.
 */
const CLI_OWNED_PLUGINS = new Set(["neutron:core"]);

export function stripCliOwnedPlugins(plugins: unknown): unknown[] {
  if (!Array.isArray(plugins)) return [];
  return plugins.filter((plugin) => {
    if (plugin && typeof plugin === "object" && "name" in plugin) {
      return !CLI_OWNED_PLUGINS.has((plugin as { name: string }).name);
    }
    return true;
  });
}

/**
 * The esbuild half of the runtime. Returned separately from aliases because
 * it applies to every pass — SSR render, client bundle and islands alike.
 */
export function runtimeEsbuild(runtime: NeutronRuntime) {
  return resolveRuntimeJsx(runtime);
}

/**
 * Warn once when a project registers `neutronPlugin()` itself.
 *
 * The CLI already injects a fully configured instance. A project copy is
 * unconfigured AND is imported from the project's own @neutron-build/core,
 * which may be a different version than this CLI ships against — so the two
 * can disagree about route paths, and which one answers is incidental. The
 * fix is for the project to delete it.
 */
let warned = false;
export function warnOnDuplicateCorePlugin(plugins: unknown): void {
  if (warned || !Array.isArray(plugins)) return;
  const duplicate = plugins.some(
    (p) => p && typeof p === "object" && "name" in p && (p as { name: string }).name === "neutron:core"
  );
  if (!duplicate) return;
  warned = true;
  console.warn(
    "\n  vite.config: remove neutronPlugin() — the CLI adds its own, already configured.\n" +
      "  Two instances can disagree about routes when your @neutron-build/core differs from the CLI's.\n"
  );
}
