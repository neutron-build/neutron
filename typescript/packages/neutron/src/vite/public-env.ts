import * as path from "node:path";
import type { Plugin } from "vite";
import type { NeutronEnvConfig } from "../config.js";

export const DEFAULT_PUBLIC_ENV_PREFIXES = ["PUBLIC_"];

const JS_ID = /\.(?:[cm]?[jt]sx?)$/;
const PROCESS_ENV_KEY = /(?<![\w$.])process\.env\.([A-Za-z_$][\w$]*)/g;

export function resolvePublicEnvPrefixes(env?: NeutronEnvConfig): string[] {
  const prefixes = env?.publicPrefixes ?? DEFAULT_PUBLIC_ENV_PREFIXES;
  for (const prefix of prefixes) {
    // An empty prefix would match every variable, secrets included.
    if (typeof prefix !== "string" || prefix.length === 0) {
      throw new Error("[neutron] env.publicPrefixes entries must be non-empty strings.");
    }
  }
  return prefixes;
}

/**
 * Public environment variables (`env.publicPrefixes` in neutron.config) for
 * client and server code. The prefixes join Vite's `envPrefix`, so
 * `import.meta.env.<PREFIX>X` is exposed by Vite itself (from `.env` files and
 * the process environment). `process.env.<PREFIX>X`, the spelling Next apps
 * use, is replaced with the build-time value in every environment: through
 * `define` in builds, and by the transform below in dev. In client code a
 * build also turns the rest of `process.env` into `{}` and inlines
 * `NODE_ENV`; dev client code gets the same replacement, so it reads what the
 * build reads instead of throwing `process is not defined`. On the server an
 * unset public variable stays a runtime read. Variables without a public
 * prefix are never replaced with a value.
 */
export function neutronPublicEnvPlugin(env?: NeutronEnvConfig): Plugin {
  const prefixes = resolvePublicEnvPrefixes(env);
  const isPublic = (key: string) => prefixes.some((prefix) => key.startsWith(prefix));
  let values: Record<string, string> = {};
  let nodeEnv = "development";

  return {
    name: "neutron:public-env",

    async config(userConfig, { command, mode }) {
      // Loaded lazily: under react-compat, core runs inside the SSR graph and
      // createServer imports this module, so a top-level `vite` import would
      // be pulled into that graph.
      const { loadEnv } = await import("vite");
      const root = path.resolve(userConfig.root ?? process.cwd());
      const envDir = userConfig.envDir === false ? false : path.resolve(root, userConfig.envDir ?? "");
      values = loadEnv(mode, envDir, prefixes);
      // What Vite's client build inlines for process.env.NODE_ENV.
      nodeEnv = process.env.NODE_ENV || mode;
      const define: Record<string, string> = {};
      if (command === "build") {
        for (const [key, value] of Object.entries(values)) {
          define[`process.env.${key}`] = JSON.stringify(value);
        }
      }
      return {
        // Returned arrays are concatenated onto the user's envPrefix; with
        // none set, Vite's default VITE_ must be kept explicitly.
        envPrefix: userConfig.envPrefix === undefined ? ["VITE_", ...prefixes] : prefixes,
        // Not in dev: there Vite would publish client defines as a
        // `globalThis.process` object instead of replacing them.
        define,
      };
    },

    async transform(code, id) {
      // Bundled builds are covered by `define`. Unbundled (dev) code reaches
      // this plugin as JavaScript, after Vite's own TS/JSX transform.
      if (this.environment?.config.isBundled !== false) return null;
      if (id.startsWith("\0") || !code.includes("process.env")) return null;
      const file = id.split("?", 1)[0];
      if (!JS_ID.test(file)) return null;
      const client = this.environment.config.consumer === "client";
      // Client: the build's replacement (process.env -> {}, NODE_ENV inlined).
      const define: Record<string, string> = client
        ? { "process.env": "{}", "process.env.NODE_ENV": JSON.stringify(nodeEnv) }
        : {};
      for (const [, key] of code.matchAll(PROCESS_ENV_KEY)) {
        if (isPublic(key) && key in values) define[`process.env.${key}`] = JSON.stringify(values[key]);
      }
      if (Object.keys(define).length === 0) return null;
      const { transformWithOxc } = await import("vite");
      const result = await transformWithOxc(code, file, { lang: "js", define, sourcemap: true });
      return { code: result.code, map: result.map };
    },
  };
}
