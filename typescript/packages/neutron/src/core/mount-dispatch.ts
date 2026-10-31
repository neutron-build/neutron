/**
 * Mounted sub-apps for the generated adapter runtime (vercel, netlify,
 * cloudflare, docker), with `neutron-ts start`'s semantics: a request under a
 * mount's prefix goes to its Hono app with the global middleware outermost
 * (unless the mount opts out); a path the sub-app does not match falls
 * through to the routes, with the chain already run. Hono apps are
 * fetch-compatible, so this needs nothing of Node.
 */
import { Hono } from "hono";
import { runMiddlewareChain } from "./middleware.js";
import { isResponse } from "./response.js";
import { isProblemError } from "./problem.js";
import type { MiddlewareFn } from "./types.js";

type MountApp = { fetch: (request: Request, ...rest: unknown[]) => Response | Promise<Response> };
type MountEntry = MountApp | { app: MountApp; globalMiddleware?: boolean };

export interface CompiledMount {
  prefix: string;
  globalMiddleware: boolean;
  /** The sub-app's answer, or null when it has no route for the request. */
  dispatch: (request: Request) => Promise<Response | null>;
}

const UNMATCHED = "x-neutron-mount-unmatched";

function prefixCovers(prefix: string, pathname: string): boolean {
  if (prefix === "/" || prefix === pathname) return true;
  return pathname.startsWith(prefix + "/");
}

export function compileMounts(mounts: Record<string, MountEntry> | undefined): CompiledMount[] {
  if (!mounts) return [];
  const token = crypto.randomUUID();
  return Object.entries(mounts).map(([rawPrefix, value]) => {
    if (!rawPrefix.startsWith("/")) {
      throw new TypeError(`Mount prefix "${rawPrefix}" must start with "/".`);
    }
    const prefix = rawPrefix.replace(/\/+$/, "") || "/";
    const entry =
      typeof (value as MountApp).fetch === "function"
        ? { app: value as MountApp, globalMiddleware: true }
        : (value as { app: MountApp; globalMiddleware?: boolean });
    if (!entry?.app || typeof entry.app.fetch !== "function") {
      throw new TypeError(`Mount "${rawPrefix}" is not a Hono app (or { app, globalMiddleware }).`);
    }
    const dispatcher = new Hono();
    dispatcher.route(prefix, entry.app as unknown as Hono);
    dispatcher.notFound(() => new Response(null, { status: 404, headers: { [UNMATCHED]: token } }));
    return {
      prefix,
      globalMiddleware: entry.globalMiddleware !== false,
      dispatch: async (request) => {
        const response = await dispatcher.fetch(request);
        return response.headers.get(UNMATCHED) === token ? null : response;
      },
    };
  });
}

/**
 * Answer `request` from the first mount covering its path, or with
 * `fallThrough(globalChainRan)` — the routes — when none does or the sub-app
 * has no matching route.
 */
export async function dispatchMounts(
  mounts: CompiledMount[],
  request: Request,
  pathname: string,
  globalMiddleware: MiddlewareFn[],
  fallThrough: (globalChainRan: boolean) => Promise<Response>
): Promise<Response> {
  const mount = mounts.find((candidate) => prefixCovers(candidate.prefix, pathname));
  if (!mount) return fallThrough(false);
  const runChain = mount.globalMiddleware && globalMiddleware.length > 0;
  const inner = async () => (await mount.dispatch(request)) ?? fallThrough(runChain);
  if (!runChain) return inner();
  try {
    return await runMiddlewareChain(globalMiddleware, request, {}, inner);
  } catch (error) {
    // The catch-all's treatment of middleware throws (server/index.ts).
    if (isResponse(error)) return error;
    if (isProblemError(error)) return error.toResponse(pathname);
    throw error;
  }
}
