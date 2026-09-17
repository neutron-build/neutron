import { describe, it, expect } from "vitest";
import { renderAppRoute } from "./render-app-route.js";
import type { Route, RouteMatch, RouteModule, LoaderArgs, MiddlewareFn } from "./types.js";
import { createMemoryLoaderCacheStore } from "../server/cache-store.js";

/**
 * Regression tests for the render-core audit fixes (TS-02, TS-18, TS-25,
 * TS-26, TS-27): direct renderAppRoute calls against synthetic route modules.
 */

function makeRoute(overrides: Partial<Route> = {}): Route {
  return {
    id: "route:index.ts",
    path: "/",
    file: "route:index.ts",
    params: [],
    config: { mode: "app" },
    parentId: null,
    ...overrides,
  };
}

function makeMatch(route: Route, layouts: Route[] = [], params: Record<string, string> = {}): RouteMatch {
  return { route, layouts, params };
}

const loaderCache = () => createMemoryLoaderCacheStore();

describe("TS-26: falsy thrown loader values take the error path", () => {
  for (const thrown of [null, undefined, 0, ""]) {
    it(`throw ${JSON.stringify(thrown)} renders a 500, not a success document`, async () => {
      const route = makeRoute();
      const modules = new Map<string, RouteModule>([
        [
          route.id,
          {
            default: () => null,
            // eslint-disable-next-line @typescript-eslint/only-throw-error
            loader: async () => {
              throw thrown;
            },
          } as RouteModule,
        ],
      ]);
      const response = await renderAppRoute(
        new Request("http://localhost/"),
        makeMatch(route),
        modules,
        {
          clientEntryScriptSrc: null,
          loaderDataCache: loaderCache(),
          requestTrace: { requestId: "r", method: "GET", pathname: "/" },
        }
      );
      expect(response.status).toBe(500);
    });
  }
});

describe("TS-27: method dispatch", () => {
  it("answers 405 with Allow for OPTIONS on a resource route", async () => {
    const route = makeRoute();
    const modules = new Map<string, RouteModule>([
      [route.id, { loader: async () => new Response("data") } as RouteModule],
    ]);
    const response = await renderAppRoute(
      new Request("http://localhost/", { method: "OPTIONS" }),
      makeMatch(route),
      modules,
      {
        clientEntryScriptSrc: null,
        loaderDataCache: loaderCache(),
        requestTrace: { requestId: "r", method: "OPTIONS", pathname: "/" },
      }
    );
    expect(response.status).toBe(405);
    expect(response.headers.get("Allow")).toBe("GET, HEAD");
  });

  it("answers 405 for POST to a component route with no action", async () => {
    const route = makeRoute();
    const modules = new Map<string, RouteModule>([
      [route.id, { default: () => null, loader: async () => ({}) } as RouteModule],
    ]);
    const response = await renderAppRoute(
      new Request("http://localhost/", { method: "POST" }),
      makeMatch(route),
      modules,
      {
        clientEntryScriptSrc: null,
        loaderDataCache: loaderCache(),
        requestTrace: { requestId: "r", method: "POST", pathname: "/" },
      }
    );
    expect(response.status).toBe(405);
    expect(response.headers.get("Allow")).toBe("GET, HEAD");
  });
});

describe("TS-25: layout loaders receive matched params", () => {
  it("passes the full match params to a parent layout loader", async () => {
    const layout = makeRoute({
      id: "route:orgs/_layout.ts",
      path: "/orgs",
      file: "route:orgs/_layout.ts",
      isLayout: true,
    });
    const leaf = makeRoute({
      id: "route:orgs/projects.ts",
      path: "/orgs/projects",
      file: "route:orgs/projects.ts",
    });
    let layoutParams: Record<string, string> | undefined;
    const modules = new Map<string, RouteModule>([
      [
        layout.id,
        {
          default: ({ children }: { children: unknown }) => children as never,
          loader: async ({ params }: LoaderArgs) => {
            layoutParams = params as Record<string, string>;
            return {};
          },
        } as unknown as RouteModule,
      ],
      [leaf.id, { default: () => null, loader: async () => ({}) } as RouteModule],
    ]);
    const response = await renderAppRoute(
      new Request("http://localhost/orgs/projects"),
      makeMatch(leaf, [layout], { orgId: "acme" }),
      modules,
      {
        clientEntryScriptSrc: null,
        loaderDataCache: loaderCache(),
        requestTrace: { requestId: "r", method: "GET", pathname: "/orgs/projects" },
      }
    );
    expect(response.status).toBe(200);
    expect(layoutParams).toEqual({ orgId: "acme" });
  });
});

describe("TS-02: the shared cache boundary runs inside the middleware chain", () => {
  it("a cache hit still executes route middleware (a denial wins)", async () => {
    const route = makeRoute();
    const modules = new Map<string, RouteModule>([
      [
        route.id,
        {
          default: () => null,
          loader: async () => ({ n: 1 }),
          middleware: [
            async (_request: Request, _context: unknown, next: () => Promise<Response>) => {
              const res = await next();
              return res;
            },
          ],
        } as unknown as RouteModule,
      ],
    ]);
    let gateRuns = 0;
    const globalMiddleware = [
      async () => {
        gateRuns += 1;
        return new Response("denied", { status: 403 });
      },
    ];
    let reads = 0;
    const response = await renderAppRoute(
      new Request("http://localhost/"),
      makeMatch(route),
      modules,
      {
        clientEntryScriptSrc: null,
        loaderDataCache: loaderCache(),
        requestTrace: { requestId: "r", method: "GET", pathname: "/" },
        globalMiddleware,
        responseCache: {
          enabled: true,
          read: async () => {
            reads += 1;
            return new Response("cached-page", { status: 200 });
          },
          store: () => {},
        },
      }
    );
    // The gate ran BEFORE the cache was even consulted — its denial wins,
    // and a denial short-circuits before the cache read entirely.
    expect(gateRuns).toBe(1);
    expect(reads).toBe(0);
    expect(response.status).toBe(403);
    expect(await response.text()).toBe("denied");
  });

  it("stores the rendered response on a miss", async () => {
    const route = makeRoute();
    const modules = new Map<string, RouteModule>([
      [route.id, { default: () => null, loader: async () => ({}) } as RouteModule],
    ]);
    let stored: Response | null = null;
    const response = await renderAppRoute(
      new Request("http://localhost/"),
      makeMatch(route),
      modules,
      {
        clientEntryScriptSrc: null,
        loaderDataCache: loaderCache(),
        requestTrace: { requestId: "r", method: "GET", pathname: "/" },
        responseCache: {
          enabled: true,
          read: async () => null,
          store: (r) => {
            stored = r;
          },
        },
      }
    );
    expect(response.status).toBe(200);
    expect(stored).toBe(response);
  });
});

describe("TS-18: resolveRouteHeaders keeps every Set-Cookie", () => {
  it("a route headers() returning two cookies keeps both", async () => {
    const route = makeRoute();
    const modules = new Map<string, RouteModule>([
      [
        route.id,
        {
          default: () => null,
          loader: async () => ({ ok: true }),
          headers: async () => {
            const headers = new Headers();
            headers.append("Set-Cookie", "a=1");
            headers.append("Set-Cookie", "b=2");
            return headers;
          },
        } as unknown as RouteModule,
      ],
    ]);
    const response = await renderAppRoute(
      new Request("http://localhost/", { headers: { accept: "application/json" } }),
      makeMatch(route),
      modules,
      {
        clientEntryScriptSrc: null,
        loaderDataCache: loaderCache(),
        requestTrace: { requestId: "r", method: "GET", pathname: "/" },
      }
    );
    const cookies = response.headers.getSetCookie();
    expect(cookies).toEqual(["a=1", "b=2"]);
  });
});
