import { describe, it, expect } from "vitest";
import { h } from "preact";
import { renderToString } from "preact-render-to-string";

import { withRouterProviders, type CreateElement } from "./router-providers.js";
import { RouterContext } from "../client/contexts.js";
import {
  useLocation,
  useParams,
  useSearchParams,
  useLoaderData,
  useActionData,
  useNavigation,
  useNavigate,
  useSubmit,
  useRevalidator,
  useMatches,
} from "../client/hooks.js";
import { renderAppRoute } from "./render-app-route.js";
import type { Route, RouteMatch, RouteModule } from "./types.js";
import type { NeutronLoaderCacheStore } from "../server/cache-store.js";

/**
 * A-011. `useLocation` read the RouterContext default during SSR — `pathname:
 * "/"` on every route, silently — because only the client hydrate path mounted
 * a provider. The failure mode that makes this expensive is that it RENDERS: a
 * layout branching on pathname emits the home-route branch server-side and the
 * correct one after hydration, so the page visibly changes on load and the
 * server HTML is wrong for search engines and for JS-off readers.
 *
 * These assert the hooks are isomorphic through the real render paths, not
 * that a provider exists somewhere.
 */

function Probe() {
  const { pathname, search } = useLocation();
  const params = useParams();
  const [searchParams] = useSearchParams();
  return h(
    "div",
    null,
    h("span", { id: "pathname" }, pathname),
    h("span", { id: "search" }, search),
    h("span", { id: "slug" }, params.slug ?? "(none)"),
    h("span", { id: "q" }, searchParams.get("q") ?? "(none)")
  );
}

describe("withRouterProviders", () => {
  it("makes the router hooks report the real request, not the context default", () => {
    const html = renderToString(
      withRouterProviders(h as CreateElement, h(Probe, null), {
        routeId: "posts/[slug]",
        pathname: "/posts/hello",
        search: "?q=neutron",
        params: { slug: "hello" },
        loaderData: {},
        actionData: undefined,
      })
    );

    expect(html).toContain('<span id="pathname">/posts/hello</span>');
    expect(html).toContain('<span id="search">?q=neutron</span>');
    expect(html).toContain('<span id="slug">hello</span>');
    expect(html).toContain('<span id="q">neutron</span>');
  });

  it("mounts the loader data so useLoaderData works server-side too", () => {
    function LoaderProbe() {
      const data = useLoaderData<{ title: string }>();
      return h("span", { id: "title" }, data?.title ?? "(none)");
    }

    const html = renderToString(
      withRouterProviders(h as CreateElement, h(LoaderProbe, null), {
        routeId: "posts/[slug]",
        pathname: "/posts/hello",
        search: "",
        params: { slug: "hello" },
        loaderData: { "posts/[slug]": { title: "Hello" } },
        actionData: undefined,
      })
    );

    expect(html).toContain('<span id="title">Hello</span>');
  });

  // Pins the reason the provider is required rather than merely nice. If the
  // default is ever changed to something that throws or renders visibly wrong,
  // this fails and the docs in router-providers.ts need updating with it.
  it("without a provider the hooks return a plausible wrong answer, not an error", () => {
    const html = renderToString(h(Probe, null));
    expect(html).toContain('<span id="pathname">/</span>');
    expect(html).toContain('<span id="slug">(none)</span>');
  });

  it("does not add markup of its own", () => {
    const wrapped = renderToString(
      withRouterProviders(h as CreateElement, h("p", null, "body"), {
        routeId: "index",
        pathname: "/",
        search: "",
        params: {},
        loaderData: {},
        actionData: undefined,
      })
    );
    expect(wrapped).toBe("<p>body</p>");
  });
});

describe("renderAppRoute mounts the router contexts", () => {
  const noopLoaderCache: NeutronLoaderCacheStore = {
    async get() {
      return null;
    },
    async set() {},
    async deleteByPath() {},
    async clear() {},
  };

  async function renderPath(path: string, routeId: string, params: Record<string, string>) {
    const route: Route = {
      id: routeId,
      path: "/posts/:slug",
      file: "routes/posts/[slug].tsx",
      config: {},
    } as Route;

    const match: RouteMatch = { route, params, layouts: [] };
    const modules = new Map<string, RouteModule>([
      [routeId, { default: Probe } as unknown as RouteModule],
    ]);

    const response = await renderAppRoute(
      new Request(`http://localhost${path}`),
      match,
      modules,
      {
        clientEntryScriptSrc: null,
        loaderDataCache: noopLoaderCache,
        requestTrace: { requestId: "test", method: "GET", pathname: path },
      }
    );

    return response.text();
  }

  // The regression proper: this is the assertion that fails on the pre-fix
  // renderer, on the exact shape that produced the wrong nav in the dogfood.
  it("serves the requested pathname, not /", async () => {
    const html = await renderPath("/posts/hello?q=neutron", "posts/[slug]", { slug: "hello" });

    expect(html).toContain('<span id="pathname">/posts/hello</span>');
    expect(html).not.toContain('<span id="pathname">/</span>');
  });

  it("serves the request's search string and route params", async () => {
    const html = await renderPath("/posts/hello?q=neutron", "posts/[slug]", { slug: "hello" });

    expect(html).toContain('<span id="search">?q=neutron</span>');
    expect(html).toContain('<span id="q">neutron</span>');
    expect(html).toContain('<span id="slug">hello</span>');
  });
});

describe("the router hooks are all safe to call during SSR (A-021)", () => {
  // useNavigation seeded useState from readNavigationState(), which reads
  // `window` with no guard — and a useState initializer runs on the server, so
  // the hook threw `window is not defined` and could not appear in any
  // server-rendered component. A pending-state spinner, its most ordinary use,
  // had to be pushed into an island.
  it("useNavigation renders instead of throwing, and reports idle", () => {
    function NavProbe() {
      const nav = useNavigation();
      return h("span", { id: "nav" }, nav.state);
    }

    const html = renderToString(
      withRouterProviders(h as CreateElement, h(NavProbe, null), {
        routeId: "index",
        pathname: "/",
        search: "",
        params: {},
        loaderData: {},
        actionData: undefined,
      })
    );

    expect(html).toContain('<span id="nav">idle</span>');
  });

  it("useNavigation is idle without a provider too, rather than throwing", () => {
    function NavProbe() {
      const nav = useNavigation();
      return h("span", { id: "nav" }, nav.state);
    }
    expect(renderToString(h(NavProbe, null))).toContain('<span id="nav">idle</span>');
  });

  // The whole surface, so a new hook that reads `window` at render time is
  // caught here rather than by an adopter. Effects are not a concern: they do
  // not run during SSR.
  it("every exported router hook renders on the server", () => {
    const calls: Array<[string, () => unknown]> = [
      ["useLoaderData", () => useLoaderData()],
      ["useActionData", () => useActionData()],
      ["useNavigation", () => useNavigation()],
      ["useNavigate", () => useNavigate()],
      ["useSubmit", () => useSubmit()],
      ["useParams", () => useParams()],
      ["useLocation", () => useLocation()],
      ["useSearchParams", () => useSearchParams()],
      ["useRevalidator", () => useRevalidator()],
      ["useMatches", () => useMatches()],
    ];

    const threw: string[] = [];
    for (const [name, call] of calls) {
      function Probe() {
        call();
        return h("i", null, name);
      }
      try {
        renderToString(h(Probe, null));
      } catch (error) {
        threw.push(`${name}: ${String(error)}`);
      }
    }

    expect(threw).toEqual([]);
  });
});

describe("RouterContext", () => {
  // Two createContext calls produce two unrelated contexts, so a provider
  // mounted from one module and a hook reading from another never match. The
  // contexts moved to their own module so the server could import them without
  // the navigation machinery; this pins that hooks.ts still re-exports the same
  // objects rather than making new ones.
  it("is the same object the client hooks module exposes", async () => {
    const clientIndex = await import("../client/hooks.js");
    expect(clientIndex.RouterContext).toBe(RouterContext);
  });
});
