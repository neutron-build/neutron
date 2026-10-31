// @vitest-environment happy-dom
import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import type { ComponentChildren, ComponentType, VNode } from "preact";

/**
 * The SPA/app-tier hydration path: `init()` → `hydrateApp()` in hydrate.ts,
 * the document-level click interceptor, and the wire-up between the streamed
 * SSR document (whose data script is emitted in the shell suffix, after the
 * streamed body) and the hydrated tree.
 *
 * The standalone islands path has its own suite (island-runtime.test.ts);
 * only hydrateApp's claiming of router-owned islands is tested here. Every
 * other test exercises the full-tree preact hydrate inside `#app`.
 *
 * hydrate.ts keeps its state in module-level variables (`initialized`,
 * `hydrated`, the route table), so each test boots a fresh module graph with
 * `vi.resetModules()` and dynamic imports. Everything the fresh graph
 * registers on window/document is captured during boot and torn down after
 * the test — a stale click interceptor from an earlier test would otherwise
 * `preventDefault` the clicks meant for the current one, because the handler
 * bails on `defaultPrevented`.
 */

type HydrateModule = typeof import("./hydrate.js");
type HooksModule = typeof import("./hooks.js");
type PreactModule = typeof import("preact");
type PreactHooksModule = typeof import("preact/hooks");

interface PageDeps {
  h: PreactModule["h"];
  hooks: HooksModule;
  preactHooks: PreactHooksModule;
}

interface RouteReg {
  path?: string;
  parentId?: string | null;
  isLayout?: boolean;
  isNotFound?: boolean;
  mode?: "static" | "app";
  hasLoader?: boolean;
  default: ComponentType;
  load?: () => Promise<unknown>;
}

interface BootOptions {
  pathname: string;
  /** Route id of the page being SSR'd — keys into `routes`. */
  routeId: string;
  routes: (deps: PageDeps) => Record<string, RouteReg>;
  /** Initial loader data via `window.__NEUTRON_DATA__` (already-decoded shape). */
  data?: Record<string, unknown>;
  /**
   * Initial loader data via `window.__NEUTRON_DATA_SERIALIZED__` — the
   * channel a streamed document uses (the suffix script assigns it before
   * the client entry module executes).
   */
  serializedData?: string;
  /** Overrides the generated SSR markup inside #app when given. */
  appHtml?: string;
  /** Runs after the DOM is set up but before registerRoutes/init. */
  beforeInit?: () => void;
}

interface Booted {
  hydrate: HydrateModule;
  hooks: HooksModule;
  preact: PreactModule;
  app: HTMLDivElement;
}

interface CapturedListener {
  target: EventTarget;
  type: string;
  listener: EventListenerOrEventListenerObject;
  capture: boolean;
}

let capturedListeners: CapturedListener[] = [];
let restorePatchedAddEventListener: (() => void) | null = null;

/** Flush microtasks + a macrotask so dynamic-import + async renders settle. */
function flush(): Promise<void> {
  return new Promise((r) => setTimeout(r, 0));
}

function jsonResponse(body: unknown, init: Partial<Response> = {}): Response {
  return {
    ok: true,
    redirected: false,
    headers: new Headers({ "content-type": "application/json" }),
    json: async () => body,
    ...init,
  } as Response;
}

function patchListenerRegistration(): void {
  capturedListeners = [];
  const restoreFns: Array<() => void> = [];
  for (const target of [window, document] as Array<EventTarget & {
    addEventListener: typeof window.addEventListener;
  }>) {
    const hadOwn = Object.prototype.hasOwnProperty.call(
      target,
      "addEventListener"
    );
    const previous = target.addEventListener;
    const original = target.addEventListener.bind(target);
    const patched = (
      type: string,
      listener: EventListenerOrEventListenerObject,
      options?: boolean | AddEventListenerOptions
    ): void => {
      const capture =
        typeof options === "boolean" ? options : Boolean(options?.capture);
      capturedListeners.push({ target, type, listener, capture });
      original(type, listener, options);
    };
    target.addEventListener = patched as typeof window.addEventListener;
    restoreFns.push(() => {
      if (hadOwn) {
        target.addEventListener = previous;
      } else {
        delete (target as { addEventListener?: unknown }).addEventListener;
      }
    });
  }
  restorePatchedAddEventListener = () => {
    for (const fn of restoreFns) fn();
    restorePatchedAddEventListener = null;
  };
}

/**
 * Mirrors hydrateApp's element nesting (layouts wrap the page, innermost
 * route gets params/actionData) and its provider stack, so the SSR markup in
 * #app matches what preact's hydrate() expects. Providers and error
 * boundaries render no DOM of their own.
 */
function buildSsrMarkup(
  deps: PageDeps & { renderToString: typeof import("preact-render-to-string")["renderToString"] },
  routes: Record<string, RouteReg>,
  routeId: string,
  loaderData: Record<string, unknown>
): string {
  const { h, renderToString, hooks } = deps;
  const actionData = loaderData.__action__;
  const rest = { ...loaderData };
  delete rest.__action__;

  const chainIds: string[] = [];
  let cursor: string | null = routeId;
  while (cursor) {
    chainIds.unshift(cursor);
    cursor = routes[cursor]?.parentId ?? null;
  }

  let element: VNode | null = null;
  for (let i = chainIds.length - 1; i >= 0; i--) {
    const Component = routes[chainIds[i]].default;
    if (element === null) {
      element = h(Component, {
        data: rest[chainIds[i]],
        params: {},
        actionData,
      } as never);
    } else {
      element = h(Component, { data: rest[chainIds[i]] } as never, element);
    }
  }

  const app = h(
    hooks.RouterContext.Provider,
    {
      value: {
        routeId,
        pathname: window.location.pathname,
        search: window.location.search,
        params: {},
      },
    },
    h(
      hooks.LoaderContext.Provider,
      { value: rest },
      h(
        hooks.ActionDataContext.Provider,
        { value: actionData },
        h(
          hooks.NavigationContext.Provider,
          { value: { state: "idle" } },
          element
        )
      )
    )
  );
  return renderToString(app);
}

async function bootRouter(options: BootOptions): Promise<Booted> {
  window.history.replaceState(null, "", options.pathname);

  document.body.innerHTML = "";
  const app = document.createElement("div");
  app.id = "app";
  document.body.appendChild(app);

  delete window.__NEUTRON_DATA__;
  delete window.__NEUTRON_DATA_SERIALIZED__;
  if (options.data !== undefined) {
    window.__NEUTRON_DATA__ = options.data;
  }
  if (options.serializedData !== undefined) {
    window.__NEUTRON_DATA_SERIALIZED__ = options.serializedData;
  }

  patchListenerRegistration();
  vi.resetModules();

  const preact = await import("preact");
  const preactHooks = await import("preact/hooks");
  const hooks = await import("./hooks.js");
  const { renderToString } = await import("preact-render-to-string");
  const { deserializeTransportData } = await import("../core/serialization.js");
  const hydrate = await import("./hydrate.js");

  const deps: PageDeps & {
    renderToString: typeof renderToString;
  } = { h: preact.h, hooks, preactHooks, renderToString };
  const routes = options.routes(deps);

  const initialData = options.data
    ? options.data
    : options.serializedData !== undefined
      ? deserializeTransportData<Record<string, unknown>>(options.serializedData)
      : {};
  app.innerHTML =
    options.appHtml !== undefined
      ? options.appHtml
      : buildSsrMarkup(deps, routes, options.routeId, initialData);

  options.beforeInit?.();

  hydrate.registerRoutes(routes as Parameters<typeof hydrate.registerRoutes>[0]);
  await hydrate.init();

  return { hydrate, hooks, preact, app };
}

function addAnchor(
  href: string,
  attrs: Record<string, string> = {}
): HTMLAnchorElement {
  const anchor = document.createElement("a");
  anchor.href = href;
  anchor.textContent = "link";
  for (const [key, value] of Object.entries(attrs)) {
    anchor.setAttribute(key, value);
  }
  document.body.appendChild(anchor);
  return anchor;
}

/** true = left to the browser; false = preventDefault was called. */
function clickAnchor(
  anchor: HTMLAnchorElement,
  modifiers: Partial<MouseEventInit> = {}
): boolean {
  return anchor.dispatchEvent(
    new MouseEvent("click", {
      bubbles: true,
      cancelable: true,
      composed: true,
      button: 0,
      ...modifiers,
    })
  );
}

const WINDOW_STATE_KEYS = [
  "__NEUTRON_DATA__",
  "__NEUTRON_DATA_SERIALIZED__",
  "__NEUTRON_ROUTER_ACTIVE__",
  "__NEUTRON_NAVIGATION_STATE__",
  "__NEUTRON_ACTIVE_ROUTE_IDS__",
  "__NEUTRON_PREFETCH_CACHE__",
  "__NEUTRON_ROUTE__",
  "__NEUTRON_VIEW_TRANSITIONS__",
  "__NEUTRON_MATCHES__",
] as const;

describe("hydrate — init/hydrateApp (SPA full-tree path)", () => {
  beforeEach(() => {
    window.history.replaceState(null, "", "/");
  });

  afterEach(() => {
    for (const { target, type, listener, capture } of capturedListeners) {
      target.removeEventListener(type, listener, capture);
    }
    restorePatchedAddEventListener?.();
    document.body.innerHTML = "";
    for (const key of WINDOW_STATE_KEYS) {
      delete (window as unknown as Record<string, unknown>)[key];
    }
    delete (window as unknown as Record<string, unknown>).__ISLAND_COMPONENTS__;
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  // THE acceptance criterion for this file: this is the test that fails if
  // the initial `hydrate(app, appElement)` call is removed from hydrateApp.
  // SSR markup is inert — only a mounted preact root attaches the onClick
  // handler, so the button incrementing proves the hydrate call ran.
  it("mounts an interactive preact root over the SSR markup", async () => {
    let hydratedEvents = 0;
    const onHydrated = (): void => {
      hydratedEvents++;
    };
    // hydrateApp dispatches this on document, not window.
    document.addEventListener("neutron:hydrated", onHydrated);

    const booted = await bootRouter({
      pathname: "/count",
      routeId: "route:/count",
      data: { "route:/count": { greeting: "hello" } },
      routes: ({ h, hooks, preactHooks }) => {
        function Page(): preact.VNode<any> {
          const data = hooks.useLoaderData<{ greeting: string }>();
          const [clicks, setClicks] = preactHooks.useState(0);
          return h(
            "div",
            null,
            h("p", { class: "greeting" }, data?.greeting ?? "none"),
            h(
              "button",
              { class: "count", onClick: () => setClicks(clicks + 1) },
              `clicked:${clicks}`
            )
          );
        }
        return { "route:/count": { path: "/count", mode: "app", default: Page } };
      },
    });

    const button = booted.app.querySelector<HTMLButtonElement>("button.count")!;
    expect(button.textContent).toBe("clicked:0");
    button.click();
    await flush();
    expect(button.textContent).toBe("clicked:1");
    expect(booted.app.querySelector(".greeting")?.textContent).toBe("hello");

    // Wire-ups other client code reads.
    expect(window.__NEUTRON_ROUTER_ACTIVE__).toBe(true);
    expect(window.__NEUTRON_ACTIVE_ROUTE_IDS__).toEqual(["route:/count"]);
    // ScrollReveal (and anything else post-hydration) keys off this event.
    expect(hydratedEvents).toBe(1);
    document.removeEventListener("neutron:hydrated", onHydrated);
  });

  // The streaming wire-up, client half. The server streams shell → body →
  // suffix, and the suffix is where `window.__NEUTRON_DATA_SERIALIZED__` and
  // the client entry live (render-app-route.ts buildHtmlSuffix), so by the
  // time this module runs the serialized payload is the only data channel
  // that has been populated. init() must decode it (devalue — richer than
  // JSON) and hand it to the tree through LoaderContext.
  it("hydrates loader data delivered by the streaming suffix script", async () => {
    const { serializeForInlineScript } = await import("../core/serialization.js");
    // Exactly what buildHtmlSuffix inlines: `window.__NEUTRON_DATA_SERIALIZED__=`
    // + serializeForInlineScript(allData). The browser evaluates that as a
    // JSON string literal, leaving the raw devalue payload on window.
    const inlineScriptValue = serializeForInlineScript({
      "route:/stream": {
        title: "Streamed",
        when: new Date("2026-01-02T03:04:05.000Z"),
      },
    });
    const afterBrowserEval = JSON.parse(inlineScriptValue) as string;

    const booted = await bootRouter({
      pathname: "/stream",
      routeId: "route:/stream",
      serializedData: afterBrowserEval,
      routes: ({ h, hooks, preactHooks }) => {
        function Page(): preact.VNode<any> {
          const data = hooks.useLoaderData<{
            title?: string;
            when?: unknown;
          }>();
          const [revealed, setRevealed] = preactHooks.useState(false);
          return h(
            "div",
            null,
            h("p", { class: "title" }, data?.title ?? "none"),
            h(
              "button",
              { class: "reveal", onClick: () => setRevealed(true) },
              "reveal"
            ),
            revealed
              ? h(
                  "p",
                  { class: "when" },
                  `${data?.when instanceof Date}:${
                    data?.when instanceof Date
                      ? data.when.getUTCFullYear()
                      : "none"
                  }`
                )
              : null
          );
        }
        return { "route:/stream": { path: "/stream", mode: "app", default: Page } };
      },
    });

    expect(booted.app.querySelector(".title")?.textContent).toBe("Streamed");

    // Two things must both be true after the reveal click: the button works
    // (initial hydrate happened) and the value is a real Date (the payload
    // went through devalue decode, not JSON passthrough).
    booted.app.querySelector<HTMLButtonElement>("button.reveal")!.click();
    await flush();
    expect(booted.app.querySelector(".when")?.textContent).toBe("true:2026");

    // readInitialLoaderData caches the decoded payload for later consumers.
    const cached = window.__NEUTRON_DATA__?.["route:/stream"] as {
      when?: unknown;
    };
    expect(cached?.when).toBeInstanceOf(Date);
  });

  // init() registers this listener; useSubmit/useRevalidator/fetchers all
  // funnel their results through neutron:data-updated. Without the listener
  // a successful submit would fetch and then render nothing.
  it("re-renders the tree when neutron:data-updated fires", async () => {
    const booted = await bootRouter({
      pathname: "/count",
      routeId: "route:/count",
      data: { "route:/count": { greeting: "first" } },
      routes: ({ h, hooks }) => {
        function Page(): preact.VNode<any> {
          const data = hooks.useLoaderData<{ greeting: string }>();
          return h("p", { class: "greeting" }, data?.greeting ?? "none");
        }
        return { "route:/count": { path: "/count", mode: "app", default: Page } };
      },
    });

    expect(booted.app.querySelector(".greeting")?.textContent).toBe("first");

    window.dispatchEvent(
      new CustomEvent("neutron:data-updated", {
        detail: { "route:/count": { greeting: "second" } },
      })
    );
    await flush();
    await flush();

    expect(booted.app.querySelector(".greeting")?.textContent).toBe("second");
  });

  it("splits __action__ out of the initial payload before mounting contexts", async () => {
    const booted = await bootRouter({
      pathname: "/form",
      routeId: "route:/form",
      data: {
        "route:/form": { ok: true },
        __action__: { result: "created" },
      },
      routes: ({ h, hooks, preactHooks }) => {
        function Page(): preact.VNode<any> {
          const loaderKeys = preactHooks.useContext(hooks.LoaderContext);
          const action = hooks.useActionData<{ result: string }>();
          return h(
            "div",
            null,
            h("p", { class: "keys" }, Object.keys(loaderKeys).join(",")),
            h("p", { class: "action" }, action?.result ?? "none")
          );
        }
        return { "route:/form": { path: "/form", mode: "app", default: Page } };
      },
    });

    // LoaderContext must not leak the action payload as a pseudo-route.
    expect(booted.app.querySelector(".keys")?.textContent).toBe("route:/form");
    expect(booted.app.querySelector(".action")?.textContent).toBe("created");
  });

  // hydrateApp claims neutron-island markers under #app so initIslands()
  // does not mount a second preact root over them (the "16%%" double-render
  // bug). island-runtime.test.ts pins the runtime honouring the claim; this
  // pins hydrateApp setting it. The island is deliberately unresolvable (no
  // registry, no manifest) so only the claim can set the flag.
  it("claims router-owned islands for the app root before initIslands", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    const booted = await bootRouter({
      pathname: "/islands",
      routeId: "route:/islands",
      data: {},
      routes: ({ h }) => {
        function Page(): preact.VNode<any> {
          return h(
            "neutron-island",
            { "data-island-id": "i1", "data-client": "load", "data-props": "{}" },
            h("button", null, "count:0")
          );
        }
        return {
          "route:/islands": { path: "/islands", mode: "app", default: Page },
        };
      },
    });

    const island = booted.app.querySelector<HTMLElement>("neutron-island")! as HTMLElement & {
      __neutronHydrated?: boolean;
    };
    expect(island.__neutronHydrated).toBe(true);
    expect(errorSpy).not.toHaveBeenCalled();
  });
});

describe("hydrate — document click interceptor", () => {
  beforeEach(() => {
    window.history.replaceState(null, "", "/");
  });

  afterEach(() => {
    for (const { target, type, listener, capture } of capturedListeners) {
      target.removeEventListener(type, listener, capture);
    }
    restorePatchedAddEventListener?.();
    document.body.innerHTML = "";
    for (const key of WINDOW_STATE_KEYS) {
      delete (window as unknown as Record<string, unknown>)[key];
    }
    delete (window as unknown as Record<string, unknown>).__ISLAND_COMPONENTS__;
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  function twoAppRoutes(
    deps: PageDeps
  ): Record<string, RouteReg> {
    const { h, hooks } = deps;
    function Start(): preact.VNode<any> {
      return h("p", { class: "title" }, "Start");
    }
    function Target(): preact.VNode<any> {
      const data = hooks.useLoaderData<{ title: string }>();
      return h("p", { class: "title" }, data?.title ?? "none");
    }
    return {
      "route:/start": { path: "/start", mode: "app", default: Start },
      "route:/target": { path: "/target", mode: "app", default: Target },
    };
  }

  function stubTargetFetch(): ReturnType<typeof vi.fn> {
    const fetchMock = vi.fn(async () =>
      jsonResponse({ "route:/target": { title: "Target" } })
    );
    vi.stubGlobal("fetch", fetchMock);
    return fetchMock;
  }

  async function bootAtStart(
    routes: (deps: PageDeps) => Record<string, RouteReg>,
    beforeInit?: () => void
  ): Promise<Booted> {
    return bootRouter({
      pathname: "/start",
      routeId: "route:/start",
      data: { "route:/start": { title: "Start" } },
      routes,
      beforeInit,
    });
  }

  it("intercepts a same-origin link to an app route and SPA-navigates", async () => {
    const fetchMock = stubTargetFetch();
    let anchor: HTMLAnchorElement | null = null;
    const booted = await bootAtStart(twoAppRoutes, () => {
      anchor = addAnchor("/target");
    });

    expect(clickAnchor(anchor!)).toBe(false); // preventDefault was called
    expect(window.location.pathname).toBe("/target");
    await flush();
    await flush();

    expect(booted.app.querySelector(".title")?.textContent).toBe("Target");
    expect(fetchMock.mock.calls.length).toBeGreaterThanOrEqual(1);
  });

  it("navigates with the data headers a navigation needs", async () => {
    const fetchMock = stubTargetFetch();
    const booted = await bootAtStart(twoAppRoutes);

    // Added after init so link prefetching cannot answer the navigation
    // instead of the fetch under assertion.
    const anchor = addAnchor("/target");
    clickAnchor(anchor);
    await flush();
    await flush();

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, init] = fetchMock.mock.calls[0] as unknown as [
      string,
      RequestInit,
    ];
    expect(url).toBe("/target");
    const headers = init.headers as Record<string, string>;
    expect(headers["X-Neutron-Data"]).toBe("true");
    expect(headers["X-Neutron-Routes"]).toBe("route:/target");
    expect(booted.app.querySelector(".title")?.textContent).toBe("Target");
  });

  it("keeps the hash on a cross-page anchor link", async () => {
    stubTargetFetch();
    let anchor: HTMLAnchorElement | null = null;
    const booted = await bootAtStart(twoAppRoutes, () => {
      anchor = addAnchor("/target#section");
    });

    expect(clickAnchor(anchor!)).toBe(false);
    expect(window.location.pathname).toBe("/target");
    expect(window.location.hash).toBe("#section");
    await flush();
    await flush();
    expect(booted.app.querySelector(".title")?.textContent).toBe("Target");
  });

  const leftToBrowser: Array<[string, (a: HTMLAnchorElement) => boolean]> = [
    [
      "external origin",
      (a) => {
        a.href = "https://elsewhere.example/x";
        return clickAnchor(a);
      },
    ],
    [
      "target=_blank",
      (a) => {
        a.target = "_blank";
        return clickAnchor(a);
      },
    ],
    [
      "download attribute",
      (a) => {
        a.setAttribute("download", "");
        return clickAnchor(a);
      },
    ],
    [
      "modifier click (meta)",
      (a) => clickAnchor(a, { metaKey: true }),
    ],
    [
      "non-primary button",
      (a) => clickAnchor(a, { button: 1 }),
    ],
    [
      "same-page hash link",
      (a) => {
        a.href = "/start#work";
        return clickAnchor(a);
      },
    ],
  ];

  // Not-prevented means happy-dom performs the anchor's default action (it
  // navigates the location, as a real browser would) — so the contract under
  // test is non-prevention plus "the router never fetched", not the URL.
  for (const [name, act] of leftToBrowser) {
    it(`leaves ${name} to the browser`, async () => {
      const fetchMock = stubTargetFetch();
      let anchor: HTMLAnchorElement | null = null;
      await bootAtStart(twoAppRoutes, () => {
        anchor = addAnchor("/target");
      });

      expect(act(anchor!)).toBe(true); // not prevented
      await flush();
      expect(fetchMock).not.toHaveBeenCalled();
    });
  }

  // A `mode: "static"` target is prerendered via speculation rules and is
  // both correct and cheaper as a browser navigation — intercepting it must
  // not happen. This is the isBrowserNavigationTarget regression class.
  it("does not intercept a link to a fully-static route, and marks it", async () => {
    const fetchMock = stubTargetFetch();
    let anchor: HTMLAnchorElement | null = null;
    await bootAtStart(
      (deps) => ({
        ...twoAppRoutes(deps),
        "route:/about": {
          path: "/about",
          mode: "static",
          hasLoader: false,
          default: () => deps.h("p", null, "About"),
        },
      }),
      () => {
        anchor = addAnchor("/about");
      }
    );

    // markStaticLinks tagged it during init — that is what scopes the
    // speculation rules to static targets.
    expect(anchor!.hasAttribute("data-neutron-static")).toBe(true);

    expect(clickAnchor(anchor!)).toBe(true); // browser handles it
    await flush();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  // Static page under an app layout: the layout still needs the router, so
  // the whole chain is not browser-navigable and the interceptor must act.
  it("intercepts a static route that sits under an app layout", async () => {
    const fetchMock = vi.fn(async () => jsonResponse({ "route:/about": {} }));
    vi.stubGlobal("fetch", fetchMock);
    let anchor: HTMLAnchorElement | null = null;
    const booted = await bootAtStart(
      ({ h }) => ({
        "route:/start": {
          path: "/start",
          mode: "app",
          default: () => h("p", { class: "title" }, "Start"),
        },
        "route:/shell": {
          path: "/",
          parentId: null,
          isLayout: true,
          mode: "app",
          default: (props: { children?: ComponentChildren }) =>
            h("main", null, props.children),
        },
        "route:/about": {
          path: "/about",
          parentId: "route:/shell",
          mode: "static",
          default: () => h("p", { class: "title" }, "About"),
        },
      }),
      () => {
        anchor = addAnchor("/about");
      }
    );

    expect(anchor!.hasAttribute("data-neutron-static")).toBe(false);
    expect(clickAnchor(anchor!)).toBe(false);
    expect(window.location.pathname).toBe("/about");
    await flush();
    await flush();
    expect(booted.app.querySelector("main .title")?.textContent).toBe("About");
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
  it("TS-F16 ignores a late JSON body after navigation to another page", async () => {
    let release!: (data: unknown) => void;
    const late = new Promise(resolve => release = resolve);
    const fetchMock = vi.fn(async (url: string) => url.startsWith('/a')
      ? jsonResponse({}, { json: () => late })
      : jsonResponse({ 'route:/b': { title: 'B' } }));
    vi.stubGlobal('fetch', fetchMock);
    const booted = await bootAtStart(({ h, hooks }) => {
      const Page = () => h('p', { class: 'title' }, hooks.useLoaderData<{ title: string }>()?.title);
      return { 'route:/start': { path: '/start', mode: 'app', default: Page }, 'route:/a': { path: '/a', mode: 'app', default: Page }, 'route:/b': { path: '/b', mode: 'app', default: Page } };
    });
    clickAnchor(addAnchor('/a')); await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledWith('/a', expect.anything()));
    clickAnchor(addAnchor('/b')); await vi.waitFor(() => expect(booted.app.textContent).toContain('B'));
    release({ 'route:/a': { title: 'STALE' } }); await flush(); await flush();
    expect(window.location.pathname).toBe('/b'); expect(booted.app.textContent).toContain('B'); expect(window.__NEUTRON_DATA__?.['route:/a']).toBeUndefined();
  });

  it("O3 diffs against committed layout data while the first destination chunk is delayed", async () => {
    let release!: () => void, entered!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    const began = new Promise<void>(resolve => { entered = resolve; });
    const fetchMock = vi.fn(async (_url: string, _options: { headers: Record<string, string>; signal: AbortSignal }) => jsonResponse({ layout: { org: 'a' }, two: { title: 'Two' } }));
    vi.stubGlobal('fetch', fetchMock);
    const booted = await bootRouter({ pathname: '/org/z/home', routeId: 'home', data: { layout: { org: 'z' }, home: {} }, routes: ({ h }) => {
      const Page = () => h('p', null, 'page');
      return {
        layout: { path: '/org/:org', isLayout: true, mode: 'app', default: (props: any) => h('main', null, props.data?.org, props.children) },
        home: { path: '/org/:org/home', parentId: 'layout', mode: 'app', default: Page },
        one: { path: '/org/:org/one', parentId: 'layout', mode: 'app', default: Page, load: async () => { entered(); await held; return { default: Page }; } },
        two: { path: '/org/:org/two', parentId: 'layout', mode: 'app', default: Page },
      };
    } });
    clickAnchor(addAnchor('/org/a/one')); await began;
    clickAnchor(addAnchor('/org/a/two'));
    await vi.waitFor(() => expect(window.__NEUTRON_DATA__?.layout).toEqual({ org: 'a' }));
    expect(fetchMock.mock.calls[0][1].headers['X-Neutron-Routes'].split(',')).toEqual(['layout', 'two']);
    release(); await flush(); await flush();
    expect(window.location.pathname).toBe('/org/a/two'); expect(booted.app.textContent).toContain('a');
    expect(window.__NEUTRON_DATA__?.layout).toEqual({ org: 'a' });
  });
  it("O3 a delayed same-URL transition immediately aborts the older body and blocks its commit", async () => {
    let release!: (data: unknown) => void, transition!: () => void | Promise<void>;
    const oldBody = new Promise(resolve => { release = resolve; });
    let calls = 0;
    const fetchMock = vi.fn(async (_url: string, _options: RequestInit) => ++calls === 1
      ? jsonResponse({}, { json: () => oldBody }) : jsonResponse({ 'route:/target': { title: 'Latest' } }));
    vi.stubGlobal('fetch', fetchMock);
    const booted = await bootAtStart(twoAppRoutes);
    clickAnchor(addAnchor('/target'));
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    window.__NEUTRON_VIEW_TRANSITIONS__ = true;
    const previous = (document as any).startViewTransition;
    (document as any).startViewTransition = (callback: () => Promise<void>) => { transition = callback; return {}; };
    vi.spyOn(performance, 'now').mockReturnValue(100000);
    try {
      const { navigate } = await import('./navigate.js');
      navigate('/target');
      expect(fetchMock.mock.calls[0][1].signal?.aborted).toBe(true);
      expect(transition).toBeTypeOf('function');
      release({ 'route:/target': { title: 'STALE' } }); await flush(); await flush();
      expect(window.__NEUTRON_DATA__?.['route:/target']).toBeUndefined();
      await transition(); await flush();
      expect(booted.app.textContent).toContain('Latest');
    } finally { (document as any).startViewTransition = previous; delete window.__NEUTRON_VIEW_TRANSITIONS__; }
  });

});

describe("hydrate — a redirected navigation lands on the redirect target (F-P4-2)", () => {
  beforeEach(() => {
    window.history.replaceState(null, "", "/");
  });

  afterEach(() => {
    for (const { target, type, listener, capture } of capturedListeners) {
      target.removeEventListener(type, listener, capture);
    }
    restorePatchedAddEventListener?.();
    document.body.innerHTML = "";
    for (const key of WINDOW_STATE_KEYS) {
      delete (window as unknown as Record<string, unknown>)[key];
    }
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  function routes(deps: PageDeps): Record<string, RouteReg> {
    const { h, hooks } = deps;
    return {
      "route:/start": { path: "/start", mode: "app", default: () => h("p", { class: "title" }, "Start") },
      "route:/open/:id": { path: "/open/:id", mode: "app", hasLoader: true, default: () => h("p", { class: "title" }, "never") },
      "route:/workspace": {
        path: "/workspace",
        mode: "app",
        hasLoader: true,
        default: function Workspace() {
          const data = hooks.useLoaderData<{ open: string }>();
          return h("p", { class: "title" }, `workspace ${data?.open ?? "none"}`);
        },
      },
    };
  }

  function redirectResponse(location: string): Response {
    return {
      ok: true,
      status: 204,
      redirected: false,
      url: "",
      headers: new Headers({ "X-Neutron-Redirect": location }),
      json: async () => {
        throw new SyntaxError("Unexpected end of JSON input");
      },
    } as unknown as Response;
  }

  async function settle(): Promise<void> {
    for (let i = 0; i < 6; i++) await flush();
  }

  it("follows a reported same-origin redirect on the client, replacing the entry, query kept", async () => {
    const fetchMock = vi.fn(async (url: string) =>
      url.startsWith("/open/")
        ? redirectResponse("/workspace?open=note:x")
        : jsonResponse({ "route:/workspace": { open: new URL(url, "http://x").searchParams.get("open") } })
    );
    vi.stubGlobal("fetch", fetchMock);
    const booted = await bootRouter({
      pathname: "/start",
      routeId: "route:/start",
      data: { "route:/start": {} },
      routes,
    });
    const historyBefore = window.history.length;

    const { navigate } = await import("./navigate.js");
    navigate("/open/note-x");
    await settle();

    expect(window.location.pathname).toBe("/workspace");
    expect(window.location.search).toBe("?open=note:x");
    expect(booted.app.querySelector(".title")?.textContent).toBe("workspace note:x");
    // One entry for the navigation: the URL that redirected was replaced.
    expect(window.history.length).toBe(historyBefore + 1);
    expect(fetchMock.mock.calls.map((call) => call[0])).toEqual([
      "/open/note-x",
      "/workspace?open=note:x",
    ]);
    const second = fetchMock.mock.calls[1] as unknown as [string, RequestInit];
    expect((second[1].headers as Record<string, string>)["X-Neutron-Routes"]).toBe("route:/workspace");
  });

  it("falls back to response.redirected when fetch followed a redirect anyway", async () => {
    const fetchMock = vi.fn(async (url: string) =>
      url.startsWith("/open/")
        ? jsonResponse({ "route:/workspace": { open: null } }, {
            redirected: true,
            url: `${window.location.origin}/workspace?open=followed`,
          } as Partial<Response>)
        : jsonResponse({ "route:/workspace": { open: new URL(url, "http://x").searchParams.get("open") } })
    );
    vi.stubGlobal("fetch", fetchMock);
    const booted = await bootRouter({
      pathname: "/start",
      routeId: "route:/start",
      data: { "route:/start": {} },
      routes,
    });

    const { navigate } = await import("./navigate.js");
    navigate("/open/note-x");
    await settle();

    expect(window.location.pathname + window.location.search).toBe("/workspace?open=followed");
    expect(booted.app.querySelector(".title")?.textContent).toBe("workspace followed");
  });

  it("hands a cross-origin redirect to the browser as a replace", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => redirectResponse("https://auth.example/login")));
    const booted = await bootRouter({
      pathname: "/start",
      routeId: "route:/start",
      data: { "route:/start": {} },
      routes,
    });
    const replace = vi.spyOn(window.location, "replace").mockImplementation(() => {});

    const { navigate } = await import("./navigate.js");
    navigate("/open/note-x");
    await settle();

    expect(replace).toHaveBeenCalledWith("https://auth.example/login");
    expect(booted.app.querySelector(".title")?.textContent).toBe("Start");
  });
});

describe("hydrate — a not-found route never shadows the page it covers", () => {
  // A `not-found.tsx` carries its DIRECTORY's path, so on the client it
  // collides by path with that directory's index route. Only the isNotFound
  // flag separates them; before the build emitted it, whichever route came
  // first in the manifest won.
  it("renders the index route at /, even when not-found is listed first", async () => {
    history.replaceState({}, "", "/");
    const { app } = await bootRouter({
      pathname: "/",
      routeId: "route:index",
      routes: ({ h }) => ({
        "route:not-found": {
          path: "/",
          mode: "app",
          isNotFound: true,
          default: () => h("p", null, "four oh four"),
        },
        "route:index": {
          path: "/",
          mode: "app",
          default: () => h("p", null, "the real home page"),
        },
      }),
    });

    expect(app.textContent).toContain("the real home page");
    expect(app.textContent).not.toContain("four oh four");
  });
});

describe("hydrate — a document the server answered with its not-found page (F-P6-5)", () => {
  beforeEach(() => {
    for (const { target, type, listener, capture } of capturedListeners) {
      target.removeEventListener(type, listener, capture);
    }
    restorePatchedAddEventListener?.();
    window.history.replaceState(null, "", "/");
    for (const key of WINDOW_STATE_KEYS) {
      delete (window as unknown as Record<string, unknown>)[key];
    }
  });

  afterEach(() => {
    for (const { target, type, listener, capture } of capturedListeners) {
      target.removeEventListener(type, listener, capture);
    }
    restorePatchedAddEventListener?.();
    document.body.innerHTML = "";
    for (const key of WINDOW_STATE_KEYS) {
      delete (window as unknown as Record<string, unknown>)[key];
    }
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  // A loader's notFound() (page or layout), or a static route's ungenerated
  // param: the URL matches a page, but the server rendered not-found.tsx.
  function routes(deps: PageDeps): Record<string, RouteReg> {
    const { h, hooks } = deps;
    return {
      "route:/_layout": {
        path: "/",
        isLayout: true,
        mode: "app",
        default: function Layout({ children }: { children?: ComponentChildren }) {
          const data = hooks.useRouteLoaderData<{ site: string }>("route:/_layout");
          return h("main", null, h("nav", null, data?.site ?? "no-site"), children);
        },
      } as RouteReg,
      "route:/items/:id": {
        path: "/items/:id",
        parentId: "route:/_layout",
        mode: "app",
        hasLoader: true,
        default: function Item() {
          // Without its loader data this page throws — the error boundary
          // F-P6-5 showed in place of the 404.
          const data = hooks.useLoaderData<{ item: { name: string } }>();
          return h("p", { class: "title" }, `item ${data!.item.name}`);
        },
      },
      "route:/home": {
        path: "/home",
        parentId: "route:/_layout",
        mode: "app",
        hasLoader: true,
        default: () => h("p", { class: "title" }, "home"),
      },
      "route:/not-found": {
        path: "/",
        parentId: "route:/_layout",
        mode: "app",
        isNotFound: true,
        default: () => h("p", { class: "title" }, "four oh four"),
      },
    };
  }

  function markNotFound(routeId: string): void {
    const script = document.createElement("script");
    script.setAttribute("data-neutron-not-found", routeId);
    document.body.appendChild(script);
  }

  async function settle(): Promise<void> {
    for (let i = 0; i < 6; i++) await flush();
  }

  it("hydrates the not-found route the server rendered, not the page the URL matches", async () => {
    const { app } = await bootRouter({
      pathname: "/items/missing",
      routeId: "route:/not-found",
      data: { "route:/_layout": { site: "site-chrome" } },
      routes,
      beforeInit: () => markNotFound("route:/not-found"),
    });

    expect(app.textContent).toContain("four oh four");
    expect(app.textContent).toContain("site-chrome");
    expect(app.textContent).not.toContain("Something went wrong");
    expect(window.__NEUTRON_ACTIVE_ROUTE_IDS__).toEqual(["route:/_layout", "route:/not-found"]);
  });

  it("navigates away from it client-side, and reloads into it on a later loader 404", async () => {
    let anchor: HTMLAnchorElement | null = null;
    const fetchMock = vi.fn(async (url: string) => {
      if (String(url).startsWith("/home")) return jsonResponse({ "route:/home": {} });
      return jsonResponse(null, { ok: false, status: 404 } as Partial<Response>);
    });
    vi.stubGlobal("fetch", fetchMock);
    const reload = vi.spyOn(window.location, "reload").mockImplementation(() => {});

    const { app } = await bootRouter({
      pathname: "/items/missing",
      routeId: "route:/not-found",
      data: { "route:/_layout": { site: "site-chrome" } },
      routes,
      beforeInit: () => {
        markNotFound("route:/not-found");
        anchor = addAnchor("/home");
      },
    });

    expect(clickAnchor(anchor!)).toBe(false);
    await settle();
    expect(window.location.pathname).toBe("/home");
    expect(app.querySelector(".title")?.textContent).toBe("home");
    expect(app.textContent).toContain("site-chrome");
    expect(window.__NEUTRON_ACTIVE_ROUTE_IDS__).toEqual(["route:/_layout", "route:/home"]);
    expect(reload).not.toHaveBeenCalled();

    // The data request for a loader 404 answers non-OK: the router loads the
    // URL as a document, which the server answers with the not-found page
    // (the case the first test boots).
    const { navigate } = await import("./navigate.js");
    navigate("/items/other");
    await settle();
    expect(reload).toHaveBeenCalledTimes(1);
    expect(app.textContent).not.toContain("Something went wrong");
  });

  it("ignores a marker naming no registered not-found route", async () => {
    const { app } = await bootRouter({
      pathname: "/home",
      routeId: "route:/home",
      data: { "route:/_layout": { site: "site-chrome" }, "route:/home": {} },
      routes,
      beforeInit: () => markNotFound("route:/home"),
    });
    expect(app.querySelector(".title")?.textContent).toBe("home");
    expect(window.__NEUTRON_ACTIVE_ROUTE_IDS__).toEqual(["route:/_layout", "route:/home"]);
  });
});

describe("hydrate — static-tier page in an app (no client module)", () => {
  beforeEach(() => {
    // The suite above boots without cleanup: drop its click interceptor.
    for (const { target, type, listener, capture } of capturedListeners) {
      target.removeEventListener(type, listener, capture);
    }
    restorePatchedAddEventListener?.();
    window.history.replaceState(null, "", "/");
    for (const key of WINDOW_STATE_KEYS) {
      delete (window as unknown as Record<string, unknown>)[key];
    }
  });

  afterEach(() => {
    for (const { target, type, listener, capture } of capturedListeners) {
      target.removeEventListener(type, listener, capture);
    }
    restorePatchedAddEventListener?.();
    document.body.innerHTML = "";
    for (const key of WINDOW_STATE_KEYS) {
      delete (window as unknown as Record<string, unknown>)[key];
    }
    vi.unstubAllGlobals();
  });

  // The build leaves a static-tier route's module out of the client bundle
  // (staticTierRouteIds). Its document boots like a static page: islands
  // hydrate, the router does not start and never asks for the module.
  it("does not start the router, and leaves links to the browser", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    let anchor: HTMLAnchorElement | null = null;
    await bootRouter({
      pathname: "/legal",
      routeId: "route:/legal",
      appHtml: "<p>legal prerendered</p>",
      routes: ({ h }) =>
        ({
          "route:/app": { path: "/app", mode: "app", default: () => h("p", null, "app") },
          "route:/legal": { path: "/legal", mode: "static", hasLoader: false, clientModule: false },
        }) as unknown as Record<string, RouteReg>,
      beforeInit: () => {
        anchor = addAnchor("/app");
      },
    });
    expect(window.__NEUTRON_ROUTER_ACTIVE__).toBeUndefined();
    expect(document.getElementById("app")!.innerHTML).toBe("<p>legal prerendered</p>");
    expect(clickAnchor(anchor!)).toBe(true);
    await flush();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("navigates to a static-tier route with a document load, not the router", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    let anchor: HTMLAnchorElement | null = null;
    await bootRouter({
      pathname: "/app",
      routeId: "route:/app",
      routes: ({ h }) =>
        ({
          "route:/shell": { path: "/", isLayout: true, mode: "app", default: (p: { children?: ComponentChildren }) => h("div", null, p.children) },
          "route:/app": { path: "/app", parentId: "route:/shell", mode: "app", default: () => h("p", null, "app") },
          "route:/legal": { path: "/legal", parentId: "route:/shell", mode: "static", clientModule: false },
        }) as unknown as Record<string, RouteReg>,
      beforeInit: () => {
        anchor = addAnchor("/legal");
      },
    });
    expect(window.__NEUTRON_ROUTER_ACTIVE__).toBe(true);
    // Under an app layout, but without a module: only a document load renders it.
    expect(clickAnchor(anchor!)).toBe(true);
    await flush();
    expect(fetchMock).not.toHaveBeenCalled();
  });
});

/**
 * F-B5-1: client navigation kept every earlier route's stylesheets in the
 * document (Next parity), so sections redefining the same :root tokens
 * resolved by load order. `client.unloadRouteStylesheets` unlinks the route
 * stylesheets the new route does not list, after it has rendered.
 */
describe("hydrate — route stylesheets after navigation", () => {
  afterEach(() => {
    for (const { target, type, listener, capture } of capturedListeners) {
      target.removeEventListener(type, listener, capture);
    }
    restorePatchedAddEventListener?.();
    document.body.innerHTML = "";
    document.head.innerHTML = "";
    for (const key of WINDOW_STATE_KEYS) {
      delete (window as unknown as Record<string, unknown>)[key];
    }
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  function routes(deps: PageDeps): Record<string, RouteReg> {
    const { h, hooks } = deps;
    return {
      "route:/start": { path: "/start", mode: "app", default: () => h("p", { class: "title" }, "Start") },
      "route:/target": {
        path: "/target",
        mode: "app",
        default: () => h("p", { class: "title" }, hooks.useLoaderData<{ title: string }>()?.title ?? "none"),
      },
    };
  }

  const sheets = () =>
    Array.from(document.head.querySelectorAll<HTMLLinkElement>('link[rel="stylesheet"]')).map((link) => link.getAttribute("href"));

  async function navigateToTarget(unload: boolean): Promise<string[]> {
    document.head.innerHTML = [
      '<link rel="stylesheet" href="/assets/shared.css" data-neutron-css>',
      '<link rel="stylesheet" href="/assets/start.css" data-neutron-css>',
      // Loaded by an earlier visit to /target: no fetch needed to show it.
      '<link rel="stylesheet" href="/assets/target.css" data-neutron-css>',
      // Not Neutron's: a head() link, never touched.
      '<link rel="stylesheet" href="/fonts.css">',
    ].join("");
    const frames: FrameRequestCallback[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        jsonResponse({
          "route:/target": { title: "Target" },
          __css__: ["/assets/shared.css", "/assets/target.css"],
        })
      )
    );
    const booted = await bootRouter({
      pathname: "/start",
      routeId: "route:/start",
      data: { "route:/start": {} },
      routes,
    });
    if (unload) {
      (await import("./router-options.js")).configureRouter({ unloadRouteStylesheets: true });
    }
    vi.stubGlobal("requestAnimationFrame", (callback: FrameRequestCallback) => frames.push(callback));
    clickAnchor(addAnchor("/target"));
    for (let i = 0; i < 4; i++) await flush();
    // The new page is in the DOM; nothing is unlinked before the next frame.
    expect(booted.app.querySelector(".title")?.textContent).toBe("Target");
    expect(sheets()).toContain("/assets/start.css");
    for (const frame of frames.splice(0)) frame(performance.now());
    return sheets() as string[];
  }

  it("keeps earlier routes' stylesheets by default", async () => {
    expect(await navigateToTarget(false)).toEqual([
      "/assets/shared.css",
      "/assets/start.css",
      "/assets/target.css",
      "/fonts.css",
    ]);
  });

  it("with unloadRouteStylesheets, unlinks the ones the new route does not list, after it rendered", async () => {
    expect(await navigateToTarget(true)).toEqual(["/assets/shared.css", "/assets/target.css", "/fonts.css"]);
  });
});

/**
 * F-P6-3: what a production build does to the document during client
 * navigation, with `client.unloadRouteStylesheets`. Three routes each bring
 * a theme sheet that redefines the same tokens; `modal.css` is static for /a
 * (in its list) and lazy for /b (a dynamic import /b's page loads, listed in
 * `__cssLazy__`). Route chunks and lazy modules load through a stand-in for
 * Vite's preload helper with its real rules: it links a chunk's CSS
 * unmarked, skips an href already linked, and never links an href twice in
 * a page's life (its `seen` map). After A→B→C→A→C→B the document must hold
 * exactly the current route's sheets: no other theme left behind (the
 * cascade is then the same as on a fresh load), none of its own missing.
 */
describe("hydrate — route stylesheets across chunk-loaded routes (F-P6-3)", () => {
  afterEach(() => {
    for (const { target, type, listener, capture } of capturedListeners) {
      target.removeEventListener(type, listener, capture);
    }
    restorePatchedAddEventListener?.();
    document.body.innerHTML = "";
    document.head.innerHTML = "";
    for (const key of WINDOW_STATE_KEYS) {
      delete (window as unknown as Record<string, unknown>)[key];
    }
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  const SHARED = "/assets/shared.css";
  const MODAL = "/assets/modal.css";
  const theme = (route: string) => `/assets/theme-${route}.css`;

  // Vite's __vitePreload, CSS half (vite/src/node/plugins/importAnalysisBuild.ts).
  const seen = new Set<string>();
  function vitePreload(hrefs: string[]): void {
    for (const href of hrefs) {
      if (seen.has(href)) continue;
      seen.add(href);
      const linked = Array.from(document.querySelectorAll<HTMLLinkElement>('link[rel="stylesheet"]')).some(
        (link) => new URL(link.href, location.origin).pathname === href
      );
      if (linked) continue;
      const link = document.createElement("link");
      link.rel = "stylesheet";
      link.crossOrigin = "";
      link.href = href;
      document.head.appendChild(link);
    }
  }

  const payloads: Record<string, Record<string, unknown>> = {
    "/a": { "route:/a": {}, __css__: [SHARED, theme("a"), MODAL] },
    "/b": { "route:/b": {}, __css__: [SHARED, theme("b")], __cssLazy__: [MODAL] },
    "/c": { "route:/c": {}, __css__: [SHARED, theme("c")] },
  };

  function routes(deps: PageDeps): Record<string, RouteReg> {
    const { h, preactHooks } = deps;
    const page = (name: string) => () => h("p", { class: "title" }, name);
    // /b's page opens its lazily imported modal when it mounts.
    function PageB() {
      preactHooks.useEffect(() => vitePreload([MODAL]), []);
      return h("p", { class: "title" }, "B");
    }
    const chunk = (css: string[], component: ComponentType) => async () => {
      vitePreload(css);
      return { default: component };
    };
    return {
      "route:/a": { path: "/a", mode: "app", default: page("A"), load: chunk([SHARED, theme("a"), MODAL], page("A")) },
      "route:/b": { path: "/b", mode: "app", default: PageB, load: chunk([SHARED, theme("b")], PageB) },
      "route:/c": { path: "/c", mode: "app", default: page("C"), load: chunk([SHARED, theme("c")], page("C")) },
    };
  }

  const sheets = () =>
    Array.from(document.head.querySelectorAll<HTMLLinkElement>('link[rel="stylesheet"]'))
      .map((link) => new URL(link.href, location.origin).pathname)
      .sort();

  it("leaves exactly the current route's sheets after A→B→C→A→C→B", async () => {
    seen.clear();
    document.head.innerHTML = [SHARED, theme("a"), MODAL]
      .map((href) => `<link rel="stylesheet" href="${href}" data-neutron-css>`)
      .join("");
    // No stylesheet server here (happy-dom's own fetch of a sheet fails and
    // logs): report each linked sheet loaded.
    const observer = new MutationObserver((records) => {
      for (const record of records) {
        record.addedNodes.forEach((node) => {
          if (node instanceof HTMLLinkElement) setTimeout(() => node.dispatchEvent(new Event("load")), 0);
        });
      }
    });
    observer.observe(document.head, { childList: true });
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: string) => jsonResponse(structuredClone(payloads[new URL(input, location.origin).pathname])))
    );
    const frames: FrameRequestCallback[] = [];
    const booted = await bootRouter({ pathname: "/a", routeId: "route:/a", data: { "route:/a": {} }, routes });
    (await import("./router-options.js")).configureRouter({ unloadRouteStylesheets: true });
    vi.stubGlobal("requestAnimationFrame", (callback: FrameRequestCallback) => frames.push(callback));

    const go = async (to: string) => {
      clickAnchor(addAnchor(to));
      for (let i = 0; i < 8; i++) await flush();
      expect(booted.app.querySelector(".title")?.textContent).toBe(to.slice(1).toUpperCase());
      for (const frame of frames.splice(0)) frame(performance.now());
      await flush();
      return sheets();
    };

    try {
      expect(await go("/b")).toEqual([MODAL, SHARED, theme("b")].sort());
      expect(await go("/c")).toEqual([SHARED, theme("c")].sort());
      expect(await go("/a")).toEqual([MODAL, SHARED, theme("a")].sort());
      expect(await go("/c")).toEqual([SHARED, theme("c")].sort());
      // /b's modal was imported before: Vite will not link its CSS again.
      expect(await go("/b")).toEqual([MODAL, SHARED, theme("b")].sort());
    } finally {
      observer.disconnect();
    }
  });
});

/**
 * F-P6-1: client navigation applies the page's whole resolved head
 * (`__head__`): every managed <meta>/<link> the new head no longer has goes
 * (a previous page's robots noindex, both theme-colors), each one it adds
 * arrives, identical ones stay in place, and tags other code inserted at
 * runtime are left alone. Entities in the title are decoded.
 */
describe("hydrate — head tags after navigation (F-P6-1)", () => {
  afterEach(() => {
    for (const { target, type, listener, capture } of capturedListeners) {
      target.removeEventListener(type, listener, capture);
    }
    restorePatchedAddEventListener?.();
    document.body.innerHTML = "";
    document.head.innerHTML = "";
    for (const key of WINDOW_STATE_KEYS) {
      delete (window as unknown as Record<string, unknown>)[key];
    }
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  function routes(deps: PageDeps): Record<string, RouteReg> {
    const { h } = deps;
    return {
      "route:/settings": { path: "/settings", mode: "app", default: () => h("p", { class: "title" }, "Settings") },
      "route:/chat": { path: "/chat", mode: "app", default: () => h("p", { class: "title" }, "Chat") },
    };
  }

  it("replaces the previous page's managed head tags with the new page's", async () => {
    document.head.innerHTML = [
      '<meta charset="UTF-8">',
      '<meta name="viewport" content="width=device-width, initial-scale=1, interactive-widget=resizes-content">',
      "<title>Settings | Fylun</title>",
      '<meta name="theme-color" media="(prefers-color-scheme: light)" content="#fff">',
      '<meta name="theme-color" media="(prefers-color-scheme: dark)" content="#000">',
      '<meta name="robots" content="noindex">',
      '<meta property="og:site_name" content="Fylun">',
      '<link rel="canonical" href="https://fylun.ai/settings">',
      '<link rel="search" type="application/opensearchdescription+xml" href="/opensearch.xml">',
    ].join("");
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        jsonResponse({
          "route:/chat": {},
          __head__: [
            '<meta charset="UTF-8">',
            '<meta name="viewport" content="width=device-width, initial-scale=1, interactive-widget=resizes-content">',
            "<title>Chat &amp; more | Fylun</title>",
            '<meta name="theme-color" media="(prefers-color-scheme: light)" content="white">',
            '<meta name="theme-color" media="(prefers-color-scheme: dark)" content="#0a0a0a">',
            '<meta property="og:site_name" content="Fylun">',
            '<meta name="google-site-verification" content="g-token">',
            '<link rel="canonical" href="https://fylun.ai">',
            '<link rel="search" type="application/opensearchdescription+xml" href="/opensearch.xml">',
            '<script type="application/ld+json">{}</script>',
          ].join("\n"),
        })
      )
    );
    const booted = await bootRouter({ pathname: "/settings", routeId: "route:/settings", data: { "route:/settings": {} }, routes });
    const siteName = document.head.querySelector('meta[property="og:site_name"]');
    // Inserted at runtime by other code (an analytics snippet, say).
    const foreign = document.createElement("meta");
    foreign.setAttribute("name", "third-party");
    foreign.setAttribute("content", "kept");
    document.head.appendChild(foreign);

    clickAnchor(addAnchor("/chat"));
    for (let i = 0; i < 4; i++) await flush();
    expect(booted.app.querySelector(".title")?.textContent).toBe("Chat");

    expect(document.title).toBe("Chat & more | Fylun");
    const metas = (selector: string) =>
      Array.from(document.head.querySelectorAll(selector)).map((element) => element.getAttribute("content") ?? element.getAttribute("href"));
    expect(metas('meta[name="theme-color"]').sort()).toEqual(["#0a0a0a", "white"]);
    expect(metas('meta[name="robots"]')).toEqual([]);
    expect(metas('meta[name="google-site-verification"]')).toEqual(["g-token"]);
    expect(metas('link[rel="canonical"]')).toEqual(["https://fylun.ai"]);
    expect(metas('meta[name="viewport"]')).toEqual(["width=device-width, initial-scale=1, interactive-widget=resizes-content"]);
    expect(metas('link[rel="search"]')).toEqual(["/opensearch.xml"]);
    expect(document.head.querySelectorAll("meta[charset]")).toHaveLength(1);
    // Unchanged tags are the same elements; other code's are untouched.
    expect(document.head.querySelector('meta[property="og:site_name"]')).toBe(siteName);
    expect(foreign.isConnected).toBe(true);
    expect(document.head.querySelector('script[type="application/ld+json"]')).toBeNull();
  });
});
