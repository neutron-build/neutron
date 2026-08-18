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
  mode?: "static" | "app";
  hasLoader?: boolean;
  default: ComponentType;
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
});
