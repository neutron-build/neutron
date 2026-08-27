// @vitest-environment happy-dom
import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { h, render, type ComponentChild, type VNode } from "preact";
import * as hooks from "./hooks.js";
import { navigate } from "./navigate.js";
import { MatchesContext } from "./contexts.js";
import { clearPrefetchCache, hasFreshPrefetch } from "./prefetch-cache.js";

/**
 * The public hooks surface (client/hooks.ts). Context-reading hooks are
 * tested through the providers hydrateApp/the SSR renderer mount; the
 * imperative hooks (useRevalidator, useSubmit, useNavigate, useSearchParams)
 * are tested against the real window.location they mutate.
 */

function flush(): Promise<void> {
  return new Promise((r) => setTimeout(r, 0));
}

/**
 * preact defers useEffect to requestAnimationFrame, which a plain
 * setTimeout(0) flush can lose a race against. Wait one frame, then a
 * macrotask, so subscriptions are guaranteed registered (or cleaned up).
 */
function flushEffects(): Promise<void> {
  return new Promise<void>((resolve) =>
    requestAnimationFrame(() => setTimeout(resolve, 0))
  );
}

function jsonResponse(body: unknown, init: Partial<Response> = {}): Response {
  return {
    ok: true,
    redirected: false,
    url: "http://localhost/unused",
    headers: new Headers({ "content-type": "application/json" }),
    json: async () => body,
    ...init,
  } as Response;
}

const mountedContainers: HTMLDivElement[] = [];

function mount(node: ComponentChild): HTMLDivElement {
  const container = document.createElement("div");
  document.body.appendChild(container);
  mountedContainers.push(container);
  render(node, container);
  return container;
}

interface ProviderOptions {
  router?: Partial<hooks.RouterState>;
  loader?: hooks.LoaderData;
  action?: unknown;
  navigation?: hooks.NavigationState;
  matches?: hooks.UIMatch[];
}

function withProviders(node: ComponentChild, ctx: ProviderOptions = {}): VNode<any> {
  return h(
    hooks.RouterContext.Provider,
    {
      value: {
        routeId: "",
        pathname: "/",
        search: "",
        params: {},
        ...ctx.router,
      },
    },
    h(
      hooks.LoaderContext.Provider,
      { value: ctx.loader ?? {} },
      h(
        hooks.ActionDataContext.Provider,
        { value: ctx.action },
        h(
          hooks.NavigationContext.Provider,
          { value: ctx.navigation ?? { state: "idle" } },
          h(MatchesContext.Provider, { value: ctx.matches ?? [] }, node)
        )
      )
    )
  );
}

function makeForm(fields: Record<string, string>): HTMLFormElement {
  const form = document.createElement("form");
  for (const [name, value] of Object.entries(fields)) {
    const input = document.createElement("input");
    input.name = name;
    input.value = value;
    form.appendChild(input);
  }
  document.body.appendChild(form);
  return form;
}

beforeEach(() => {
  window.history.replaceState(null, "", "/");
  document.body.innerHTML = "";
  mountedContainers.length = 0;
  clearPrefetchCache();
  for (const key of [
    "__NEUTRON_DATA__",
    "__NEUTRON_DATA_SERIALIZED__",
    "__NEUTRON_NAVIGATION_STATE__",
    "__NEUTRON_ACTIVE_ROUTE_IDS__",
    "__NEUTRON_PREFETCH_CACHE__",
  ] as const) {
    delete (window as unknown as Record<string, unknown>)[key];
  }
});

afterEach(async () => {
  for (const container of mountedContainers) {
    if (container.isConnected) render(null, container);
  }
  document.body.innerHTML = "";
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("context hooks", () => {
  it("useLoaderData returns the current route's slice", () => {
    let seen: { title: string } | undefined;
    function Probe(): VNode<any> {
      seen = hooks.useLoaderData<{ title: string }>();
      return h("p", null, String(seen?.title));
    }
    const container = mount(
      withProviders(h(Probe, null), {
        router: { routeId: "route:/a" },
        loader: { "route:/a": { title: "A" }, "route:/b": { title: "B" } },
      })
    );
    expect(seen).toEqual({ title: "A" });
    expect(container.textContent).toBe("A");
  });

  it("useLoaderData falls back to data.page when the route id misses", () => {
    let seen: unknown;
    function Probe(): VNode<any> {
      seen = hooks.useLoaderData();
      return h("p", null, "x");
    }
    mount(
      withProviders(h(Probe, null), {
        router: { routeId: "route:/missing" },
        loader: { page: { title: "Page" } },
      })
    );
    expect(seen).toEqual({ title: "Page" });
  });

  it("useLoaderData is undefined when nothing matches", () => {
    let seen: unknown = "sentinel";
    function Probe(): VNode<any> {
      seen = hooks.useLoaderData();
      return h("p", null, "x");
    }
    mount(withProviders(h(Probe, null), { router: { routeId: "route:/x" } }));
    expect(seen).toBeUndefined();
  });

  it("useRouteLoaderData reads any route in the chain, undefined when absent", () => {
    let direct: unknown;
    let missing: unknown;
    function Probe(): VNode<any> {
      direct = hooks.useRouteLoaderData<{ n: number }>("route:/layout");
      missing = hooks.useRouteLoaderData("route:/nope");
      return h("p", null, "x");
    }
    mount(
      withProviders(h(Probe, null), {
        loader: { "route:/layout": { n: 3 }, "route:/page": {} },
      })
    );
    expect(direct).toEqual({ n: 3 });
    expect(missing).toBeUndefined();
  });

  it("useActionData is undefined without an action, the payload with one", () => {
    let withoutAction: unknown = "sentinel";
    let withAction: unknown;
    function A(): VNode<any> {
      withoutAction = hooks.useActionData();
      return h("p", null, "x");
    }
    function B(): VNode<any> {
      withAction = hooks.useActionData<{ ok: boolean }>();
      return h("p", null, "y");
    }
    mount(withProviders(h(A, null)));
    mount(withProviders(h(B, null), { action: { ok: true } }));
    expect(withoutAction).toBeUndefined();
    expect(withAction).toEqual({ ok: true });
  });

  it("useParams and useLocation read the router context", () => {
    let params: Record<string, string> | undefined;
    let location: { pathname: string; search: string } | undefined;
    function Probe(): VNode<any> {
      params = hooks.useParams();
      location = hooks.useLocation();
      return h("p", null, "x");
    }
    mount(
      withProviders(h(Probe, null), {
        router: { pathname: "/users/7", search: "?tab=logs", params: { id: "7" } },
      })
    );
    expect(params).toEqual({ id: "7" });
    expect(location).toEqual({ pathname: "/users/7", search: "?tab=logs" });
  });

  it("useMatches returns the mounted matches", () => {
    // Note: no framework path mounts MatchesContext today (neither the SSR
    // renderer nor hydrateApp), so this provider stands in for the wiring
    // that does not exist yet — see the S74 report. The hook itself reads
    // the context correctly when one is mounted.
    const matches: hooks.UIMatch[] = [
      { id: "route:/layout", pathname: "/", params: {}, data: null },
      { id: "route:/page", pathname: "/p", params: {}, data: { x: 1 }, handle: { nav: 1 } },
    ];
    let seen: hooks.UIMatch[] | undefined;
    function Probe(): VNode<any> {
      seen = hooks.useMatches();
      return h("p", null, "x");
    }
    mount(withProviders(h(Probe, null), { matches }));
    expect(seen).toBe(matches);
  });
});

describe("useNavigation", () => {
  it("reports idle when nothing is in flight", () => {
    function Probe(): VNode<any> {
      const nav = hooks.useNavigation();
      return h("p", { class: "state" }, nav.state);
    }
    const container = mount(withProviders(h(Probe, null)));
    expect(container.querySelector(".state")?.textContent).toBe("idle");
  });

  it("prefers an in-flight window state over the context", () => {
    window.__NEUTRON_NAVIGATION_STATE__ = {
      state: "loading",
      location: "/next",
    };
    function Probe(): VNode<any> {
      const nav = hooks.useNavigation();
      return h("p", { class: "state" }, `${nav.state}:${nav.location ?? ""}`);
    }
    const container = mount(
      withProviders(h(Probe, null), { navigation: { state: "idle" } })
    );
    expect(container.querySelector(".state")?.textContent).toBe("loading:/next");
  });

  it("falls back to the context state when the window goes idle", () => {
    // The context is what the SSR renderer mounted; once the client state
    // machine returns to idle the context answer must win again.
    window.__NEUTRON_NAVIGATION_STATE__ = { state: "idle" };
    function Probe(): VNode<any> {
      const nav = hooks.useNavigation();
      return h("p", { class: "state" }, nav.state);
    }
    const container = mount(
      withProviders(h(Probe, null), { navigation: { state: "loading" } })
    );
    expect(container.querySelector(".state")?.textContent).toBe("loading");
  });

  it("tracks neutron:navigation events after mount", async () => {
    function Probe(): VNode<any> {
      const nav = hooks.useNavigation();
      return h("p", { class: "state" }, nav.state);
    }
    const container = mount(withProviders(h(Probe, null)));
    await flushEffects(); // let the subscription effect register

    hooks.setNavigationState({ state: "submitting" });
    await flush();
    expect(container.querySelector(".state")?.textContent).toBe("submitting");

    hooks.setNavigationState({ state: "idle" });
    await flush();
    expect(container.querySelector(".state")?.textContent).toBe("idle");
  });
});

describe("setNavigationState", () => {
  it("writes the window global and dispatches the event", () => {
    const events: hooks.NavigationState[] = [];
    const onNav = (event: Event): void => {
      events.push((event as CustomEvent<hooks.NavigationState>).detail);
    };
    window.addEventListener("neutron:navigation", onNav);

    hooks.setNavigationState({ state: "loading", location: "/x" });

    expect(window.__NEUTRON_NAVIGATION_STATE__).toEqual({
      state: "loading",
      location: "/x",
    });
    expect(events).toEqual([{ state: "loading", location: "/x" }]);
    window.removeEventListener("neutron:navigation", onNav);
  });
});

describe("useSearchParams", () => {
  it("parses the current search", () => {
    let params: URLSearchParams | undefined;
    function Probe(): VNode<any> {
      const [searchParams] = hooks.useSearchParams();
      params = searchParams;
      return h("p", null, "x");
    }
    mount(
      withProviders(h(Probe, null), { router: { search: "?q=neutron&tab=docs" } })
    );
    expect(params?.get("q")).toBe("neutron");
    expect(params?.get("tab")).toBe("docs");
  });

  it("setter navigates to the new query string", () => {
    window.history.replaceState(null, "", "/list");
    let setSearchParams: ((p: URLSearchParams) => void) | undefined;
    function Probe(): VNode<any> {
      [, setSearchParams] = hooks.useSearchParams();
      return h("p", null, "x");
    }
    mount(withProviders(h(Probe, null)));

    setSearchParams!(new URLSearchParams("page=2"));
    expect(window.location.pathname + window.location.search).toBe(
      "/list?page=2"
    );

    // Empty params drop the "?" entirely rather than leaving a bare one.
    setSearchParams!(new URLSearchParams());
    expect(window.location.pathname + window.location.search).toBe("/list");
  });
});

describe("useNavigate", () => {
  it("navigates for a string target", () => {
    let navigate: ((to: "/dest") => void) | undefined;
    function Probe(): VNode<any> {
      navigate = hooks.useNavigate();
      return h("p", null, "x");
    }
    mount(withProviders(h(Probe, null)));

    navigate!("/dest");
    expect(window.location.pathname).toBe("/dest");
  });

  it("delegates a number target to history.go", () => {
    const goSpy = vi.spyOn(window.history, "go").mockImplementation(() => {});
    let navigate: ((to: -1) => void) | undefined;
    function Probe(): VNode<any> {
      navigate = hooks.useNavigate();
      return h("p", null, "x");
    }
    mount(withProviders(h(Probe, null)));

    navigate!(-1);
    expect(goSpy).toHaveBeenCalledWith(-1);
    expect(window.location.pathname).toBe("/"); // no pushState happened
  });
});

describe("useRevalidator", () => {
  it("refetches the current URL with the data headers and publishes the result", async () => {
    window.history.replaceState(null, "", "/dash");
    window.__NEUTRON_ACTIVE_ROUTE_IDS__ = ["route:/layout", "route:/dash"];
    // Held pending so the in-flight state is observable before the result.
    let resolveFetch!: (response: Response) => void;
    const fetchMock = vi.fn(
      () =>
        new Promise<Response>((resolve) => {
          resolveFetch = resolve;
        })
    );
    vi.stubGlobal("fetch", fetchMock);

    const dataEvents: unknown[] = [];
    const onData = (event: Event): void => {
      dataEvents.push((event as CustomEvent).detail);
    };
    window.addEventListener("neutron:data-updated", onData);

    let revalidate: (() => Promise<void>) | undefined;
    function Probe(): VNode<any> {
      const { revalidate: fn, state } = hooks.useRevalidator();
      revalidate = fn;
      return h("p", { class: "state" }, state);
    }
    const container = mount(
      withProviders(h(Probe, null), { router: { routeId: "route:/dash" } })
    );
    await flushEffects();
    expect(container.querySelector(".state")?.textContent).toBe("idle");

    const pending = revalidate!();
    await flush();
    expect(container.querySelector(".state")?.textContent).toBe("loading");

    resolveFetch(jsonResponse({ "route:/dash": { total: 41 } }));
    await pending;
    await flush();
    expect(container.querySelector(".state")?.textContent).toBe("idle");

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, init] = fetchMock.mock.calls[0] as unknown as [
      string,
      RequestInit,
    ];
    expect(url).toBe(`${window.location.origin}/dash`);
    const headers = init.headers as Record<string, string>;
    expect(headers.Accept).toBe("application/json");
    expect(headers["X-Neutron-Data"]).toBe("true");
    expect(headers["X-Neutron-Routes"]).toBe("route:/layout,route:/dash");

    expect(window.__NEUTRON_DATA__).toEqual({ "route:/dash": { total: 41 } });
    expect(dataEvents).toEqual([{ "route:/dash": { total: 41 } }]);
    window.removeEventListener("neutron:data-updated", onData);
  });

  it("discards a stale response when a newer revalidate wins the race", async () => {
    window.history.replaceState(null, "", "/dash");
    let resolveFirst!: (response: Response) => void;
    const firstFetch = new Promise<Response>((resolve) => {
      resolveFirst = resolve;
    });
    const fetchMock = vi
      .fn()
      .mockImplementationOnce(() => firstFetch)
      .mockImplementationOnce(async () =>
        jsonResponse({ "route:/dash": { total: 2 } })
      );
    vi.stubGlobal("fetch", fetchMock);

    let revalidate: (() => Promise<void>) | undefined;
    function Probe(): VNode<any> {
      const { revalidate: fn } = hooks.useRevalidator();
      revalidate = fn;
      return h("p", null, "x");
    }
    mount(withProviders(h(Probe, null)));

    const first = revalidate!();
    await flush();
    const second = revalidate!();
    await second;
    await flush();
    expect(window.__NEUTRON_DATA__).toEqual({ "route:/dash": { total: 2 } });

    resolveFirst(jsonResponse({ "route:/dash": { total: 1 } }));
    await first;
    await flush();
    // The older payload must not overwrite the newer one.
    expect(window.__NEUTRON_DATA__).toEqual({ "route:/dash": { total: 2 } });
  });

  it("keeps state idle and publishes nothing when the response is not ok", async () => {
    window.history.replaceState(null, "", "/dash");
    const fetchMock = vi.fn(async () =>
      jsonResponse({ "route:/dash": { total: 99 } }, { ok: false })
    );
    vi.stubGlobal("fetch", fetchMock);

    const onData = vi.fn();
    window.addEventListener("neutron:data-updated", onData);

    let revalidate: (() => Promise<void>) | undefined;
    function Probe(): VNode<any> {
      const { revalidate: fn, state } = hooks.useRevalidator();
      revalidate = fn;
      return h("p", { class: "state" }, state);
    }
    const container = mount(withProviders(h(Probe, null)));

    await revalidate!();
    await flush();

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(window.__NEUTRON_DATA__).toBeUndefined();
    expect(onData).not.toHaveBeenCalled();
    expect(container.querySelector(".state")?.textContent).toBe("idle");
    window.removeEventListener("neutron:data-updated", onData);
  });
});

describe("useSubmit", () => {
  function mountPanel(): {
    submit: (form: HTMLFormElement | FormData, options?: hooks.SubmitOptions) => Promise<void>;
    container: HTMLDivElement;
  } {
    const ref: {
      submit?: (
        form: HTMLFormElement | FormData,
        options?: hooks.SubmitOptions
      ) => Promise<void>;
    } = {};
    function Panel(): VNode<any> {
      const { submit } = hooks.useSubmit();
      ref.submit = submit;
      const nav = hooks.useNavigation();
      return h("p", { class: "nav" }, nav.state);
    }
    const container = mount(withProviders(h(Panel, null)));
    return { submit: ref.submit!, container };
  }

  it("GET turns the form into a query string and navigates without fetching", async () => {
    window.history.replaceState(null, "", "/search");
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const form = makeForm({ q: "neutron" });

    const { submit } = mountPanel();
    await submit(form, { method: "GET" });

    expect(window.location.pathname + window.location.search).toBe(
      "/search?q=neutron"
    );
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("POST submits the FormData, publishes the response data, and returns to idle", async () => {
    window.history.replaceState(null, "", "/form");
    // Held pending so the submitting state is observable mid-flight.
    let resolveFetch!: (response: Response) => void;
    const fetchMock = vi.fn(
      () =>
        new Promise<Response>((resolve) => {
          resolveFetch = resolve;
        })
    );
    vi.stubGlobal("fetch", fetchMock);
    const form = makeForm({ title: "hello" });

    const onData = vi.fn();
    window.addEventListener("neutron:data-updated", onData);

    const { submit, container } = mountPanel();
    await flushEffects(); // useNavigation's subscription must be registered

    const pending = submit(form);
    await flush();
    expect(container.querySelector(".nav")?.textContent).toBe("submitting");

    resolveFetch(jsonResponse({ "route:/form": { ok: true } }));
    await pending;
    await flush();

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [action, init] = fetchMock.mock.calls[0] as unknown as [
      string,
      RequestInit,
    ];
    expect(action).toBe("/form");
    expect(init.method).toBe("POST");
    expect(init.body).toBeInstanceOf(FormData);
    const headers = init.headers as Record<string, string>;
    expect(headers["X-Neutron-Data"]).toBe("true");

    expect(window.__NEUTRON_DATA__).toEqual({ "route:/form": { ok: true } });
    expect(onData).toHaveBeenCalledWith(
      expect.objectContaining({ detail: { "route:/form": { ok: true } } })
    );
    // Post-mutation data is stashed via the shared (expiring) prefetch cache.
    expect(hasFreshPrefetch("/form")).toBe(true);
    expect(container.querySelector(".nav")?.textContent).toBe("idle");
    window.removeEventListener("neutron:data-updated", onData);
  });

  it("follows a same-origin redirect response by navigating to it", async () => {
    window.history.replaceState(null, "", "/form");
    const fetchMock = vi.fn(async () =>
      jsonResponse({}, { redirected: true, url: `${window.location.origin}/done` })
    );
    vi.stubGlobal("fetch", fetchMock);

    const onData = vi.fn();
    window.addEventListener("neutron:data-updated", onData);

    const { submit } = mountPanel();
    await submit(makeForm({}));

    expect(window.location.pathname).toBe("/done");
    expect(window.__NEUTRON_DATA__).toBeUndefined();
    expect(onData).not.toHaveBeenCalled();
    window.removeEventListener("neutron:data-updated", onData);
  });

  it("follows a JSON redirect result body", async () => {
    window.history.replaceState(null, "", "/form");
    const fetchMock = vi.fn(async () =>
      jsonResponse({ redirect: "/elsewhere" })
    );
    vi.stubGlobal("fetch", fetchMock);

    const { submit } = mountPanel();
    await submit(makeForm({}));

    expect(window.location.pathname).toBe("/elsewhere");
    expect(window.__NEUTRON_DATA__).toBeUndefined();
  });

  it("swallows a non-ok response and still returns to idle", async () => {
    window.history.replaceState(null, "", "/form");
    const fetchMock = vi.fn(async () =>
      jsonResponse({ route: "unused" }, { ok: false })
    );
    vi.stubGlobal("fetch", fetchMock);

    const { submit, container } = mountPanel();
    await submit(makeForm({}));
    await flush();

    expect(window.__NEUTRON_DATA__).toBeUndefined();
    expect(container.querySelector(".nav")?.textContent).toBe("idle");
  });

  it("falls back to a popstate render for a non-JSON response", async () => {
    window.history.replaceState(null, "", "/form");
    const fetchMock = vi.fn(async () =>
      jsonResponse("<html></html>", {
        headers: new Headers({ "content-type": "text/html" }),
      })
    );
    vi.stubGlobal("fetch", fetchMock);

    let popstates = 0;
    const onPop = (): void => {
      popstates++;
    };
    window.addEventListener("popstate", onPop);

    const { submit } = mountPanel();
    await submit(makeForm({}));

    expect(popstates).toBe(1);
    window.removeEventListener("popstate", onPop);
  });
});

describe("useBeforeUnload", () => {
  it("listens while mounted and stops after unmount", async () => {
    const seen: Event[] = [];
    function Probe(): VNode<any> {
      hooks.useBeforeUnload((event) => {
        seen.push(event);
      });
      return h("p", null, "x");
    }
    const container = mount(withProviders(h(Probe, null)));
    await flushEffects(); // let the subscription effect register

    window.dispatchEvent(new Event("beforeunload"));
    expect(seen.length).toBe(1);

    render(null, container);
    await flushEffects(); // let the cleanup effect run
    window.dispatchEvent(new Event("beforeunload"));
    expect(seen.length).toBe(1);
  });
});

describe("useBlocker", () => {
  it("registers a working blocker without CommonJS require (ESM bundle)", async () => {
    // useBlocker used to call require('./navigate.js') inside its effect —
    // a ReferenceError in any ESM/browser bundle, so no blocker was ever
    // registered and navigations sailed through. Registering the blocker is
    // the observable: navigate() must emit neutron:navigation-blocked.
    window.history.replaceState(null, "", "/form");

    const blocked: Event[] = [];
    const onBlocked = (event: Event): void => {
      blocked.push(event);
    };
    window.addEventListener("neutron:navigation-blocked", onBlocked);

    function Probe(): VNode<any> {
      hooks.useBlocker(true);
      return h("p", null, "x");
    }
    const container = mount(withProviders(h(Probe, null)));
    await flushEffects(); // let the effect register the blocker

    navigate("/away");

    expect(blocked.length).toBe(1);
    expect(window.location.pathname).toBe("/form");

    window.removeEventListener("neutron:navigation-blocked", onBlocked);
    render(null, container);
    await flushEffects();
  });
});

describe("useSubmit error state", () => {
  function mountPanel(): {
    submit: (
      form: HTMLFormElement | FormData,
      options?: hooks.SubmitOptions
    ) => Promise<void>;
    error: () => hooks.SubmitError | null;
    container: HTMLDivElement;
  } {
    const ref: {
      submit?: (
        form: HTMLFormElement | FormData,
        options?: hooks.SubmitOptions
      ) => Promise<void>;
      error?: hooks.SubmitError | null;
    } = {};
    function Panel(): VNode<any> {
      const { submit, error } = hooks.useSubmit();
      ref.submit = submit;
      ref.error = error;
      return h("p", { class: "err" }, error ? `${error.status}` : "none");
    }
    const container = mount(withProviders(h(Panel, null)));
    return { submit: ref.submit!, error: () => ref.error ?? null, container };
  }

  function errorResponse(status: number, body: string): Response {
    return {
      ok: false,
      status,
      statusText: "Server Error",
      redirected: false,
      url: "http://localhost/unused",
      headers: new Headers({ "content-type": "application/json" }),
      json: async () => body,
    } as Response;
  }

  it("a non-ok response is surfaced as an error state, not silence", async () => {
    window.history.replaceState(null, "", "/form");
    const fetchMock = vi.fn(async () => errorResponse(500, "boom"));
    vi.stubGlobal("fetch", fetchMock);
    const form = makeForm({ title: "hello" });

    const { submit, error, container } = mountPanel();
    await submit(form);
    await flush();

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(error()).not.toBeNull();
    expect(error()!.status).toBe(500);
    expect(container.textContent ?? "").toContain("500");
  });

  it("a network failure is surfaced as a status-0 error", async () => {
    window.history.replaceState(null, "", "/form");
    const fetchMock = vi.fn(async () => {
      throw new TypeError("network down");
    });
    vi.stubGlobal("fetch", fetchMock);
    const form = makeForm({ title: "hello" });

    const { submit, error } = mountPanel();
    // The rejection still propagates to the direct caller; the hook records
    // it as a status-0 error for everyone rendering off the state.
    await expect(submit(form)).rejects.toThrow("network down");
    await flush();

    expect(error()).not.toBeNull();
    expect(error()!.status).toBe(0);
    expect(error()!.message).toContain("network down");
  });

  it("a successful submit clears a previous error", async () => {
    window.history.replaceState(null, "", "/form");
    let fail = true;
    const fetchMock = vi.fn(async () =>
      fail
        ? errorResponse(500, "boom")
        : jsonResponse({ "route:/form": { ok: true } })
    );
    vi.stubGlobal("fetch", fetchMock);
    const form = makeForm({ title: "hello" });

    const { submit, error } = mountPanel();
    await submit(form);
    await flush();
    expect(error()?.status).toBe(500);

    fail = false;
    await submit(form);
    await flush();
    expect(error()).toBeNull();
  });
});
