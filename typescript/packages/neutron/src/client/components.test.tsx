// @vitest-environment happy-dom
import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { h, render } from "preact";
import { Form, Link, NavLink, prefetch } from "./components.js";
import type { FormRenderState } from "./components.js";
import { ViewTransitions } from "./view-transitions.js";
import { ScrollReveal } from "./scroll-reveal.js";
import type { JsxChildren } from "./jsx-agnostic.js";
import { Island } from "./island.js";
import { clearPrefetchCache, hasFreshPrefetch, takePrefetch } from "./prefetch-cache.js";

const mountedContainers: HTMLDivElement[] = [];

function mount(node: h.JSX.Element): HTMLDivElement {
  const container = document.createElement("div");
  document.body.appendChild(container);
  mountedContainers.push(container);
  render(node as never, container);
  return container;
}

function flush(): Promise<void> {
  return new Promise((r) => setTimeout(r, 0));
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

function jsonResponse(body: unknown): Response {
  return {
    ok: true,
    redirected: false,
    url: "http://localhost/unused",
    headers: new Headers({ "content-type": "application/json" }),
    json: async () => body,
  } as Response;
}

beforeEach(() => {
  window.history.replaceState(null, "", "/");
  document.body.innerHTML = "";
  mountedContainers.length = 0;
  clearPrefetchCache();
  delete (window as unknown as Record<string, unknown>).__NEUTRON_PREFETCH_CACHE__;
  (window as unknown as Record<string, unknown>).__NEUTRON_ROUTER_ACTIVE__ = true;
});

afterEach(() => {
  for (const container of mountedContainers) {
    if (container.isConnected) render(null, container);
  }
  document.body.innerHTML = "";
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("prefetch", () => {
  it("warms the cache that navigation consumes (takePrefetch)", async () => {
    const fetchMock = vi.fn(async () => jsonResponse({ "route:/target": { title: "T" } }));
    vi.stubGlobal("fetch", fetchMock);

    await prefetch("/target");

    expect(fetchMock).toHaveBeenCalledTimes(1);
    // The raw window global alone is not a navigation cache: handleNavigation
    // reads takePrefetch(). A prefetch that nothing consumes is a double fetch.
    expect(takePrefetch("/target")).toEqual({ "route:/target": { title: "T" } });
  });

  it("skips the network when a fresh entry already exists", async () => {
    const fetchMock = vi.fn(async () => jsonResponse({ "route:/target": { title: "T" } }));
    vi.stubGlobal("fetch", fetchMock);

    await prefetch("/target");
    await prefetch("/target");

    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});

describe("Form", () => {
  it("stores post-mutation data in the expiring prefetch cache, not the raw global", async () => {
    window.history.replaceState(null, "", "/form");
    const fetchMock = vi.fn(async () => jsonResponse({ "route:/form": { ok: true } }));
    vi.stubGlobal("fetch", fetchMock);

    mount(h(Form, { method: "post" }, h("input", { name: "title", value: "hello" })));
    const form = document.querySelector("form") as HTMLFormElement;
    form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    await flush();
    await flush();

    expect(fetchMock).toHaveBeenCalledTimes(1);
    // The documented contract (prefetch-cache.ts): post-mutation payloads go
    // through storePrefetch so they expire and are consumed on read, instead
    // of living in the raw global forever.
    expect(hasFreshPrefetch("/form")).toBe(true);
    expect(takePrefetch("/form")).toEqual({ "route:/form": { ok: true } });
  });
});

describe("NavLink", () => {
  async function settleEffects(): Promise<void> {
    // Preact schedules passive effects on requestAnimationFrame in
    // happy-dom; a bare setTimeout(0) can run before them.
    await new Promise<void>((resolve) => requestAnimationFrame(() => resolve()));
    await flush();
    await flush();
  }

  it("is active on subpaths for to='/'", async () => {
    window.history.replaceState(null, "", "/blog/post");
    mount(h(NavLink, { to: "/", activeClass: "active" }, "Home"));
    await settleEffects();

    const anchor = document.querySelector("a");
    expect(anchor?.className).toContain("active");
  });

  it("is not active on unrelated paths", async () => {
    window.history.replaceState(null, "", "/shop");
    mount(h(NavLink, { to: "/blog", activeClass: "active" }, "Blog"));
    await settleEffects();

    const anchor = document.querySelector("a");
    expect(anchor?.className).not.toContain("active");
  });

  it("is active on exact match and subpaths for a non-root to", async () => {
    window.history.replaceState(null, "", "/blog/post");
    mount(h(NavLink, { to: "/blog", activeClass: "active" }, "Blog"));
    await settleEffects();

    const anchor = document.querySelector("a");
    expect(anchor?.className).toContain("active");
  });
});

describe("Form error state", () => {
  function errorResponse(status: number): Response {
    return {
      ok: false,
      status,
      statusText: "Server Error",
      redirected: false,
      url: "http://localhost/unused",
      headers: new Headers(),
      json: async () => ({}),
    } as Response;
  }

  it("reports a failed submission through onError and data-submit-error", async () => {
    window.history.replaceState(null, "", "/form");
    const fetchMock = vi.fn(async () => errorResponse(500));
    vi.stubGlobal("fetch", fetchMock);
    const onError = vi.fn();

    mount(h(Form, { method: "post", onError }, h("input", { name: "t", value: "x" })));
    const form = document.querySelector("form") as HTMLFormElement;
    form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    await flush();
    await flush();

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(onError).toHaveBeenCalledTimes(1);
    expect((onError.mock.calls[0][0] as { status: number }).status).toBe(500);
    expect(form.getAttribute("data-submit-error")).toBe("500");
  });

  it("function children receive the live error state", async () => {
    window.history.replaceState(null, "", "/form");
    let fail = true;
    const fetchMock = vi.fn(async () =>
      fail ? errorResponse(422) : jsonResponse({ "route:/form": { ok: true } })
    );
    vi.stubGlobal("fetch", fetchMock);

    mount(
      h(
        Form,
        { method: "post" },
        (state: { submitting: boolean; error: { status: number } | null }) =>
          h("p", { class: "state" }, state.error ? `err ${state.error.status}` : "ok")
      )
    );
    const form = document.querySelector("form") as HTMLFormElement;

    form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    await flush();
    await flush();
    expect(document.querySelector(".state")?.textContent).toBe("err 422");

    fail = false;
    form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    await flush();
    await flush();
    expect(document.querySelector(".state")?.textContent).toBe("ok");
    expect(form.hasAttribute("data-submit-error")).toBe(false);
  });
});

// Preact-side counterpart of type-fixtures/react-island/: this file is
// compiled by the package build (jsxImportSource: preact), so the JSX usage
// below pins that the JSX-agnostic return type did not make <Island> less
// usable — or less typed — on the preact side it was written for.
describe("Island (preact JSX)", () => {
  it("renders children through to the islanded component inside its marker", () => {
    function Slot(props: { children?: preact.ComponentChildren }) {
      return h("section", { "data-slot": "yes" }, props.children);
    }

    mount(
      <Island component={Slot} client="load">
        <p>fallback until hydration</p>
      </Island>
    );

    const marker = document.querySelector("neutron-island");
    expect(marker).not.toBeNull();
    expect(marker!.getAttribute("data-client")).toBe("load");
    expect(marker!.getAttribute("data-component")).toBe("Slot");
    // Children are ordinary props: they render through the islanded component.
    const slot = marker!.querySelector("section[data-slot='yes']");
    expect(slot?.querySelector("p")?.textContent).toBe("fallback until hydration");
  });
});

// Preact-side counterpart of type-fixtures/react-nav-form/: the navigation/
// form components (and ViewTransitions/ScrollReveal) now return `any`
// elements so React-typed JSX can use them too. This file is compiled by the
// package build (jsxImportSource: preact), so the JSX below pins that preact
// DX and prop checking did not regress — including the @ts-expect-error,
// which fails the build if a mistyped prop stops being rejected.
describe("navigation/form components (preact JSX)", () => {
  it("still rejects unknown props (props stay fully typed)", () => {
    // @ts-expect-error wrongProp is not a LinkProps member
    return <Link to="/x" wrongProp={1}>broken</Link>;
  });

  it("renders Form, Link, NavLink, ViewTransitions and ScrollReveal through preact JSX", () => {
    window.history.replaceState(null, "", "/blog/post");

    mount(
      <div>
        <Link to="/elsewhere" prefetch="none">Elsewhere</Link>
        <NavLink to="/blog" end={false} activeClass="is-active">Blog</NavLink>
      </div>
    );

    const link = document.querySelectorAll("a")[0] as HTMLAnchorElement;
    expect(link.getAttribute("href")).toBe("/elsewhere");

    // Script-bearing components render into a DETACHED container: happy-dom
    // evaluates inline scripts when they connect to the document, and the
    // bootstraps are written for real browsers. The contract to pin here is
    // what preact JSX renders, not script execution.
    const detached = document.createElement("div");
    render(
      <div>
        <ViewTransitions />
        <ScrollReveal injectCSS={false} />
      </div>,
      detached
    );
    expect(detached.querySelector("style")).not.toBeNull();
    expect(detached.querySelector("script")).not.toBeNull();
    expect(detached.querySelectorAll("script").length).toBeGreaterThanOrEqual(2);
    render(null, detached);
  });

  it("submits a JSX-mounted Form through the client router path", async () => {
    window.history.replaceState(null, "", "/form");
    const fetchMock = vi.fn(async () => jsonResponse({ "route:/form": { ok: true } }));
    vi.stubGlobal("fetch", fetchMock);

    mount(
      <Form method="post">
        <input name="title" value="hello" />
      </Form>
    );
    const form = document.querySelector("form") as HTMLFormElement;
    form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    await flush();
    await flush();

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(takePrefetch("/form")).toEqual({ "route:/form": { ok: true } });
  });

  it("passes function children the live submit state through JSX", async () => {
    window.history.replaceState(null, "", "/form");
    let seen: FormRenderState | null = null;

    mount(
      <Form method="get">
        {(state) => {
          seen = state;
          return h("p", { class: "state" }, state.submitting ? "busy" : "idle");
        }}
      </Form>
    );

    expect(document.querySelector(".state")?.textContent).toBe("idle");
    expect(seen).not.toBeNull();
    expect(seen!.submitting).toBe(false);
  });

  it("renders JsxChildren-typed children from preact JSX (both-worlds layout shape)", () => {
    function Layout(props: { children?: JsxChildren }) {
      return h("main", { "data-layout": "yes" }, props.children);
    }

    mount(
      <Layout>
        <p data-child="a">first</p>
        {"text child"}
      </Layout>
    );

    const layout = document.querySelector("main[data-layout='yes']");
    expect(layout?.querySelector("p")?.textContent).toBe("first");
    expect(layout?.textContent).toContain("text child");
  });
});
