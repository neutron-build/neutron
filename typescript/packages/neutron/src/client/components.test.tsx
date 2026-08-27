// @vitest-environment happy-dom
import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { h, render } from "preact";
import { Form, NavLink, prefetch } from "./components.js";
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
