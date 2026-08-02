// @vitest-environment happy-dom
import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";

import { incrementalPrefetch } from "./incremental-prefetch.js";
import { clearPrefetchCache, hasFreshPrefetch, takePrefetch } from "./prefetch-cache.js";

function jsonResponse(body: unknown, init: Partial<Response> = {}): Response {
  return {
    ok: true,
    redirected: false,
    headers: new Headers({ "content-type": "application/json" }),
    json: async () => body,
    ...init,
  } as Response;
}

describe("incrementalPrefetch", () => {
  beforeEach(() => {
    clearPrefetchCache();
    window.history.replaceState({}, "", "/");
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("warms the cache the navigation path reads", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => jsonResponse({ root: { title: "Engine" } }))
    );

    await incrementalPrefetch("/engine");

    expect(hasFreshPrefetch("/engine")).toBe(true);
    expect(takePrefetch("/engine")).toEqual({ root: { title: "Engine" } });
  });

  // The whole point: a warmed link must not cost a second request on click.
  it("does not re-fetch a URL that is already warm", async () => {
    const fetchMock = vi.fn(async () => jsonResponse({ root: 1 }));
    vi.stubGlobal("fetch", fetchMock);

    await incrementalPrefetch("/engine");
    await incrementalPrefetch("/engine");

    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("requests the same payload a navigation would", async () => {
    const fetchMock = vi.fn(async () => jsonResponse({ root: 1 }));
    vi.stubGlobal("fetch", fetchMock);

    await incrementalPrefetch("/engine");

    const [, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    const headers = init.headers as Record<string, string>;
    expect(headers["X-Neutron-Data"]).toBe("true");
    // Marks the request speculative so servers and proxies can tell it from a
    // user-initiated navigation.
    expect(headers.Purpose).toBe("prefetch");
  });

  // A redirect means the linked URL is not what the server will serve. Caching
  // the destination's payload under the link's URL would make the click render
  // a page the server would have refused for it — the auth-gated case.
  it("refuses to warm a URL that redirected", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => jsonResponse({ root: "secret" }, { redirected: true }))
    );

    await incrementalPrefetch("/admin");

    expect(hasFreshPrefetch("/admin")).toBe(false);
  });

  it("ignores a non-ok response instead of caching an error page", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => jsonResponse({ error: "nope" }, { ok: false }))
    );

    await incrementalPrefetch("/gone");

    expect(hasFreshPrefetch("/gone")).toBe(false);
  });

  it("ignores a non-JSON response", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        jsonResponse("<html></html>", {
          headers: new Headers({ "content-type": "text/html" }),
        })
      )
    );

    await incrementalPrefetch("/html");

    expect(hasFreshPrefetch("/html")).toBe(false);
  });

  // A speculative request that fails is not an error the user should see: the
  // click falls back to the normal fetch, which is the un-prefetched behavior.
  it("stays silent when the network fails", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => {
        throw new Error("offline");
      })
    );

    await expect(incrementalPrefetch("/engine")).resolves.toBeUndefined();
    expect(hasFreshPrefetch("/engine")).toBe(false);
  });

  it("does not spend requests when the user asked to save data", async () => {
    const fetchMock = vi.fn(async () => jsonResponse({ root: 1 }));
    vi.stubGlobal("fetch", fetchMock);
    Object.defineProperty(navigator, "connection", {
      value: { saveData: true },
      configurable: true,
    });

    await incrementalPrefetch("/engine");

    expect(fetchMock).not.toHaveBeenCalled();
    Object.defineProperty(navigator, "connection", {
      value: undefined,
      configurable: true,
    });
  });
});
