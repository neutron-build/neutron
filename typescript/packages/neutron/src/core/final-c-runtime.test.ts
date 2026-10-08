import { csrfMiddleware } from "../server/csrf.js";
import { describe, expect, it, vi } from "vitest";
import { renderAppRoute } from "./render-app-route.js";
import { assertStaticRoutesUngated } from "./render-static.js";
import { mutableResponse } from "./response.js";
import { createMemoryLoaderCacheStore } from "../server/cache-store.js";
import { capRequestBody } from "../server/input-limits.js";
import { installTransportPeer, transportPeer } from "../server/peer.js";
import { captureCacheBody } from "../server/cache-capture.js";
import type { Route, RouteModule, MiddlewareFn } from "./types.js";

const route: Route = { id: "r", path: "/", file: "r.ts", params: [], parentId: null, config: { mode: "app" } };
function render(module: RouteModule, extra: Record<string, any> = {}) {
  return renderAppRoute(new Request("https://example.test/"), { route, params: {}, layouts: [] }, new Map([[route.id, module]]), {
    clientEntryScriptSrc: null, loaderDataCache: createMemoryLoaderCacheStore(), requestTrace: { requestId: "1", method: "GET", pathname: "/" }, ...extra,
  });
}
describe("final C runtime contracts", () => {
  it("TS-F01 rejects global gates before loading/writing any static route", async () => {
    const load = vi.fn();
    await expect(assertStaticRoutesUngated([{ ...route, config: { mode: "static" } }], { loadRouteModule: load, getLayoutChain: () => [], globalMiddleware: true })).rejects.toThrow("global middleware");
    expect(load).not.toHaveBeenCalled();
  });
  it.each(["private", "no-store", "cookie"])("TS-F04 publishes only final %s policy", async policy => {
    const stored: Response[] = [];
    const middleware: MiddlewareFn = async (_r, _c, next) => {
      const response = mutableResponse(await next());
      response.headers.set(policy === "cookie" ? "Set-Cookie" : "Cache-Control", policy === "cookie" ? "session=unique" : policy);
      return response;
    };
    await render({ loader: async () => new Response("form"), middleware: Object.assign(middleware, { sharedCacheSafe: true as const }) }, { responseCache: { enabled: true, read: async () => null, store: (r: Response) => stored.push(r) } });
    expect(stored).toHaveLength(1);
    expect(stored[0].headers.get(policy === "cookie" ? "Set-Cookie" : "Cache-Control")).toBe(policy === "cookie" ? "session=unique" : policy);
  });
  it("TS-F04 never reuses request-scoped tokens", async () => {
    const read = vi.fn(async () => new Response("stale")); const store = vi.fn();
    const response = await render({ loader: async ({ context }) => new Response(String(context.csrfToken)), middleware: async (_r, c, next) => { c.csrfToken = "fresh"; return next(); } }, { responseCache: { enabled: true, read, store } });
    expect(await response.text()).toBe("fresh"); expect(read).not.toHaveBeenCalled(); expect(store).not.toHaveBeenCalled();
  });
  it("TS-F09 rewraps immutable native redirects without losing cookies or status", () => {
    const response = mutableResponse(Response.redirect("https://example.test/destination", 307));
    response.headers.append("Set-Cookie", "a=1"); response.headers.append("Set-Cookie", "b=2");
    expect(response.status).toBe(307); expect(response.headers.get("Location")).toContain("destination"); expect(response.headers.getSetCookie()).toEqual(["a=1", "b=2"]);
  });
  it("TS-F15 retains abort and immutable socket identity", async () => {
    const controller = new AbortController();
    const original = new Request("https://example.test/", { method: "POST", body: "body", signal: controller.signal });
    installTransportPeer(original, "127.0.0.1");
    const wrapped = capRequestBody(original, 20); controller.abort();
    expect(wrapped.signal.aborted).toBe(true); expect(transportPeer(wrapped)).toEqual({ remoteAddress: "127.0.0.1" });
    await expect(wrapped.text()).rejects.toThrow("aborted");
  });
  it.each(["return", "throw"])("TS-F18 records exactly one terminal loader event for %s Response", async mode => {
    const start = vi.fn(); const end = vi.fn();
    const response = await render({ default: () => null, loader: async () => { const r = Response.redirect("https://example.test/elsewhere"); if (mode === "throw") throw r; return r; } }, { hooks: { onLoaderStart: start, onLoaderEnd: end } });
    expect(response.status).toBe(302); expect(start).toHaveBeenCalledTimes(1); expect(end).toHaveBeenCalledTimes(1); expect(end.mock.calls[0][0].outcome).toBe("response");
  });
  it("TS-F08 rejects over-cap capture without draining the client stream", async () => {
    let reads = 0;
    const response = new Response(new ReadableStream({ pull(c) { reads++; c.enqueue(new Uint8Array(8)); } }));
    expect(await captureCacheBody(response, 10, 100)).toBeNull(); expect(reads).toBeLessThan(10);
    void response.body!.cancel();
  });
  it("TS-F08 releases capture of neverending streams within the deadline", async () => {
    const response = new Response(new ReadableStream({ pull() { return new Promise(() => {}); } }));
    const start = Date.now(); expect(await captureCacheBody(response, 10, 30)).toBeNull(); expect(Date.now() - start).toBeLessThan(500);
    void response.body!.cancel();
  });
  it("TS-F15 preserves 413 when an SSR action consumes an oversized stream", async () => {
    const request = capRequestBody(new Request('https://example.test/', { method: 'POST', body: 'too large' }), 2);
    const response = await renderAppRoute(request, { route, params: {}, layouts: [] }, new Map([[route.id, { default: () => null, action: async ({ request }) => { await request.text(); return {}; } }]]), {
      clientEntryScriptSrc: null, loaderDataCache: createMemoryLoaderCacheStore(), requestTrace: { requestId: 'oversize', method: 'POST', pathname: '/' },
    });
    expect(response.status).toBe(413);
  });

  it("TS-F04 fresh CSRF visitors each receive matching distinct body/cookie tokens", async () => {
    const store = vi.fn(), read = vi.fn(async () => new Response('stale-token'));
    const module: RouteModule = { loader: async ({ context }) => new Response(String(context.csrfToken)), middleware: csrfMiddleware({ cookieOptions: { secure: false } }) };
    const values: string[] = [];
    for (let i = 0; i < 2; i++) {
      const response = await render(module, { responseCache: { enabled: true, read, store } });
      const token = await response.text();
      expect(response.headers.get('Set-Cookie')).toContain('_csrf=' + token);
      values.push(token);
    }
    expect(values[0]).not.toBe(values[1]); expect(read).not.toHaveBeenCalled(); expect(store).not.toHaveBeenCalled();
  });

});

it('TS-F15 abort cancels the source once and releases a pending capped reader', async () => {
  const abort = new AbortController();
  const cancel = vi.fn();
  const body = new ReadableStream<Uint8Array>({ pull() { return new Promise(() => {}); }, cancel });
  const original = new Request('https://example.test/', { method: 'POST', body, signal: abort.signal, duplex: 'half' } as RequestInit);
  const capped = capRequestBody(original, 10);
  const reading = capped.text();
  abort.abort(new Error('transport disconnected'));
  await expect(reading).rejects.toThrow('transport disconnected');
  await Promise.resolve(); await Promise.resolve();
  expect(cancel).toHaveBeenCalledTimes(1);
  expect(body.locked).toBe(false);
});

it('TS-F08 monotonic deadline also bounds an endless stream of empty chunks', async () => {
  const response = new Response(new ReadableStream<Uint8Array>({ pull(controller) { controller.enqueue(new Uint8Array()); } }));
  const started = performance.now();
  expect(await captureCacheBody(response, 10, 30)).toBeNull();
  expect(performance.now() - started).toBeLessThan(500);
  void response.body!.cancel();
});

it.each(['private', 'no-store', 'cookie', 'late-context'])('O1 stages loader fills behind final %s policy', async policy => {
  const cache = createMemoryLoaderCacheStore();
  const get = vi.spyOn(cache, 'get'); const publish = vi.fn(cache.setIfGeneration!); cache.setIfGeneration = publish;
  const loader = vi.fn(async ({ request }: any) => ({ value: request.headers.get('x-fixture-principal') }));
  const middleware: MiddlewareFn = Object.assign(async (_r: Request, context: any, next: () => Promise<Response>) => {
    const result = mutableResponse(await next());
    if (policy === 'late-context') context.principal = 'late';
    else result.headers.set(policy === 'cookie' ? 'Set-Cookie' : 'Cache-Control', policy === 'cookie' ? 'session=unique' : policy);
    return result;
  }, { sharedCacheSafe: true as const });
  const cachedRoute = { ...route, config: { mode: 'app' as const, cache: { loaderMaxAge: 120 } } };
  for (const principal of ['alice', 'bob']) {
    const request = new Request('https://example.test/', { headers: { 'X-Neutron-Data': 'true', 'x-fixture-principal': principal } });
    const result = await renderAppRoute(request, { route: cachedRoute, params: {}, layouts: [] }, new Map([['r', { default: () => null, loader, middleware }]]), { clientEntryScriptSrc: null, loaderDataCache: cache, requestTrace: { requestId: principal, method: 'GET', pathname: '/' } });
    const { decodeSerializedPayload } = await import('./serialization.js');
    expect(decodeSerializedPayload<Record<string, unknown>>(await result.json()).r).toEqual({ value: principal });
  }
  expect(loader).toHaveBeenCalledTimes(2); expect(get).toHaveBeenCalledTimes(2); expect(publish).not.toHaveBeenCalled();
});
it('O1 undeclared global/layout middleware bypasses loader READ even with a warmed entry', async () => {
  const cache = createMemoryLoaderCacheStore();
  const get = vi.spyOn(cache, 'get'); const publish = vi.fn(cache.setIfGeneration!); cache.setIfGeneration = publish;
  const cachedRoute = { ...route, config: { mode: 'app' as const, cache: { loaderMaxAge: 120 } } };
  const loader = vi.fn(async () => ({ value: 'fresh' }));
  const module = { default: () => null, loader };
  const request = new Request('https://example.test/', { headers: { 'X-Neutron-Data': 'true' } });
  const options = { clientEntryScriptSrc: null, loaderDataCache: cache, requestTrace: { requestId: 'r', method: 'GET', pathname: '/' } };
  await renderAppRoute(request, { route: cachedRoute, params: {}, layouts: [] }, new Map([['r', module]]), options);
  get.mockClear(); publish.mockClear();
  const middleware: MiddlewareFn = async (_r, _c, next) => next();
  for (const global of [true, false]) {
    await renderAppRoute(request, { route: cachedRoute, params: {}, layouts: global ? [] : [{ ...route, id: 'layout', isLayout: true }] }, new Map<string, RouteModule>([['r', module], ['layout', { middleware }]]), { ...options, globalMiddleware: global ? [middleware] : [] });
  }
  expect(get).not.toHaveBeenCalled(); expect(publish).not.toHaveBeenCalled(); expect(loader).toHaveBeenCalledTimes(3);
});
it('O1 loader representations include full Accept and preserve warm public reuse', async () => {
  const cache = createMemoryLoaderCacheStore();
  const loader = vi.fn(async ({ request }: any) => ({ accept: request.headers.get('accept') }));
  const cachedRoute = { ...route, config: { mode: 'app' as const, cache: { loaderMaxAge: 120 } } };
  for (const accept of ['text/csv', 'application/xml', 'text/csv']) {
    const request = new Request('https://example.test/', { headers: { 'X-Neutron-Data': 'true', Accept: accept } });
    const result = await renderAppRoute(request, { route: cachedRoute, params: {}, layouts: [] }, new Map([['r', { default: () => null, loader }]]), { clientEntryScriptSrc: null, loaderDataCache: cache, requestTrace: { requestId: accept, method: 'GET', pathname: '/' } });
    const { decodeSerializedPayload } = await import('./serialization.js');
    expect(decodeSerializedPayload<Record<string, unknown>>(await result.json()).r).toEqual({ accept });
  }
  expect(loader).toHaveBeenCalledTimes(2);
});
