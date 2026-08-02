import { describe, it, expect, beforeEach, vi, afterEach } from "vitest";

import {
  PREFETCH_TTL_MS,
  clearPrefetchCache,
  hasFreshPrefetch,
  storePrefetch,
  takePrefetch,
} from "./prefetch-cache.js";

describe("prefetch cache", () => {
  beforeEach(() => {
    (globalThis as { window?: unknown }).window ??= globalThis;
    clearPrefetchCache();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("returns what was stored", () => {
    storePrefetch("/a", { root: { title: "A" } });
    expect(takePrefetch("/a")).toEqual({ root: { title: "A" } });
  });

  it("returns null for a URL that was never warmed", () => {
    expect(takePrefetch("/never")).toBeNull();
  });

  // A warmed payload answers exactly one navigation. Keeping it would make the
  // second visit to a URL render the first visit's data.
  it("consumes the entry on read", () => {
    storePrefetch("/a", { root: 1 });
    expect(takePrefetch("/a")).toEqual({ root: 1 });
    expect(takePrefetch("/a")).toBeNull();
  });

  // The regression for the real defect: useSubmit wrote post-mutation data
  // here with no expiry, and handleNavigation served it as a prefetch. A
  // navigation away and back re-rendered that snapshot with no network,
  // forever — every later change stayed invisible until a hard reload.
  it("expires an entry rather than serving it forever", () => {
    vi.useFakeTimers();
    storePrefetch("/dashboard", { root: { total: 1 } });

    vi.advanceTimersByTime(PREFETCH_TTL_MS - 1);
    expect(hasFreshPrefetch("/dashboard")).toBe(true);

    vi.advanceTimersByTime(2);
    expect(hasFreshPrefetch("/dashboard")).toBe(false);
    expect(takePrefetch("/dashboard")).toBeNull();
  });

  it("bounds memory on link-dense pages", () => {
    for (let i = 0; i < 64; i++) {
      storePrefetch(`/p/${i}`, { root: i });
    }
    // The oldest are evicted; the most recent survive.
    expect(takePrefetch("/p/0")).toBeNull();
    expect(takePrefetch("/p/63")).toEqual({ root: 63 });
  });

  it("keeps the legacy global in sync so external reads still work", () => {
    storePrefetch("/a", { root: 1 });
    expect(window.__NEUTRON_PREFETCH_CACHE__?.["/a"]).toEqual({ root: 1 });
    takePrefetch("/a");
    expect(window.__NEUTRON_PREFETCH_CACHE__?.["/a"]).toBeUndefined();
  });

  it("refreshing a URL does not leave the stale copy behind", () => {
    storePrefetch("/a", { root: "old" });
    storePrefetch("/a", { root: "new" });
    expect(takePrefetch("/a")).toEqual({ root: "new" });
    expect(takePrefetch("/a")).toBeNull();
  });
});
