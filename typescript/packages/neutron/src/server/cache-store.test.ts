import { describe, expect, it } from "vitest";
import {
  createMemoryAppCacheStore,
  createMemoryLoaderCacheStore,
  normalizeCachePathname,
} from "./cache-store.js";

const encoder = new TextEncoder();

describe("memory app cache store", () => {
  it("stores and reads byte-exact entries", async () => {
    const store = createMemoryAppCacheStore();
    await store.set("html\nhttp://x\n/dashboard\n", {
      status: 200,
      statusText: "OK",
      headers: [["content-type", "text/html"]],
      body: encoder.encode("<h1>ok</h1>"),
      expiresAt: Date.now() + 5_000,
    });

    const hit = await store.get("html\nhttp://x\n/dashboard\n");
    expect(hit?.status).toBe(200);
    expect(Buffer.from(hit!.body).toString("utf-8")).toContain("ok");
  });

  it("round-trips non-UTF-8 bytes unchanged", async () => {
    const store = createMemoryAppCacheStore();
    const raw = Uint8Array.of(0x00, 0xff, 0x80, 0x41, 0xfe);
    await store.set("html\nhttp://x\n/bin\n", {
      status: 200,
      statusText: "OK",
      headers: [],
      body: raw,
      expiresAt: Date.now() + 5_000,
    });
    const hit = await store.get("html\nhttp://x\n/bin\n");
    expect(Array.from(hit!.body)).toEqual(Array.from(raw));
  });

  it("invalidates by normalized path, exactly", async () => {
    const store = createMemoryAppCacheStore();
    await store.set("html\nhttp://x\n/users\n?x=1", {
      status: 200,
      statusText: "OK",
      headers: [],
      body: encoder.encode("users"),
      expiresAt: Date.now() + 5_000,
    });
    await store.set("json\nhttp://x\n/users\n?x=1", {
      status: 200,
      statusText: "OK",
      headers: [],
      body: encoder.encode("{}"),
      expiresAt: Date.now() + 5_000,
    });
    // A sibling-prefixed path must NOT be swept by the /users invalidation.
    await store.set("html\nhttp://x\n/users/123\n", {
      status: 200,
      statusText: "OK",
      headers: [],
      body: encoder.encode("one user"),
      expiresAt: Date.now() + 5_000,
    });

    await store.deleteByPath("/users/");
    expect(await store.get("html\nhttp://x\n/users\n?x=1")).toBeNull();
    expect(await store.get("json\nhttp://x\n/users\n?x=1")).toBeNull();
    expect(await store.get("html\nhttp://x\n/users/123\n")).not.toBeNull();
  });

  it("normalizes traversal as whole segments, not substrings", () => {
    expect(normalizeCachePathname("/a..b")).toBe("/a..b");
    expect(normalizeCachePathname("/a/../b")).toBeNull();
    expect(normalizeCachePathname("/users/")).toBe("/users");
    expect(normalizeCachePathname("/100%25")).toBe("/100%");
  });
});

describe("memory loader cache store", () => {
  it("stores and invalidates by path", async () => {
    const store = createMemoryLoaderCacheStore();
    await store.set("/users::?page=1::routes/users::[]", {
      data: { ok: true },
      expiresAt: Date.now() + 5_000,
    });

    const before = await store.get("/users::?page=1::routes/users::[]");
    expect(before?.data).toEqual({ ok: true });

    await store.deleteByPath("/users");
    const after = await store.get("/users::?page=1::routes/users::[]");
    expect(after).toBeNull();
  });

  it("returns cloned data — mutating a read does not poison the store", async () => {
    const store = createMemoryLoaderCacheStore();
    await store.set("/a", {
      data: { profile: { role: "reader" } },
      expiresAt: Date.now() + 5_000,
    });
    const first = await store.get("/a");
    (first!.data as { profile: { role: string } }).profile.role = "writer";
    const second = await store.get("/a");
    expect((second!.data as { profile: { role: string } }).profile.role).toBe("reader");
  });

  it("evicts least-recently-used, not first-inserted (app store)", async () => {
    const store = createMemoryAppCacheStore({ maxEntries: 2 });
    const entry = (body: string) => ({
      status: 200,
      statusText: "OK",
      headers: [],
      body: encoder.encode(body),
      expiresAt: Date.now() + 5_000,
    });
    await store.set("html\nhttp://x\n/a\n", entry("a"));
    await store.set("html\nhttp://x\n/b\n", entry("b"));
    // Touch /a so /b becomes the least recently used.
    await store.get("html\nhttp://x\n/a\n");
    await store.set("html\nhttp://x\n/c\n", entry("c"));

    expect(await store.get("html\nhttp://x\n/a\n")).not.toBeNull();
    expect(await store.get("html\nhttp://x\n/b\n")).toBeNull();
    expect(await store.get("html\nhttp://x\n/c\n")).not.toBeNull();
  });

  it("evicts least-recently-used, not first-inserted (loader store)", async () => {
    const store = createMemoryLoaderCacheStore({ maxEntries: 2 });
    const entry = (data: unknown) => ({ data, expiresAt: Date.now() + 5_000 });
    await store.set("/a", entry(1));
    await store.set("/b", entry(2));
    await store.get("/a");
    await store.set("/c", entry(3));

    expect((await store.get("/a"))?.data).toBe(1);
    expect(await store.get("/b")).toBeNull();
    expect((await store.get("/c"))?.data).toBe(3);
  });
});
