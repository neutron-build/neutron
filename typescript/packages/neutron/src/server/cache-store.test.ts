import { describe, expect, it } from "vitest";
import {
  createMemoryAppCacheStore,
  createMemoryLoaderCacheStore,
} from "./cache-store.js";

describe("memory app cache store", () => {
  it("stores and reads entries", async () => {
    const store = createMemoryAppCacheStore();
    await store.set("html:/dashboard", {
      status: 200,
      statusText: "OK",
      headers: [["content-type", "text/html"]],
      body: "<h1>ok</h1>",
      expiresAt: Date.now() + 5_000,
    });

    const hit = await store.get("html:/dashboard");
    expect(hit?.status).toBe(200);
    expect(hit?.body).toContain("ok");
  });

  it("invalidates by normalized path", async () => {
    const store = createMemoryAppCacheStore();
    await store.set("html:/users?x=1", {
      status: 200,
      statusText: "OK",
      headers: [],
      body: "users",
      expiresAt: Date.now() + 5_000,
    });
    await store.set("json:/users?x=1", {
      status: 200,
      statusText: "OK",
      headers: [],
      body: "{}",
      expiresAt: Date.now() + 5_000,
    });

    await store.deleteByPath("/users/");
    expect(await store.get("html:/users?x=1")).toBeNull();
    expect(await store.get("json:/users?x=1")).toBeNull();
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

  it("evicts least-recently-used, not first-inserted (app store)", async () => {
    const store = createMemoryAppCacheStore({ maxEntries: 2 });
    const entry = (body: string) => ({
      status: 200,
      statusText: "OK",
      headers: [],
      body,
      expiresAt: Date.now() + 5_000,
    });
    await store.set("html:/a", entry("a"));
    await store.set("html:/b", entry("b"));
    // Touch /a so /b becomes the least recently used.
    await store.get("html:/a");
    await store.set("html:/c", entry("c"));

    expect(await store.get("html:/a")).not.toBeNull();
    expect(await store.get("html:/b")).toBeNull();
    expect(await store.get("html:/c")).not.toBeNull();
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
