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
});
