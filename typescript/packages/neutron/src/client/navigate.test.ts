import { describe, it, expect } from "vitest";
import { matchRoute, extractParams } from "./navigate.js";

describe("navigate matchRoute", () => {
  it("matches wildcard route across multiple segments", () => {
    const routes = ["/files/*", "/about"];
    expect(matchRoute("/files/a/b/c", routes)).toBe("/files/*");
  });

  it("matches static and param routes correctly", () => {
    const routes = ["/about", "/users/:id", "/files/*"];
    expect(matchRoute("/about", routes)).toBe("/about");
    expect(matchRoute("/users/42", routes)).toBe("/users/:id");
    expect(matchRoute("/unknown", routes)).toBeNull();
  });

  it("extractParams captures wildcard remainder", () => {
    const params = extractParams("/files/*", "/files/a/b/c");
    expect(params["*"]).toBe("a/b/c");
  });

  it("extractParams captures named params", () => {
    const params = extractParams("/users/:id", "/users/42");
    expect(params.id).toBe("42");
  });

  it("does not match a wildcard route when the remainder is empty", () => {
    // The server trie requires at least one segment after the wildcard
    // (router.ts: `if (!value) continue`); the client must agree, or the
    // client router claims a path the server 404s.
    expect(matchRoute("/files", ["/files/*", "/about"])).toBeNull();
    expect(matchRoute("/files/a", ["/files/*", "/about"])).toBe("/files/*");
  });

  it("extractParams decodes percent-encoded segments like the server", () => {
    const params = extractParams("/blog/:slug", "/blog/hello%20world");
    expect(params.slug).toBe("hello world");
  });

  it("extractParams decodes the wildcard remainder", () => {
    const params = extractParams("/files/*", "/files/a%20b/c");
    expect(params["*"]).toBe("a b/c");
  });
});
