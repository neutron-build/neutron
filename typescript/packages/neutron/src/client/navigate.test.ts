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
});
