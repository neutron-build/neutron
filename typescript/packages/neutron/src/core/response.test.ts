import { describe, it, expect } from "vitest";
import { redirect, json, isResponse, notFound } from "./response.js";

describe("response helpers", () => {
  it("redirect creates a Response with Location header", () => {
    const response = redirect("/login");
    
    expect(response.status).toBe(302);
    expect(response.headers.get("Location")).toBe("/login");
  });

  it("redirect accepts custom status code", () => {
    const response = redirect("/dashboard", 301);
    
    expect(response.status).toBe(301);
    expect(response.headers.get("Location")).toBe("/dashboard");
  });

  it("json creates a Response with JSON content", async () => {
    const data = { message: "Hello", count: 42 };
    const response = json(data);
    
    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe("application/json");
    
    const body = await response.json();
    expect(body).toEqual(data);
  });

  it("json accepts custom status code", async () => {
    const data = { error: "Not found" };
    const response = json(data, 404);
    
    expect(response.status).toBe(404);
    
    const body = await response.json();
    expect(body).toEqual(data);
  });

  it("isResponse returns true for Response objects", () => {
    expect(isResponse(new Response())).toBe(true);
    expect(isResponse(redirect("/"))).toBe(true);
    expect(isResponse(json({}))).toBe(true);
  });

  // @hono/node-server replaces globalThis.Response. Response.json() (and the
  // other static factories) still build NATIVE instances, which fail
  // `instanceof` against the replacement — the cause of an action's
  // `Response.json(..., { status: 201 })` being served as 200 `{}`.
  it("isResponse recognizes native and @hono/node-server Responses alike", async () => {
    const nativeResponse = globalThis.Response;
    const nativeRequest = globalThis.Request;
    const { getRequestListener } = await import("@hono/node-server");
    getRequestListener(() => new Response());
    try {
      expect(globalThis.Response).not.toBe(nativeResponse);
      const fromFactory = Response.json({ ok: true }, { status: 201 });
      const fromCtor = new Response("x", { status: 201 });
      expect(fromFactory instanceof Response).toBe(false);
      expect(isResponse(fromFactory)).toBe(true);
      expect(isResponse(fromCtor)).toBe(true);
      expect(isResponse(Response.redirect("http://localhost/", 302))).toBe(true);
      expect(isResponse(new nativeResponse(null, { status: 204 }))).toBe(true);
      expect(isResponse({ status: 201, headers: new Headers() })).toBe(false);
    } finally {
      Object.defineProperty(globalThis, "Response", { value: nativeResponse });
      Object.defineProperty(globalThis, "Request", { value: nativeRequest });
    }
  });

  it("isResponse returns false for non-Response objects", () => {
    expect(isResponse(null)).toBe(false);
    expect(isResponse(undefined)).toBe(false);
    expect(isResponse({})).toBe(false);
    expect(isResponse("response")).toBe(false);
    expect(isResponse(200)).toBe(false);
  });

  // These asserted the old contract, where notFound() returned the bare string
  // "Not Found" with no content type — which is what browsers rendered as an
  // unstyled page. It now returns an HTML document; the message is still the
  // caller's, it is just no longer the entire response.
  it("notFound returns 404 as an HTML document", async () => {
    const response = notFound();
    expect(response.status).toBe(404);
    expect(response.headers.get("Content-Type")).toContain("text/html");
    expect(await response.text()).toMatch(/^<!doctype html>/i);
  });

  it("notFound carries a custom message into the document", async () => {
    const response = notFound("Page not found");
    expect(response.status).toBe(404);
    expect(await response.text()).toContain("Page not found");
  });

  it("notFound escapes the message with the shared escapeHtml (including apostrophes)", async () => {
    const response = notFound("it's <over>");
    const body = await response.text();
    expect(body).toContain("it&#39;s &lt;over&gt;");
    expect(body).not.toContain("it's <over>");
  });
});
