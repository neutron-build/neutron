import { describe, expect, it } from "vitest";
import { createI18nMiddleware, resolveLocalePath, withLocalePath } from "./i18n.js";

describe("i18n utilities", () => {
  const options = {
    locales: ["en", "es", "fr"],
    defaultLocale: "en",
    strategy: "prefix-except-default" as const,
  };

  it("resolves prefixed locale", () => {
    const result = resolveLocalePath("/es/pricing", options);
    expect(result.locale).toBe("es");
    expect(result.pathWithoutLocale).toBe("/pricing");
    expect(result.redirectTo).toBeUndefined();
  });

  it("resolves default locale without prefix", () => {
    const result = resolveLocalePath("/pricing", options);
    expect(result.locale).toBe("en");
    expect(result.pathWithoutLocale).toBe("/pricing");
  });

  it("builds locale-aware paths", () => {
    expect(withLocalePath("/pricing", "es", options)).toBe("/es/pricing");
    expect(withLocalePath("/pricing", "en", options)).toBe("/pricing");
  });

  it("middleware annotates context", async () => {
    const middleware = createI18nMiddleware(options);
    const request = new Request("https://example.com/es/docs");
    const context: Record<string, unknown> = {};
    const response = await middleware(request, context, async () => new Response("ok"));

    expect(response.status).toBe(200);
    expect(context.locale).toBe("es");
    expect(context.pathWithoutLocale).toBe("/docs");
  });
});

describe("createI18nMiddleware redirect response", () => {
  // `Response.redirect()` returns a response whose header guard is "immutable".
  // Every middleware in the documented standard stack sets a header on the way
  // out, so a redirect built that way throws `TypeError: immutable` and reaches
  // the client as an empty-body 500. This asserts the property that matters --
  // a caller can still set a header -- rather than asserting how the response
  // was constructed.
  it("returns a redirect whose headers a later middleware can still set", async () => {
    const mw = createI18nMiddleware({
      locales: ["en", "es"],
      defaultLocale: "en",
      strategy: "prefix-except-default",
    });
    const request = new Request("http://example.test/en/pricing");
    const res = (await mw(request, {} as never, async () => new Response("next"))) as Response;

    expect(res.status).toBe(307);
    expect(res.headers.get("location")).toBe("/pricing");
    expect(() => res.headers.set("x-request-id", "abc")).not.toThrow();
    expect(res.headers.get("x-request-id")).toBe("abc");
  });

  it("preserves the query string across the canonical redirect", async () => {
    const mw = createI18nMiddleware({
      locales: ["en", "es"],
      defaultLocale: "en",
      strategy: "prefix-except-default",
    });
    const request = new Request("http://example.test/en/pricing?plan=team");
    const res = (await mw(request, {} as never, async () => new Response("next"))) as Response;

    expect(res.headers.get("location")).toBe("/pricing?plan=team");
  });
});
