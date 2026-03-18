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
