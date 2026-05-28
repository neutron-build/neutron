import { describe, expect, it } from "vitest";
import {
  compileRouteRules,
  resolveRouteRuleHeaders,
  resolveRouteRuleRedirect,
  resolveRouteRuleRewrite,
} from "./route-rules.js";

describe("route rules", () => {
  it("resolves redirects with params and preserves search when destination has no query", () => {
    const compiled = compileRouteRules({
      redirects: [
        {
          source: "/old/:slug",
          destination: "/new/:slug",
          permanent: true,
        },
      ],
    });

    const redirect = resolveRouteRuleRedirect(compiled, "/old/welcome", "?from=legacy");
    expect(redirect).toEqual({
      location: "/new/welcome?from=legacy",
      status: 308,
    });
  });

  it("neutralizes an open redirect from a protocol-relative param value", () => {
    const compiled = compileRouteRules({
      redirects: [
        { source: "/go/:dest*", destination: "/:dest*", permanent: false },
      ],
    });

    // Attacker supplies a protocol-relative target via the captured param.
    const redirect = resolveRouteRuleRedirect(compiled, "/go//evil.example/phish", "");
    expect(redirect?.location.startsWith("//")).toBe(false);
    expect(redirect?.location).toBe("/evil.example/phish");
  });

  it("neutralizes a tab/newline-smuggled protocol-relative redirect", () => {
    const compiled = compileRouteRules({
      redirects: [
        { source: "/go/:dest*", destination: "/:dest*", permanent: false },
      ],
    });
    // "/\t/evil.example" resolves to "//evil.example" after browsers strip TAB.
    const redirect = resolveRouteRuleRedirect(compiled, "/go/\t/evil.example", "");
    expect(redirect?.location).toBe("/evil.example");
  });

  it("preserves an intentionally absolute destination", () => {
    const compiled = compileRouteRules({
      redirects: [
        { source: "/ext", destination: "https://example.com/ok", permanent: false },
      ],
    });
    const redirect = resolveRouteRuleRedirect(compiled, "/ext", "");
    expect(redirect?.location).toBe("https://example.com/ok");
  });

  it("resolves rewrites and destination params", () => {
    const compiled = compileRouteRules({
      rewrites: [
        {
          source: "/docs/:path*",
          destination: "/content/:path*",
        },
      ],
    });

    const rewrite = resolveRouteRuleRewrite(compiled, "/docs/guides/getting-started");
    expect(rewrite).toEqual({
      pathname: "/content/guides/getting-started",
      matchedSource: "/docs/:path*",
    });
  });

  it("rejects patterns with multiple catch-all segments", () => {
    expect(() =>
      compileRouteRules({
        redirects: [
          { source: "/a/*/b/*", destination: "/c", permanent: false },
        ],
      })
    ).toThrow("catch-all");
  });

  it("matches header rules by source pattern", () => {
    const compiled = compileRouteRules({
      headers: [
        {
          source: "/users/:id",
          headers: {
            "X-Frame-Options": "DENY",
            "X-Env": "test",
          },
        },
      ],
    });

    const matches = resolveRouteRuleHeaders(compiled, "/users/123");
    expect(matches).toHaveLength(1);
    expect(matches[0].headers["X-Frame-Options"]).toBe("DENY");
    expect(matches[0].headers["X-Env"]).toBe("test");
  });
});
