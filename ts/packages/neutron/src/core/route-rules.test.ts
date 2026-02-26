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
