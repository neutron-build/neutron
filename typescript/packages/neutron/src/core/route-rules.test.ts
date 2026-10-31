import { describe, expect, it } from "vitest";
import {
  applyRouteRuleHeaders,
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

  it("merges the incoming query into a destination that has one; the destination wins per key", () => {
    const compiled = compileRouteRules({
      redirects: [
        { source: "/old/:slug", destination: "/new/:slug?tab=info&ref=rule", permanent: false },
        { source: "/frag", destination: "/target?a=1#top", permanent: false },
        { source: "/empty", destination: "/target?", permanent: false },
      ],
    });

    expect(
      resolveRouteRuleRedirect(compiled, "/old/post", "?utm_source=mail&tab=comments&x=1&x=2")
    ).toEqual({
      location: "/new/post?tab=info&ref=rule&utm_source=mail&x=1&x=2",
      status: 307,
    });
    // Only conflicting keys incoming: the destination stands as written.
    expect(resolveRouteRuleRedirect(compiled, "/old/post", "?tab=x")?.location).toBe(
      "/new/post?tab=info&ref=rule"
    );
    // No incoming query: unchanged.
    expect(resolveRouteRuleRedirect(compiled, "/old/post", "")?.location).toBe(
      "/new/post?tab=info&ref=rule"
    );
    // The query goes before a destination fragment.
    expect(resolveRouteRuleRedirect(compiled, "/frag", "?b=2")?.location).toBe("/target?a=1&b=2#top");
    expect(resolveRouteRuleRedirect(compiled, "/empty", "?b=2")?.location).toBe("/target?b=2");
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

  // Next's `:path*` is zero or more segments; a ported `headers()` source of
  // `/builder/:path*` must cover the `/builder` document itself.
  it("matches a named catch-all with zero segments, dropping its slash in the destination", () => {
    const compiled = compileRouteRules({
      redirects: [{ source: "/docs/:path*", destination: "/v2/docs/:path*", permanent: false }],
      headers: [{ source: "/builder/:path*", headers: { "Cross-Origin-Embedder-Policy": "credentialless" } }],
    });
    expect(resolveRouteRuleRedirect(compiled, "/docs", "")?.location).toBe("/v2/docs");
    expect(resolveRouteRuleRedirect(compiled, "/docs/a/b", "")?.location).toBe("/v2/docs/a/b");
    expect(resolveRouteRuleRedirect(compiled, "/docsx", "")).toBeNull();
    expect(resolveRouteRuleHeaders(compiled, "/builder")).toHaveLength(1);
    expect(resolveRouteRuleHeaders(compiled, "/builder/p1/files")).toHaveLength(1);
    expect(resolveRouteRuleHeaders(compiled, "/builders")).toHaveLength(0);
  });

  it("keeps a null header value: it suppresses rather than sets", () => {
    const compiled = compileRouteRules({
      headers: [{ source: "/embed/:path*", headers: { "X-Frame-Options": null, "X-Embed": "1" } }],
    });
    const matches = resolveRouteRuleHeaders(compiled, "/embed/card");
    const response = new Response("ok");
    const suppressed = applyRouteRuleHeaders(response, matches);
    expect([...suppressed]).toEqual(["x-frame-options"]);
    expect(response.headers.get("x-embed")).toBe("1");
    expect(response.headers.has("x-frame-options")).toBe(false);
  });
});
