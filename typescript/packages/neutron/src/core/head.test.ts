import { describe, expect, it } from "vitest";
import { resolveHeadHtml, resolveHeadDocument, type HeadRouteEntry } from "./head.js";
import type { Route, RouteModule } from "./types.js";

function route(id: string, path = "/"): Route {
  return {
    id,
    path,
    file: `src/routes/${id}.tsx`,
    pattern: /.*/,
    params: [],
    config: { mode: "app" },
    parentId: null,
  };
}

function entry(id: string, module: RouteModule | undefined, path = "/"): HeadRouteEntry {
  return { route: route(id, path), module };
}

const baseOptions = {
  request: new Request("http://localhost/"),
  params: {},
  context: {},
  pathname: "/",
  loaderData: {} as Record<string, unknown>,
};

describe("resolveHeadHtml", () => {
  it("emits a default head when no module implements head()", async () => {
    const html = await resolveHeadHtml(
      [entry("layout", { default: () => null }), entry("page", { default: () => null })],
      baseOptions
    );
    expect(html).toContain('<meta charset="UTF-8">');
    expect(html).toContain("<title>Home</title>");
  });

  it("skips undefined modules and modules without head()", async () => {
    const html = await resolveHeadHtml(
      [entry("missing", undefined), entry("no-head", { default: () => null })],
      baseOptions
    );
    expect(html).toContain('name="viewport"');
  });

  it("merges structured SeoMetaInput outermost-first (page overrides layout)", async () => {
    const layout: RouteModule = {
      head: () => ({ title: "Site", description: "Layout desc" }),
    };
    const page: RouteModule = {
      head: () => ({ description: "Page desc" }),
    };
    const html = await resolveHeadHtml(
      [entry("layout", layout), entry("page", page)],
      baseOptions
    );
    // Page's description wins; layout's title survives.
    expect(html).toContain("<title>Site</title>");
    expect(html).toContain('content="Page desc"');
    expect(html).not.toContain('content="Layout desc"');
  });

  it("passes raw string fragments through verbatim", async () => {
    const page: RouteModule = {
      head: () => '<link rel="preconnect" href="https://cdn.example.com">',
    };
    const html = await resolveHeadHtml([entry("page", page)], baseOptions);
    expect(html).toContain('<link rel="preconnect" href="https://cdn.example.com">');
  });

  it("gives each head() its own loader-data slice as `data`", async () => {
    let seen: unknown;
    const page: RouteModule = {
      head: (args) => {
        seen = args.data;
        return null;
      },
    };
    await resolveHeadHtml([entry("page", page)], {
      ...baseOptions,
      loaderData: { page: { hello: "world" }, other: 1 },
    });
    expect(seen).toEqual({ hello: "world" });
  });

  it("applies the CSP nonce to head-emitted scripts when provided", async () => {
    const page: RouteModule = {
      head: () => ({ jsonLd: { "@type": "WebSite" } }),
    };
    const withNonce = await resolveHeadHtml([entry("page", page)], {
      ...baseOptions,
      nonce: "abc123",
    });
    expect(withNonce).toContain('nonce="abc123"');

    const withoutNonce = await resolveHeadHtml([entry("page", page)], baseOptions);
    expect(withoutNonce).not.toContain("nonce=");
  });
});

describe("resolveHeadDocument", () => {
  it("returns the merged SEO alongside the head HTML", async () => {
    const layout: RouteModule = {
      head: () => ({ htmlAttrs: { lang: "en-CA" }, title: "Site" }),
    };
    const page: RouteModule = {
      head: () => ({ bodyAttrs: { "data-page": "home" } }),
    };
    const { headHtml, seo } = await resolveHeadDocument(
      [entry("layout", layout), entry("page", page)],
      baseOptions
    );
    expect(headHtml).toContain("<title>Site</title>");
    expect(seo?.htmlAttrs).toEqual({ lang: "en-CA" });
    expect(seo?.bodyAttrs).toEqual({ "data-page": "home" });
  });

  it("returns null SEO when no route emits structured head data", async () => {
    const { seo } = await resolveHeadDocument(
      [entry("page", { default: () => null })],
      baseOptions
    );
    expect(seo).toBeNull();
  });

  it("lets the page override layout htmlAttrs per-attribute", async () => {
    const layout: RouteModule = {
      head: () => ({ htmlAttrs: { lang: "en", "data-theme": "light" } }),
    };
    const page: RouteModule = {
      head: () => ({ htmlAttrs: { lang: "fr" } }),
    };
    const { seo } = await resolveHeadDocument(
      [entry("layout", layout), entry("page", page)],
      baseOptions
    );
    expect(seo?.htmlAttrs).toEqual({ lang: "fr", "data-theme": "light" });
  });
});
