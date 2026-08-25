import { describe, expect, it } from "vitest";
import {
  buildMetaTags,
  renderMetaTags,
  buildSitemapXml,
  buildRobotsTxt,
  mergeSeoMetaInput,
  renderDocumentHead,
} from "./seo.js";

describe("seo utilities", () => {
  it("builds and renders meta tags", () => {
    const tags = buildMetaTags({
      title: "Neutron",
      description: "Framework benchmark leader",
      canonical: "https://example.com",
      openGraph: {
        image: "https://example.com/og.png",
      },
    });
    const html = renderMetaTags(tags);

    expect(tags.length).toBeGreaterThan(0);
    expect(html).toContain("<title>Neutron</title>");
    expect(html).toContain('name="description"');
    expect(html).toContain('property="og:image"');
  });

  it("builds sitemap xml", () => {
    const xml = buildSitemapXml([
      { url: "https://example.com/" },
      { url: "https://example.com/pricing", priority: 0.8 },
    ]);

    expect(xml).toContain("<urlset");
    expect(xml).toContain("<loc>https://example.com/</loc>");
    expect(xml).toContain("<priority>0.8</priority>");
  });

  it("builds robots.txt", () => {
    const robots = buildRobotsTxt({
      rules: [
        {
          userAgent: "*",
          allow: ["/"],
          disallow: ["/admin"],
        },
      ],
      sitemap: "https://example.com/sitemap.xml",
    });

    expect(robots).toContain("User-agent: *");
    expect(robots).toContain("Allow: /");
    expect(robots).toContain("Disallow: /admin");
    expect(robots).toContain("Sitemap: https://example.com/sitemap.xml");
  });

  it("merges layered seo metadata with route-level override precedence", () => {
    const merged = mergeSeoMetaInput(
      {
        title: "Layout Title",
        description: "Layout description",
        openGraph: {
          title: "Layout OG Title",
          image: "/layout.png",
        },
      },
      {
        title: "Page Title",
        openGraph: {
          image: "/page.png",
        },
      }
    );

    expect(merged?.title).toBe("Page Title");
    expect(merged?.description).toBe("Layout description");
    expect(merged?.openGraph?.title).toBe("Layout OG Title");
    expect(merged?.openGraph?.image).toBe("/page.png");
  });

  it("renders document head with default title fallback and custom fragments", () => {
    const html = renderDocumentHead("/about/team", undefined, [
      '<meta name="x-test" content="on">',
    ]);

    expect(html).toContain("<title>about - team</title>");
    expect(html).toContain('name="x-test"');
  });

  it("does not add fallback title when custom title is present", () => {
    const html = renderDocumentHead("/about", { title: "Custom Page" });
    expect(html).toContain("<title>Custom Page</title>");
    expect(html).not.toContain("<title>about - Neutron</title>");
  });

  it("emits custom <link> tags (e.g. favicon)", () => {
    const html = renderMetaTags(
      buildMetaTags({ link: { rel: "icon", type: "image/svg+xml", href: "/favicon.svg" } }),
    );
    expect(html).toContain("<link");
    expect(html).toContain('rel="icon"');
    expect(html).toContain('href="/favicon.svg"');
  });

  it("keeps multiple same-rel links (e.g. preconnect) instead of deduping them", () => {
    const html = renderMetaTags(
      buildMetaTags({
        link: [
          { rel: "preconnect", href: "https://a.example" },
          { rel: "preconnect", href: "https://b.example" },
        ],
      }),
    );
    expect(html).toContain('href="https://a.example"');
    expect(html).toContain('href="https://b.example"');
  });

  it("merges link tags from layout and route", () => {
    const merged = mergeSeoMetaInput(
      { link: { rel: "icon", href: "/favicon.svg" } },
      { link: { rel: "manifest", href: "/site.webmanifest" } },
    );
    const links = Array.isArray(merged?.link) ? merged?.link : [merged?.link];
    expect(links?.length).toBe(2);
  });

  it("sanitizes attribute names in renderMetaTags (no attribute injection)", () => {
    const html = renderMetaTags([
      {
        tag: "link",
        attrs: {
          rel: "icon",
          'href="/evil" onclick="alert(1)': "payload",
        } as Record<string, string>,
      },
    ]);
    expect(html).not.toContain('onclick="');
    expect(html).not.toContain('href="/evil"');
  });

  it("drops event-handler attribute names in renderMetaTags", () => {
    const html = renderMetaTags([
      {
        tag: "link",
        attrs: { rel: "icon", href: "/ok", onload: "alert(1)" } as Record<string, string>,
      },
    ]);
    expect(html).not.toContain("onload");
    expect(html).toContain('rel="icon"');
  });
});
