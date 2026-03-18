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

    expect(html).toContain("<title>about - team - Neutron</title>");
    expect(html).toContain('name="x-test"');
  });

  it("does not add fallback title when custom title is present", () => {
    const html = renderDocumentHead("/about", { title: "Custom Page" });
    expect(html).toContain("<title>Custom Page</title>");
    expect(html).not.toContain("<title>about - Neutron</title>");
  });
});
