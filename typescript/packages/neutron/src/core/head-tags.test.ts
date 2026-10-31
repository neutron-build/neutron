import { describe, expect, it } from "vitest";
import { buildMetaTags, mergeSeoMetaInput, renderDocumentHead, renderMetaTags, type SeoMetaInput } from "./seo.js";
import { extractDocumentHead } from "./document-head.js";

/**
 * F-P6-1: the head tags a Next app's metadata produces, expressible through
 * head() — structured fields in Next's Metadata shape, a generic `meta`
 * escape hatch — and the app's index.html head carried into server-rendered
 * documents under the route chain's tags.
 */

const head = (seo: SeoMetaInput | null, documentHead = extractDocumentHead(""), fragments: string[] = []) =>
  renderDocumentHead("/chat", seo, fragments, undefined, documentHead);

// Fylun's index.html, as the P6 port ships it.
const FYLUN_INDEX = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <!-- interactive-widget: shrink the layout viewport when the keyboard opens. -->
    <meta name="viewport" content="width=device-width, initial-scale=1, interactive-widget=resizes-content" />
    <meta name="theme-color" media="(prefers-color-scheme: light)" content="white" />
    <meta name="theme-color" media="(prefers-color-scheme: dark)" content="#0a0a0a" />
    <meta name="author" content="Fylun" />
    <meta name="creator" content="Fylun" />
    <meta property="og:locale" content="en_US" />
    <meta property="og:site_name" content="Fylun" />
    <link rel="search" type="application/opensearchdescription+xml" title="Fylun Search" href="/opensearch.xml" />
    <link rel="stylesheet" href="/src/styles.css" />
    <script>window.inline = true</script>
    <title>Fylun &mdash; Every AI Model. One Subscription.</title>
  </head>
  <body><div id="app"></div><script type="module" src="/src/main.tsx"></script></body>
</html>`;

describe("head() structured fields (Next Metadata shape)", () => {
  it("renders viewport, theme-color per media, authors, creator, verification, og locale/site name/images and robots", () => {
    const html = renderMetaTags(
      buildMetaTags({
        title: "Chat",
        viewport: { width: "device-width", initialScale: 1, interactiveWidget: "resizes-content" },
        themeColor: [
          { media: "(prefers-color-scheme: light)", color: "white" },
          { media: "(prefers-color-scheme: dark)", color: "#0a0a0a" },
        ],
        authors: [{ name: "Fylun", url: "https://fylun.ai/about" }],
        creator: "Fylun",
        verification: { google: "g-token", other: { "msvalidate.01": "bing-token" } },
        robots: { index: true, follow: true },
        canonical: "https://fylun.ai",
        openGraph: {
          locale: "en_US",
          siteName: "Fylun",
          images: [{ url: "https://fylun.ai/og.png", width: 1200, height: 630, type: "image/png", alt: "Fylun" }],
        },
        twitter: { card: "summary_large_image" },
      })
    );
    for (const tag of [
      '<meta name="viewport" content="width=device-width, initial-scale=1, interactive-widget=resizes-content">',
      '<meta name="theme-color" media="(prefers-color-scheme: light)" content="white">',
      '<meta name="theme-color" media="(prefers-color-scheme: dark)" content="#0a0a0a">',
      '<meta name="author" content="Fylun">',
      '<link rel="author" href="https://fylun.ai/about">',
      '<meta name="creator" content="Fylun">',
      '<meta name="google-site-verification" content="g-token">',
      '<meta name="msvalidate.01" content="bing-token">',
      '<meta name="robots" content="index, follow">',
      // The canonical the app gave, without a trailing slash added.
      '<link rel="canonical" href="https://fylun.ai">',
      '<meta property="og:url" content="https://fylun.ai">',
      '<meta property="og:locale" content="en_US">',
      '<meta property="og:site_name" content="Fylun">',
      '<meta property="og:image" content="https://fylun.ai/og.png">',
      '<meta property="og:image:width" content="1200">',
      '<meta property="og:image:height" content="630">',
      '<meta property="og:image:type" content="image/png">',
      '<meta property="og:image:alt" content="Fylun">',
      '<meta name="twitter:image" content="https://fylun.ai/og.png">',
    ]) {
      expect(html).toContain(tag);
    }
  });

  it("emits the robots value asked for: noindex is noindex, a string is sent as written", () => {
    expect(renderMetaTags(buildMetaTags({ noindex: true }))).toBe('<meta name="robots" content="noindex">');
    expect(renderMetaTags(buildMetaTags({ robots: "noindex, nofollow, noarchive" }))).toBe(
      '<meta name="robots" content="noindex, nofollow, noarchive">'
    );
    expect(renderMetaTags(buildMetaTags({ noindex: true, robots: { index: false, follow: true } }))).toBe(
      '<meta name="robots" content="noindex, follow">'
    );
  });

  it("emits several og:image groups in order", () => {
    const html = renderMetaTags(
      buildMetaTags({ openGraph: { images: ["https://x.test/a.png", { url: "https://x.test/b.png", alt: "B" }] } })
    );
    expect(html.indexOf("a.png")).toBeLessThan(html.indexOf("b.png"));
    expect(html.match(/property="og:image"/g)).toHaveLength(2);
    expect(html).toContain('<meta property="og:image:alt" content="B">');
  });

  it("meta escape hatch: name, property, http-equiv and media, replacing a field's tag of the same key", () => {
    const html = renderMetaTags(
      buildMetaTags({
        description: "from the field",
        meta: [
          { name: "format-detection", content: "telephone=no" },
          { property: "fb:app_id", content: "123" },
          { httpEquiv: "x-ua-compatible", content: "IE=edge" },
          { name: "color-scheme", media: "print", content: "light" },
          { name: "description", content: "from meta" },
        ],
      })
    );
    expect(html).toContain('<meta name="format-detection" content="telephone=no">');
    expect(html).toContain('<meta property="fb:app_id" content="123">');
    expect(html).toContain('<meta http-equiv="x-ua-compatible" content="IE=edge">');
    expect(html).toContain('<meta name="color-scheme" media="print" content="light">');
    expect(html).toContain('<meta name="description" content="from meta">');
    expect(html).not.toContain("from the field");
  });

  it("escapes every value", () => {
    const hostile = '"><script>alert(1)</script>';
    const html = renderMetaTags(
      buildMetaTags({
        themeColor: [{ color: hostile, media: hostile }],
        verification: { google: hostile },
        openGraph: { images: [{ url: hostile, alt: hostile }], siteName: hostile },
        meta: [{ name: hostile, content: hostile }],
        viewport: hostile,
      })
    );
    expect(html).not.toContain("<script>");
    expect(html).not.toMatch(/"><script/);
    expect(html).toContain("&quot;&gt;&lt;script&gt;");
  });

  it("merges the chain: a route's meta entries replace its layout's for the same key, lists are replaced", () => {
    const merged = mergeSeoMetaInput(
      {
        meta: [
          { name: "format-detection", content: "telephone=no" },
          { name: "referrer", content: "origin" },
        ],
        themeColor: ["white", "black"],
        verification: { google: "g" },
        openGraph: { siteName: "Fylun", images: ["/a.png"] },
      },
      {
        meta: [{ name: "referrer", content: "no-referrer" }],
        themeColor: "red",
        verification: { yandex: "y" },
        openGraph: { images: ["/b.png"] },
      }
    )!;
    expect(merged.meta).toEqual([
      { name: "format-detection", content: "telephone=no" },
      { name: "referrer", content: "no-referrer" },
    ]);
    expect(merged.themeColor).toBe("red");
    expect(merged.verification).toEqual({ google: "g", yandex: "y" });
    expect(merged.openGraph).toEqual({ siteName: "Fylun", images: ["/b.png"] });

    // A page's noindex replaces its layout's robots, and the other way round.
    const page = (seo: SeoMetaInput, base: SeoMetaInput) =>
      renderMetaTags(buildMetaTags(mergeSeoMetaInput(base, seo)!));
    expect(page({ noindex: true }, { robots: "index, follow" })).toBe('<meta name="robots" content="noindex">');
    expect(page({ robots: "index" }, { noindex: true })).toBe('<meta name="robots" content="index">');
  });
});

describe("index.html head in server-rendered documents", () => {
  it("extracts title, meta and link tags as written, not charset, stylesheets, scripts or comments", () => {
    expect(extractDocumentHead(FYLUN_INDEX)).toEqual([
      {
        key: "meta:name:viewport",
        html: '<meta name="viewport" content="width=device-width, initial-scale=1, interactive-widget=resizes-content" />',
      },
      { key: "meta:name:theme-color", html: '<meta name="theme-color" media="(prefers-color-scheme: light)" content="white" />' },
      { key: "meta:name:theme-color", html: '<meta name="theme-color" media="(prefers-color-scheme: dark)" content="#0a0a0a" />' },
      { key: "meta:name:author", html: '<meta name="author" content="Fylun" />' },
      { key: "meta:name:creator", html: '<meta name="creator" content="Fylun" />' },
      { key: "meta:property:og:locale", html: '<meta property="og:locale" content="en_US" />' },
      { key: "meta:property:og:site_name", html: '<meta property="og:site_name" content="Fylun" />' },
      {
        key: "link:search:/opensearch.xml",
        html: '<link rel="search" type="application/opensearchdescription+xml" title="Fylun Search" href="/opensearch.xml" />',
      },
      { key: "title", html: "<title>Fylun &mdash; Every AI Model. One Subscription.</title>" },
    ]);
  });

  it("is the layer under the route chain: same-key tags of the chain replace it", () => {
    const documentHead = extractDocumentHead(FYLUN_INDEX);
    const bare = head(null, documentHead);
    // index.html's viewport replaces the framework default; one charset.
    expect(bare.match(/name="viewport"/g)).toHaveLength(1);
    expect(bare).toContain("interactive-widget=resizes-content");
    expect(bare.match(/charset/g)).toHaveLength(1);
    expect(bare).toContain('<meta name="theme-color" media="(prefers-color-scheme: dark)" content="#0a0a0a" />');
    expect(bare).toContain('<link rel="search" type="application/opensearchdescription+xml" title="Fylun Search" href="/opensearch.xml" />');
    // index.html's title (as written) instead of one guessed from the URL.
    expect(bare.match(/<title>/g)).toHaveLength(1);
    expect(bare).toContain("<title>Fylun &mdash; Every AI Model. One Subscription.</title>");
    expect(bare).not.toContain("<script>");

    const page = head(
      {
        title: "Chat",
        themeColor: "red",
        viewport: "width=device-width",
        openGraph: { siteName: "Fylun Chat" },
      },
      documentHead
    );
    expect(page.match(/<title>/g)).toHaveLength(1);
    expect(page).toContain("<title>Chat</title>");
    // Every theme-color of index.html gives way to the page's.
    expect(page.match(/name="theme-color"/g)).toHaveLength(1);
    expect(page).toContain('<meta name="theme-color" content="red">');
    expect(page.match(/name="viewport"/g)).toHaveLength(1);
    expect(page).toContain('<meta name="viewport" content="width=device-width">');
    expect(page.match(/og:site_name/g)).toHaveLength(1);
    expect(page).toContain('content="Fylun Chat"');
    // Untouched keys stay.
    expect(page).toContain('<meta property="og:locale" content="en_US" />');
    expect(page).toContain('<meta name="author" content="Fylun" />');
  });

  it("a raw head() string replaces index.html tags with the keys it writes", () => {
    const page = head(null, extractDocumentHead(FYLUN_INDEX), [
      '<title>Raw</title><meta name="author" content="Someone"><meta name="viewport" content="width=500">',
    ]);
    expect(page.match(/<title>/g)).toHaveLength(1);
    expect(page.match(/name="author"/g)).toHaveLength(1);
    expect(page.match(/name="viewport"/g)).toHaveLength(1);
    expect(page).toContain('content="width=500"');
  });
});
