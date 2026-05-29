import { describe, it, expect } from "vitest";
import { buildRssFeed } from "./rss.js";
import { renderAttrs } from "./seo.js";

describe("buildRssFeed", () => {
  it("escapes the CDATA terminator in content:encoded to prevent breakout", () => {
    const xml = buildRssFeed({
      title: "Feed",
      description: "d",
      link: "https://example.com",
      items: [
        {
          title: "Post",
          link: "https://example.com/p",
          content: "before]]><script>alert(1)</script>after",
        },
      ],
    });
    // The raw "]]>" terminator must not survive verbatim inside the CDATA.
    expect(xml).not.toContain("]]><script>");
    expect(xml).toContain("]]]]><![CDATA[>");
  });
});

describe("renderAttrs", () => {
  it("drops event-handler attribute names", () => {
    const out = renderAttrs({ lang: "en", onload: "alert(1)", onClick: "x" });
    expect(out).toContain('lang="en"');
    expect(out).not.toContain("onload");
    expect(out.toLowerCase()).not.toContain("onclick");
  });
});
