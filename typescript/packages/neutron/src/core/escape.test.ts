import { describe, it, expect } from "vitest";
import { escapeHtml, escapeXml } from "./escape.js";

describe("escapeHtml", () => {
  it("escapes all five HTML metacharacters (apostrophe as &#39;)", () => {
    expect(escapeHtml(`<a href="x" id='y'>&</a>`)).toBe(
      "&lt;a href=&quot;x&quot; id=&#39;y&#39;&gt;&amp;&lt;/a&gt;"
    );
  });

  it("escapes & first so existing entities are not double-broken", () => {
    expect(escapeHtml("a & <b>")).toBe("a &amp; &lt;b&gt;");
  });
});

describe("escapeXml", () => {
  it("escapes all five XML metacharacters (apostrophe as &apos;)", () => {
    expect(escapeXml(`'&"<>`)).toBe("&apos;&amp;&quot;&lt;&gt;");
  });
});
