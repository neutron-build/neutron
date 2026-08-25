import { describe, it, expect } from "vitest";
import { Marked } from "marked";
import { markedShikiExtension, highlightCode } from "./syntax-highlight.js";

describe("markedShikiExtension", () => {
  it("highlights fenced code blocks without leaking '[object Promise]'", async () => {
    const marked = new Marked();
    marked.use(markedShikiExtension());
    const html = await marked.parse("```js\nconst x = 1;\n```");
    // The v15 sync-renderer bug produced "[object Promise]"; the walkTokens
    // approach must not.
    expect(html).not.toContain("[object Promise]");
    // Shiki emits a <pre class="shiki ..."> wrapper with the tokenized source.
    expect(html).toContain("shiki");
    expect(html).toContain("const");
  });

  it("leaves non-code markdown untouched", async () => {
    const marked = new Marked();
    marked.use(markedShikiExtension());
    const html = await marked.parse("# Title\n\nA paragraph.");
    expect(html).toContain("<h1>");
    expect(html).toContain("A paragraph.");
  });
});

describe("highlightCode", () => {
  it("falls back to escaped <pre><code> for an unknown language", async () => {
    const html = await highlightCode("<b>&</b>", "definitely-not-a-real-lang");
    expect(html).toContain("&lt;b&gt;");
    expect(html).not.toContain("<b>&</b>");
  });

  it("escapes the language before interpolating it into the class attribute", async () => {
    const html = await highlightCode("x", 'nope" onclick="alert(1)');
    // The fence info-string is untrusted; it must not break out of the
    // class attribute in the fallback path.
    expect(html).toContain("language-nope&quot;");
    expect(html).not.toContain('onclick="alert');
  });

  it("loads a second theme after the first (per-theme highlighters)", async () => {
    await highlightCode("const x = 1;", "js", "github-dark");
    const html = await highlightCode("const x = 1;", "js", "github-light");
    expect(html).toContain("const");
  });
});
