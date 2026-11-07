import assert from "node:assert/strict";
import { test } from "node:test";
import { Marked } from "marked";
// The visual editor's Markdown, from source: it is bundled for the browser, not compiled with the package.
import { fromMarkdown, toMarkdown } from "../src/editor/markdown.js";

const marked = new Marked({ gfm: true, breaks: true });
const lexer = (source: string) => marked.lexer(source) as unknown[];
const entities: Record<string, string> = { amp: "&", lt: "<", gt: ">", quot: '"', "#39": "'" };
const decode = (text: string) => text.replace(/&(amp|lt|gt|quot|#39);/g, (_, name: string) => entities[name]);
const rendered = (markdown: string) => (marked.parse(markdown) as string).replace(/>\s+</g, "><").trim();
const roundTrip = (markdown: string) => toMarkdown(fromMarkdown(markdown, lexer, decode));

const CASES: Record<string, string> = {
  inline: "Hi *there* **bold** ~~gone~~ `code` [link](https://x.test \"t\") and a_b_c\nnext line",
  escapes: "\\*not em\\* 5 < 6 &amp; Q&A, 1\\. not a list, \\# not a heading",
  lists: "- a\n- **b**\n\n1. one\n\n   two\n\n3. three\n4. four",
  blocks: "# Title\n\n> quoted\n> more\n\n```js\nlet x = *1*;\n```\n\n---\n\n![A photo](/media/abc-2000x1000.webp)",
  html: "<audio controls src=\"/a.mp3\"></audio>\n\nText with <b>inline</b> HTML.\n\n<div class=\"columns\">\n<div>",
  unsupported: "| a | b |\n|---|---|\n| 1 | 2 |\n\n- [ ] task\n\n    indented code\n\n[empty]()",
};

for (const [name, markdown] of Object.entries(CASES)) {
  test(`the visual editor keeps ${name} exactly as the site renders it`, () => {
    const back = roundTrip(markdown);
    assert.equal(rendered(back), rendered(markdown));
    assert.equal(roundTrip(back), back, "a second pass changes nothing");
  });
}

test("typed angle brackets stay text, never HTML", () => {
  const { schema } = fromMarkdown("", lexer, decode).type;
  const doc = schema.node("doc", null, [schema.node("paragraph", null, [schema.text("<script>alert(1)</script>")])]);
  assert.doesNotMatch(rendered(toMarkdown(doc)), /<script>/);
});
