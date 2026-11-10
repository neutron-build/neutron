import assert from "node:assert/strict";
import { test } from "node:test";
import { Marked } from "marked";
import { renderMarkdown } from "./markdown.js";
// The visual editor's Markdown, from source: it is bundled for the browser, not compiled with the package.
import { fromMarkdown, toMarkdown } from "../src/editor/markdown.js";
import { embedFrom, isEmbed, providerEmbed } from "../src/editor/embed.js";

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

test("typed text that looks like structure stays text", () => {
  const { schema } = fromMarkdown("", lexer, decode).type;
  const para = (...content: Parameters<typeof schema.node>[2][]) => schema.node("paragraph", null, content.flat() as never);
  const cases = [
    para(schema.text("1) not a list")),
    para(schema.text("    not code")),
    para(schema.text("first"), schema.node("hard_break"), schema.text("# not a heading")),
    para(schema.text("first"), schema.node("hard_break"), schema.text("2) not a list")),
  ];
  for (const paragraph of cases) {
    const html = rendered(toMarkdown(schema.node("doc", null, [paragraph])));
    assert.match(html, /^<p>/, html);
    assert.doesNotMatch(html, /<(ol|ul|pre|h1)>/, html);
  }
});

test("links that could run code lose the link, not the words", () => {
  const md = "[click](javascript:alert(1)) and [ok](https://example.com) and ![x](javascript:1)";
  assert.match(renderMarkdown(md), /^<p>click and <a href="https:\/\/example\.com">ok<\/a> and x<\/p>/);
  assert.match(renderMarkdown("<b>hi</b>", { html: false }), /&lt;b&gt;hi&lt;\/b&gt;/);
});

test("an iframe block shows as an embed and comes back byte for byte", () => {
  const spotify = '<iframe style="border-radius:12px" src="https://open.spotify.com/embed/track/4uLU6hMCjMI75M1A2tKUQC?utm_source=generator&amp;theme=0" width="100%" height="152" frameBorder="0" allowfullscreen="" allow="autoplay; clipboard-write; encrypted-media; fullscreen; picture-in-picture" loading="lazy"></iframe>';
  const markdown = `Intro *text*.\n\n${spotify}\n\nAfter it.`;
  const doc = fromMarkdown(markdown, lexer, decode);
  assert.deepEqual(doc.content.content.map((node) => node.type.name), ["paragraph", "embed", "paragraph"]);
  assert.equal(toMarkdown(doc), markdown);
  // Anything more than one iframe stays a source block.
  for (const other of [`${spotify}\n<p>caption</p>`, '<iframe src="http://x.test/"></iframe>', "<div><iframe src=\"https://x.test/\"></iframe></div>"]) {
    const kinds = fromMarkdown(other, lexer, decode).content.content.map((node) => node.type.name);
    assert.deepEqual(kinds, ["raw"], other);
    assert.equal(roundTrip(other), other);
  }
});

test("player links become that provider's iframe", () => {
  const youtube = (src: string) => `<iframe src="${src}" title="YouTube video" width="560" height="315" style="width:100%;height:auto;aspect-ratio:16/9;border:0" loading="lazy" referrerpolicy="strict-origin-when-cross-origin" allow="encrypted-media; picture-in-picture; fullscreen" allowfullscreen></iframe>`;
  const cases: Record<string, string> = {
    "https://www.youtube.com/watch?v=dQw4w9WgXcQ": youtube("https://www.youtube-nocookie.com/embed/dQw4w9WgXcQ"),
    "https://youtu.be/dQw4w9WgXcQ?si=abc&t=90": youtube("https://www.youtube-nocookie.com/embed/dQw4w9WgXcQ?start=90"),
    "https://m.youtube.com/watch?v=dQw4w9WgXcQ&t=1m5s": youtube("https://www.youtube-nocookie.com/embed/dQw4w9WgXcQ?start=65"),
    "https://www.youtube.com/shorts/dQw4w9WgXcQ": youtube("https://www.youtube-nocookie.com/embed/dQw4w9WgXcQ"),
    "youtube.com/embed/dQw4w9WgXcQ": youtube("https://www.youtube-nocookie.com/embed/dQw4w9WgXcQ"),
    "https://vimeo.com/76979871": '<iframe src="https://player.vimeo.com/video/76979871" title="Vimeo video" width="560" height="315" style="width:100%;height:auto;aspect-ratio:16/9;border:0" loading="lazy" allow="picture-in-picture; fullscreen" allowfullscreen></iframe>',
    "https://vimeo.com/76979871/8272103f6e": '<iframe src="https://player.vimeo.com/video/76979871?h=8272103f6e" title="Vimeo video" width="560" height="315" style="width:100%;height:auto;aspect-ratio:16/9;border:0" loading="lazy" allow="picture-in-picture; fullscreen" allowfullscreen></iframe>',
    "https://open.spotify.com/track/4uLU6hMCjMI75M1A2tKUQC?si=x": '<iframe src="https://open.spotify.com/embed/track/4uLU6hMCjMI75M1A2tKUQC" title="Spotify track" width="100%" height="152" style="border:0;border-radius:12px" loading="lazy" allow="encrypted-media; fullscreen; picture-in-picture"></iframe>',
    "https://open.spotify.com/intl-de/album/1DFixLWuPkv3KT3TnV35m3": '<iframe src="https://open.spotify.com/embed/album/1DFixLWuPkv3KT3TnV35m3" title="Spotify album" width="100%" height="352" style="border:0;border-radius:12px" loading="lazy" allow="encrypted-media; fullscreen; picture-in-picture"></iframe>',
  };
  for (const [link, iframe] of Object.entries(cases)) {
    assert.equal(embedFrom(link), iframe, link);
    assert.equal(providerEmbed(link), iframe, link);
    // What it makes is an embed block the editor keeps exactly.
    assert.equal(fromMarkdown(iframe, lexer, decode).firstChild!.type.name, "embed", link);
    assert.equal(roundTrip(`a\n\n${iframe}`), `a\n\n${iframe}`);
    assert.doesNotMatch(iframe, /<script|\son\w+=/i);
  }
});

test("only one plain https iframe or a known player link is accepted", () => {
  const good = '<iframe src="https://example.com/player?a=1&amp;b=2" title="Map"></iframe>';
  assert.equal(embedFrom(`  ${good}\n`), good);
  for (const bad of [
    "https://example.com/video",
    "https://www.youtube.com/watch?v=short",
    "https://youtube.com/@channel",
    "https://open.spotify.com/user/someone",
    "javascript:alert(1)",
    "not a link",
    '<iframe src="http://example.com/"></iframe>',
    '<iframe src="javascript:alert(1)"></iframe>',
    '<iframe src=" javascript:alert(1)"></iframe>',
    '<iframe src="&#104;ttps://example.com/"></iframe>',
    '<iframe src="https://example.com/" onload="alert(1)"></iframe>',
    '<iframe src="https://example.com/" ONLOAD=alert(1)></iframe>',
    '<iframe srcdoc="<script>alert(1)</script>" src="https://example.com/"></iframe>',
    '<iframe src="https://example.com/" src="javascript:alert(1)"></iframe>',
    '<iframe src="https://example.com/"></iframe><iframe src="https://example.com/"></iframe>',
    '<iframe src="https://example.com/"><script>alert(1)</script></iframe>',
    '<iframe src="https://example.com/"></iframe><script>alert(1)</script>',
    '<script src="https://example.com/x.js"></script>',
    '<iframe src="https://example.com/">',
    "<iframe></iframe>",
  ]) assert.equal(embedFrom(bad), null, bad);
  assert.equal(isEmbed(good), true);
});
