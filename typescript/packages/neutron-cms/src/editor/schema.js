// What the visual editor can hold. Anything Markdown can say that isn't here is kept as a raw block.
import { Schema } from "prosemirror-model";

/** Same rule as safe-url.ts on the server: web, mail and phone links and paths on this site, nothing that runs code. */
export function isSafeUrl(href) {
  const plain = String(href).replace(/[\u0000-\u0020\u007f]/g, "");
  const scheme = plain.match(/^([a-z][a-z0-9+.-]*):/i)?.[1].toLowerCase();
  return !scheme || scheme === "http" || scheme === "https" || scheme === "mailto" || scheme === "tel";
}

export const schema = new Schema({
  nodes: {
    doc: { content: "block+" },
    paragraph: { content: "inline*", group: "block", parseDOM: [{ tag: "p" }], toDOM: () => ["p", 0] },
    heading: {
      attrs: { level: { default: 2 } },
      content: "inline*",
      group: "block",
      defining: true,
      parseDOM: [1, 2, 3, 4, 5, 6].map((level) => ({ tag: `h${level}`, attrs: { level } })),
      toDOM: (node) => [`h${node.attrs.level}`, 0],
    },
    blockquote: { content: "block+", group: "block", defining: true, parseDOM: [{ tag: "blockquote" }], toDOM: () => ["blockquote", 0] },
    code_block: {
      attrs: { params: { default: "" } },
      content: "text*",
      marks: "",
      group: "block",
      code: true,
      defining: true,
      parseDOM: [{ tag: "pre", preserveWhitespace: "full", getAttrs: (dom) => ({ params: dom.getAttribute("data-params") || "" }) }],
      toDOM: (node) => ["pre", node.attrs.params ? { "data-params": node.attrs.params } : {}, ["code", 0]],
    },
    horizontal_rule: { group: "block", parseDOM: [{ tag: "hr" }], toDOM: () => ["hr"] },
    bullet_list: {
      attrs: { tight: { default: true } },
      content: "list_item+",
      group: "block",
      parseDOM: [{ tag: "ul" }],
      toDOM: () => ["ul", 0],
    },
    ordered_list: {
      attrs: { order: { default: 1 }, tight: { default: true } },
      content: "list_item+",
      group: "block",
      parseDOM: [{ tag: "ol", getAttrs: (dom) => ({ order: dom.hasAttribute("start") ? Number(dom.getAttribute("start")) || 1 : 1 }) }],
      toDOM: (node) => ["ol", node.attrs.order === 1 ? {} : { start: node.attrs.order }, 0],
    },
    list_item: { content: "paragraph block*", defining: true, parseDOM: [{ tag: "li" }], toDOM: () => ["li", 0] },
    raw: {
      attrs: { source: { default: "" } },
      group: "block",
      atom: true,
      selectable: true,
      toDOM: (node) => ["pre", { class: "raw" }, node.attrs.source],
    },
    text: { group: "inline" },
    image: {
      attrs: { src: {}, alt: { default: "" }, title: { default: null } },
      inline: true,
      group: "inline",
      draggable: true,
      // Pasted images must be addresses, not data: or blob: copies that would bloat the text.
      parseDOM: [{ tag: "img[src]", getAttrs: (dom) => (/^(https?:|\/(?!\/))/i.test(dom.getAttribute("src")) ? { src: dom.getAttribute("src"), alt: dom.getAttribute("alt") || "", title: dom.getAttribute("title") } : false) }],
      toDOM: (node) => ["img", { src: node.attrs.src, alt: node.attrs.alt, title: node.attrs.title }],
    },
    hard_break: { inline: true, group: "inline", selectable: false, parseDOM: [{ tag: "br" }], toDOM: () => ["br"] },
  },
  marks: {
    link: {
      attrs: { href: {}, title: { default: null } },
      inclusive: false,
      parseDOM: [{ tag: "a[href]", getAttrs: (dom) => (isSafeUrl(dom.getAttribute("href")) ? { href: dom.getAttribute("href"), title: dom.getAttribute("title") } : false) }],
      toDOM: (mark) => ["a", { href: mark.attrs.href, title: mark.attrs.title, rel: "noreferrer" }, 0],
    },
    em: {
      parseDOM: [{ tag: "i" }, { tag: "em" }, { style: "font-style=italic" }, { style: "font-style=normal", clearMark: (mark) => mark.type.name === "em" }],
      toDOM: () => ["em", 0],
    },
    strong: {
      parseDOM: [
        { tag: "strong" },
        // Google Docs wraps everything in a <b> with normal weight.
        { tag: "b", getAttrs: (dom) => dom.style.fontWeight !== "normal" && null },
        { style: "font-weight=400", clearMark: (mark) => mark.type.name === "strong" },
        { style: "font-weight", getAttrs: (value) => /^(bold(er)?|[5-9]\d{2,})$/.test(value) && null },
      ],
      toDOM: () => ["strong", 0],
    },
    strike: { parseDOM: [{ tag: "s" }, { tag: "del" }, { tag: "strike" }], toDOM: () => ["s", 0] },
    code: { code: true, parseDOM: [{ tag: "code" }], toDOM: () => ["code", 0] },
  },
});
