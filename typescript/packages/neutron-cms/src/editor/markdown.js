// Markdown in and out of the visual editor. It reads with the same parser the site renders with,
// so what the editor shows is what visitors see. Whatever it can't show stays as a raw block,
// written back exactly as it came in.
import { MarkdownSerializer, defaultMarkdownSerializer as base } from "prosemirror-markdown";
import { schema } from "./schema.js";

class Unsupported extends Error {}

const hasHtml = (tokens = []) => tokens.some((token) => token.type === "html" || hasHtml(token.tokens));
const raw = (source) => schema.nodes.raw.create({ source: source.replace(/\n+$/, "") });

function inline(tokens, decode, marks = []) {
  const out = [];
  for (const token of tokens ?? []) {
    switch (token.type) {
      case "text":
        if (token.tokens) out.push(...inline(token.tokens, decode, marks));
        else if (token.text) out.push(schema.text(decode(token.text), marks));
        break;
      case "escape":
        out.push(schema.text(decode(token.text), marks));
        break;
      case "strong":
      case "em":
      case "del": {
        const mark = schema.marks[token.type === "del" ? "strike" : token.type].create();
        out.push(...inline(token.tokens, decode, mark.addToSet(marks)));
        break;
      }
      case "codespan":
        if (token.text) out.push(schema.text(decode(token.text), schema.marks.code.create().addToSet(marks)));
        break;
      case "link":
        if (!token.tokens?.length) throw new Unsupported("empty link");
        out.push(...inline(token.tokens, decode, schema.marks.link.create({ href: token.href, title: token.title || null }).addToSet(marks)));
        break;
      case "image":
        out.push(schema.nodes.image.create({ src: token.href, alt: decode(token.text), title: token.title || null }, null, marks));
        break;
      case "br":
        out.push(schema.nodes.hard_break.create());
        break;
      default:
        throw new Unsupported(token.type);
    }
  }
  return out;
}

function blocks(tokens, decode) {
  const out = [];
  for (const token of tokens ?? []) {
    try {
      out.push(...block(token, decode));
    } catch (error) {
      if (!(error instanceof Unsupported)) throw error;
      out.push(raw(token.raw));
    }
  }
  return out;
}

function block(token, decode) {
  if (token.type === "space") return [];
  if (hasHtml(token.tokens) || hasHtml(token.items)) return [raw(token.raw)];
  switch (token.type) {
    case "paragraph":
    case "text":
      return [schema.nodes.paragraph.create(null, inline(token.tokens ?? [{ type: "text", text: token.text }], decode))];
    case "heading":
      return [schema.nodes.heading.create({ level: token.depth }, inline(token.tokens, decode))];
    case "code":
      if (token.codeBlockStyle === "indented") throw new Unsupported("indented code");
      return [schema.nodes.code_block.create({ params: token.lang || "" }, token.text ? schema.text(token.text) : null)];
    case "blockquote": {
      const inside = blocks(token.tokens, decode);
      if (!inside.length) throw new Unsupported("empty quote");
      return [schema.nodes.blockquote.create(null, inside)];
    }
    case "hr":
      return [schema.nodes.horizontal_rule.create()];
    case "list": {
      if (token.items.some((item) => item.task)) throw new Unsupported("task list");
      const items = token.items.map((item) => {
        const inside = blocks(item.tokens, decode);
        if (inside[0]?.type !== schema.nodes.paragraph) throw new Unsupported("list item");
        return schema.nodes.list_item.create(null, inside);
      });
      const tight = !token.loose;
      return [token.ordered
        ? schema.nodes.ordered_list.create({ order: Number(token.start) || 1, tight }, items)
        : schema.nodes.bullet_list.create({ tight }, items)];
    }
    default:
      throw new Unsupported(token.type);
  }
}

/** A document for the editor, or the reason it can't be shown. `lexer` is marked's, set up as the site's. */
export function fromMarkdown(markdown, lexer, decode = (text) => text) {
  const content = blocks(lexer(markdown), decode);
  return schema.nodes.doc.create(null, content.length ? content : [schema.nodes.paragraph.create()]);
}

const serializer = new MarkdownSerializer(
  {
    ...base.nodes,
    raw(state, node) {
      state.write(node.attrs.source);
      state.closeBlock(node);
    },
    image(state, node, parent, index) {
      if (node.attrs.src.startsWith("blob:")) return; // still uploading
      base.nodes.image(state, node, parent, index);
    },
    bullet_list(state, node) {
      state.renderList(node, "  ", () => "- ");
    },
  },
  {
    em: { open: "*", close: "*", mixable: true, expelEnclosingWhitespace: true },
    strong: base.marks.strong,
    strike: { open: "~~", close: "~~", mixable: true, expelEnclosingWhitespace: true },
    link: base.marks.link,
    code: base.marks.code,
  },
  // Typed "<" is text, not the start of HTML.
  { escapeExtraCharacters: /[<]/g },
);

export const toMarkdown = (doc) => serializer.serialize(doc, { tightLists: true });
