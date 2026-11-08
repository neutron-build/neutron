import { Marked, type Token, type Tokens } from "marked";
import { photoAttributes } from "./images.js";
import { isSafeUrl } from "./safe-url.js";

const attribute = (value: string) => value.replace(/&(?!\w+;|#\d+;)/g, "&amp;").replace(/"/g, "&quot;").replace(/</g, "&lt;");

const renderer = {
  // A link that could run code keeps its words and loses the link.
  link(this: { parser: { parseInline(tokens: Token[]): string } }, { href, tokens }: Tokens.Link) {
    return isSafeUrl(href) ? false : this.parser.parseInline(tokens);
  },
  image({ href, title, text }: Tokens.Image) {
    if (!isSafeUrl(href)) return attribute(text);
    const titled = title ? ` title="${attribute(title)}"` : "";
    // A size lets the browser reserve the space and leave images below the fold until they're near.
    // A thumbnail, when there is one, saves most screens the full image.
    const { src, width, height, srcset, sizes } = photoAttributes(href, "(min-width: 800px) 768px, 100vw");
    const sized = width ? ` width="${width}" height="${height}"` : "";
    const responsive = srcset ? ` srcset="${attribute(srcset)}" sizes="${sizes}"` : "";
    return `<img src="${attribute(src)}"${responsive} alt="${attribute(text)}"${titled}${sized} loading="lazy" decoding="async">`;
  },
};

const marked = new Marked({ gfm: true, breaks: true, renderer });

// For Markdown from somewhere you don't control, such as a README fetched from GitHub: its HTML shows as text.
const escapeHtml = (text: string) => text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
const untrusted = new Marked({ gfm: true, breaks: true, renderer: { ...renderer, html: ({ text }: Tokens.HTML | Tokens.Tag) => escapeHtml(text) } });

/** Markdown to HTML, as the site shows it. Pass `html: false` for text you don't control: its raw HTML shows as text. */
export const renderMarkdown = (source: string, options: { breaks?: boolean; html?: boolean } = {}) => {
  const { html = true, ...rest } = options;
  return (html ? marked : untrusted).parse(source, { async: false, ...rest });
};

/** The description if there is one, otherwise the opening of the post. */
export function summarize(post: { description: string; body: string }, length = 160): string {
  if (post.description.trim()) return post.description.trim();
  const text = renderMarkdown(post.body)
    .replace(/<(iframe|figure|script|style)\b[\s\S]*?<\/\1>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&#39;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/\s+/g, " ")
    .trim();
  return text.length <= length ? text : `${text.slice(0, length - 1).replace(/\s+\S*$/, "")}…`;
}

export const firstImage = (body: string) => body.match(/!\[(?:\\.|[^\]\\])*\]\(([^)\s]+)/)?.[1] ?? null;
