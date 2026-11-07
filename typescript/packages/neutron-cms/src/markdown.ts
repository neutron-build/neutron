import { Marked } from "marked";
import { photoAttributes } from "./images.js";

const attribute = (value: string) => value.replace(/&(?!\w+;|#\d+;)/g, "&amp;").replace(/"/g, "&quot;").replace(/</g, "&lt;");

const marked = new Marked({
  gfm: true,
  breaks: true,
  renderer: {
    image({ href, title, text }) {
      const titled = title ? ` title="${attribute(title)}"` : "";
      // A size lets the browser reserve the space and leave images below the fold until they're near.
      // A thumbnail, when there is one, saves most screens the full image.
      const { src, width, height, srcset, sizes } = photoAttributes(href, "(min-width: 800px) 768px, 100vw");
      const sized = width ? ` width="${width}" height="${height}"` : "";
      const responsive = srcset ? ` srcset="${attribute(srcset)}" sizes="${sizes}"` : "";
      return `<img src="${attribute(src)}"${responsive} alt="${attribute(text)}"${titled}${sized} loading="lazy" decoding="async">`;
    },
  },
});

export const renderMarkdown = (source: string, options: { breaks?: boolean } = {}) =>
  marked.parse(source, { async: false, ...options });

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

export const firstImage = (body: string) => body.match(/!\[[^\]]*\]\(([^)\s]+)/)?.[1] ?? null;
