export interface TocEntry {
  depth: number;
  text: string;
  slug: string;
}

const HEADING_REGEX = /<h([1-6])(?:\s[^>]*)?>([^<]*(?:<[^/h][^>]*>[^<]*)*)<\/h\1>/gi;
const TAG_STRIP_REGEX = /<[^>]*>/g;

function slugify(text: string): string {
  return text
    .toLowerCase()
    .trim()
    .replace(/[^\w\s-]/g, "")
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-");
}

export function extractToc(html: string): TocEntry[] {
  const entries: TocEntry[] = [];
  let match: RegExpExecArray | null;

  // Reset regex state
  HEADING_REGEX.lastIndex = 0;

  while ((match = HEADING_REGEX.exec(html)) !== null) {
    const depth = parseInt(match[1], 10);
    const raw = match[2];
    const text = raw.replace(TAG_STRIP_REGEX, "").trim();

    if (text.length > 0) {
      entries.push({
        depth,
        text,
        slug: slugify(text),
      });
    }
  }

  return entries;
}
