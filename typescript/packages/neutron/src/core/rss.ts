/**
 * RSS 2.0 feed generation helper.
 * Use in an API route to return an RSS feed response.
 */

export interface RssItem {
  title: string;
  link: string;
  description?: string;
  pubDate?: string | Date;
  guid?: string;
  content?: string;
}

export interface RssOptions {
  title: string;
  description: string;
  link: string;
  language?: string;
  items: RssItem[];
}

function escapeXml(str: string): string {
  return str
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function formatPubDate(date: string | Date): string {
  if (date instanceof Date) {
    return date.toUTCString();
  }
  // If already a string, try to normalize to RFC 2822
  const parsed = new Date(date);
  if (!isNaN(parsed.getTime())) {
    return parsed.toUTCString();
  }
  return date;
}

export function buildRssFeed(options: RssOptions): string {
  const { title, description, link, language, items } = options;

  const channelParts: string[] = [
    `    <title>${escapeXml(title)}</title>`,
    `    <link>${escapeXml(link)}</link>`,
    `    <description>${escapeXml(description)}</description>`,
  ];

  if (language) {
    channelParts.push(`    <language>${escapeXml(language)}</language>`);
  }

  channelParts.push(
    `    <lastBuildDate>${new Date().toUTCString()}</lastBuildDate>`,
    `    <generator>Neutron</generator>`
  );

  for (const item of items) {
    const itemParts: string[] = [
      `      <title>${escapeXml(item.title)}</title>`,
      `      <link>${escapeXml(item.link)}</link>`,
    ];

    if (item.description) {
      itemParts.push(
        `      <description>${escapeXml(item.description)}</description>`
      );
    }

    if (item.pubDate) {
      itemParts.push(
        `      <pubDate>${formatPubDate(item.pubDate)}</pubDate>`
      );
    }

    if (item.guid) {
      itemParts.push(`      <guid>${escapeXml(item.guid)}</guid>`);
    } else {
      itemParts.push(
        `      <guid isPermaLink="true">${escapeXml(item.link)}</guid>`
      );
    }

    if (item.content) {
      itemParts.push(
        `      <content:encoded><![CDATA[${item.content}]]></content:encoded>`
      );
    }

    channelParts.push(`    <item>\n${itemParts.join("\n")}\n    </item>`);
  }

  return `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/" xmlns:atom="http://www.w3.org/2005/Atom">
  <channel>
${channelParts.join("\n")}
  </channel>
</rss>`;
}
