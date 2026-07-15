import { getCollection } from "@neutron-build/core";

export async function loader() {
  const posts = (await getCollection('blog', ({ data }: any) => !data.draft))
    .sort((a: any, b: any) => b.data.pubDate.valueOf() - a.data.pubDate.valueOf());

  const siteUrl = 'https://neutron.build';

  const items = posts.map((post: any) => {
    const url = `${siteUrl}/blog/${post.slug}`;
    const pubDate = post.data.pubDate.toUTCString();
    const tags = post.data.tags.map((t: string) => `<category>${t}</category>`).join('');
    return `
    <item>
      <title><![CDATA[${post.data.title}]]></title>
      <description><![CDATA[${post.data.description}]]></description>
      <link>${url}</link>
      <guid isPermaLink="true">${url}</guid>
      <pubDate>${pubDate}</pubDate>
      <author>team@neutron.build (${post.data.author})</author>
      ${tags}
    </item>`;
  }).join('');

  const rss = `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">
  <channel>
    <title>Neutron Blog</title>
    <description>Insights on full-stack development, database architecture, performance engineering, and the Neutron ecosystem.</description>
    <link>${siteUrl}/blog</link>
    <atom:link href="${siteUrl}/rss.xml" rel="self" type="application/rss+xml"/>
    <language>en-us</language>
    <lastBuildDate>${new Date().toUTCString()}</lastBuildDate>
    <managingEditor>team@neutron.build (Neutron Team)</managingEditor>
    <webMaster>team@neutron.build (Neutron Team)</webMaster>
    <image>
      <url>${siteUrl}/favicon.svg</url>
      <title>Neutron Blog</title>
      <link>${siteUrl}/blog</link>
    </image>
    ${items}
  </channel>
</rss>`;

  return new Response(rss, {
    headers: {
      'Content-Type': 'application/xml; charset=utf-8',
      'Cache-Control': 'public, max-age=3600',
    },
  });
}
