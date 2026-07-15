import { getCollection } from "@neutron-build/core";

const SITE = "https://neutron.build";

// Top-level marketing + product routes. Keep in sync with src/routes/*.tsx.
const STATIC_PATHS = [
  "agents", "ai", "cli", "client", "desktop", "elixir", "go", "julia",
  "lean", "modelica", "mojo", "native", "nucleus", "orm", "python", "quint",
  "rust", "studio", "typescript", "web", "workflow", "zig",
  "docs", "blog",
];

interface Url {
  loc: string;
  changefreq: string;
  priority: string;
}

export async function loader() {
  const docs = await getCollection("docs");
  const posts = await getCollection("blog", ({ data }: any) => !data.draft);

  const urls: Url[] = [{ loc: `${SITE}/`, changefreq: "weekly", priority: "1.0" }];

  for (const path of STATIC_PATHS) {
    urls.push({ loc: `${SITE}/${path}`, changefreq: "weekly", priority: "0.8" });
  }
  for (const doc of docs) {
    urls.push({ loc: `${SITE}/docs/${(doc as any).slug}`, changefreq: "weekly", priority: "0.7" });
  }
  for (const post of posts) {
    urls.push({ loc: `${SITE}/blog/${(post as any).slug}`, changefreq: "monthly", priority: "0.6" });
  }

  const body = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls
  .map(
    (u) =>
      `  <url>\n    <loc>${u.loc}</loc>\n    <changefreq>${u.changefreq}</changefreq>\n    <priority>${u.priority}</priority>\n  </url>`,
  )
  .join("\n")}
</urlset>`;

  return new Response(body, {
    headers: {
      "Content-Type": "application/xml; charset=utf-8",
      "Cache-Control": "public, max-age=3600",
    },
  });
}
