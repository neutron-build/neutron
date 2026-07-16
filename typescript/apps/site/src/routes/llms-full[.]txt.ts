import { getCollection } from "@neutron-build/core";

const SITE = "https://neutron.build";

// Full documentation corpus as one plain-text/markdown file, for LLM agents
// that want to ingest everything in a single fetch. Generated at build time
// from the docs collection, so it never drifts from the source.
// The curated, link-based index lives at /llms.txt (llmstxt.org convention).
export async function loader() {
  const docs = (await getCollection("docs")).sort((a: any, b: any) =>
    a.slug.localeCompare(b.slug),
  );

  const header = `# Neutron Documentation (full text)

> The complete Neutron documentation, concatenated for LLM ingestion. Neutron
> is a multi-language full-stack framework backed by Nucleus, a multi-model
> database. The curated link index is at ${SITE}/llms.txt. Each section below
> is one documentation page; its canonical URL is given under the heading.

`;

  const sections = docs
    .map((doc: any) => {
      const title = doc.data?.title ?? doc.slug;
      const description = doc.data?.description
        ? `${doc.data.description}\n`
        : "";
      return `\n---\n\n## ${title}\n\nSource: ${SITE}/docs/${doc.slug}\n${description}\n${doc.body.trim()}\n`;
    })
    .join("");

  const body = header + sections;

  return new Response(body, {
    headers: {
      "Content-Type": "text/plain; charset=utf-8",
      "Cache-Control": "public, max-age=3600",
    },
  });
}
