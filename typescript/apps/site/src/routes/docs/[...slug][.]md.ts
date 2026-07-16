import { getCollection, getEntry } from "@neutron-build/core";

// Per-page raw markdown at /docs/<slug>.md — the agent-fetchable source for a
// single doc page (companion to /llms-full.txt, which is the whole corpus).
export async function getStaticPaths() {
  const docs = await getCollection("docs");
  return docs.map((doc: any) => ({ params: { slug: doc.slug } }));
}

export async function loader({ params }: { params: { slug: string } }) {
  const entry = await getEntry("docs", params.slug);
  if (!entry) {
    return new Response(`Not found: ${params.slug}`, {
      status: 404,
      headers: { "Content-Type": "text/plain; charset=utf-8" },
    });
  }
  const title = (entry as any).data?.title ?? params.slug;
  const body = ((entry as any).body ?? "").trim();
  const markdown = `# ${title}\n\n${body}\n`;
  return new Response(markdown, {
    headers: {
      "Content-Type": "text/markdown; charset=utf-8",
      "Cache-Control": "public, max-age=3600",
    },
  });
}
