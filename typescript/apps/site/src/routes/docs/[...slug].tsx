import { getCollection, getEntry } from "@neutron-build/core";

// Read from loader data (built already) instead of calling getEntry here —
// an async head() importing the content runtime pulls server-only code
// (chokidar/fsevents) into the client bundle and breaks the build.
export function head({ data, loaderData }: any) {
  const d: any = data ?? (loaderData ? Object.values(loaderData)[0] : null);
  const title = d?.entry?.data?.title;
  return {
    title: title ? `${title} — Neutron Docs` : "Docs — Neutron",
    ...(d?.entry?.data?.description ? { description: d.entry.data.description } : {}),
  };
}

export async function getStaticPaths() {
  const docs = await getCollection("docs");
  const paths = [];
  for (const doc of docs) {
    try {
      const { Content } = await (doc as any).render();
      paths.push({ params: { slug: doc.slug }, props: { entry: doc, Content } });
    } catch {
      paths.push({ params: { slug: doc.slug }, props: { entry: doc, Content: null } });
    }
  }
  return paths;
}

export async function loader({ params }: { params: { slug: string } }) {
  const entry = await getEntry("docs", params.slug);
  if (!entry) throw new Error(`Doc not found: ${params.slug}`);
  const { Content } = await (entry as any).render();
  return { entry, Content };
}

interface DocEntry {
  data: {
    title: string;
    description?: string;
  };
  slug: string;
}

export default function DocPage({ data }: { data: { entry: DocEntry; Content: any } }) {
  return (
    <article>
      <h1>{data.entry.data.title}</h1>
      {data.Content ? <data.Content /> : <p>Content unavailable.</p>}
    </article>
  );
}
