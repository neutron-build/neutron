import { getCollection, getEntry } from "neutron/content";
import { extractToc } from "neutron";
import { calculateReadingTime } from "neutron";
import { Toc } from "../../components/Toc";
import { Footer } from "../../components/Footer";
import { Breadcrumbs } from "../../components/Breadcrumbs";
import { getPagination } from "../../lib/pagination";

export const config = { mode: "static" };

export async function getStaticPaths() {
  const docs = await getCollection("docs");
  return {
    paths: docs
      .filter((d: any) => !d.data.draft)
      .map((d: any) => ({ params: { "*": d.slug } })),
  };
}

export async function loader({ params }: { params: Record<string, string> }) {
  const slug = params["*"] || "index";
  const entry = await getEntry("docs", slug);
  if (!entry) throw new Response("Not found", { status: 404 });

  const toc = extractToc(entry.html);
  const readingTime = calculateReadingTime(entry.body);

  const docs = await getCollection("docs");
  const allEntries = docs
    .filter((d: any) => !d.data.draft)
    .map((d: any) => ({ slug: d.slug, data: d.data }));
  const pagination = getPagination(allEntries, slug);

  return {
    title: entry.data.title,
    description: entry.data.description,
    html: entry.html,
    slug,
    toc,
    readingTime,
    pagination,
  };
}

export function head({ data }: { data: { title: string; description?: string } }) {
  return {
    title: data.title,
    description: data.description,
  };
}

export default function DocPage({
  data,
}: {
  data: {
    title: string;
    html: string;
    slug: string;
    toc: { id: string; text: string; level: number }[];
    readingTime?: { text: string; minutes: number };
    pagination: {
      prev?: { title: string; slug: string };
      next?: { title: string; slug: string };
    };
  };
}) {
  return (
    <>
      <div class="docs-main">
        <Breadcrumbs slug={data.slug} />
        <article class="prose">
          <h1>{data.title}</h1>
          {data.readingTime && (
            <p class="reading-time">{data.readingTime.text}</p>
          )}
          <div dangerouslySetInnerHTML={{ __html: data.html }} />
        </article>
        <Footer prev={data.pagination.prev} next={data.pagination.next} />
      </div>
      <div class="docs-toc-wrapper">
        <Toc entries={data.toc} />
      </div>
    </>
  );
}
