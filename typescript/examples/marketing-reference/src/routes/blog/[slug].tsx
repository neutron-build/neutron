import { getCollection, getEntry } from "neutron/content";

export const config = { mode: "static" };

export async function getStaticPaths() {
  const posts = await getCollection("blog");
  return {
    paths: posts.map((post) => ({
      params: { slug: post.slug },
    })),
  };
}

export async function loader({
  params,
}: {
  params: Record<string, string>;
}) {
  const entry = await getEntry("blog", params.slug);
  if (!entry) {
    throw new Response("Not found", { status: 404 });
  }

  return {
    title: entry.data.title as string,
    html: entry.html,
  };
}

export default function BlogPost({
  data,
}: {
  data: { title: string; html: string };
}) {
  return (
    <article>
      <h1>{data.title}</h1>
      <div dangerouslySetInnerHTML={{ __html: data.html }} />
    </article>
  );
}
