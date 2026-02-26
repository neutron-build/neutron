import { getCollection } from "neutron/content";

export const config = { mode: "static" };

export async function loader() {
  const posts = await getCollection("blog");
  return {
    posts: posts
      .filter((post) => !post.data.draft)
      .map((post) => ({
        slug: post.slug,
        title: post.data.title,
      })),
  };
}

export default function BlogIndex({
  data,
}: {
  data: {
    posts: Array<{ slug: string; title: string }>;
  };
}) {
  return (
    <section>
      <h1>Blog</h1>
      <ul>
        {data.posts.map((post) => (
          <li key={post.slug}>
            <a href={`/blog/${post.slug}`}>{post.title}</a>
          </li>
        ))}
      </ul>
    </section>
  );
}
