const posts = [
  { slug: "launch", title: "Launch Day" },
  { slug: "roadmap", title: "Roadmap" },
];

export const config = { mode: "static" };

export async function loader() {
  return { posts };
}

export default function BlogIndex(props: {
  data?: { posts: Array<{ slug: string; title: string }> };
}) {
  return (
    <section>
      <h2>Blog</h2>
      <ul>
        {props.data?.posts.map((post) => (
          <li key={post.slug}>
            <a href={"/blog/" + post.slug}>{post.title}</a>
          </li>
        ))}
      </ul>
    </section>
  );
}
