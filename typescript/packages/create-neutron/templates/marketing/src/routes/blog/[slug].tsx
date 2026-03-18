const posts = [
  { slug: "launch", title: "Launch Day", body: "We launched a static-first site." },
  { slug: "roadmap", title: "Roadmap", body: "Next we add app routes where needed." },
];

export const config = { mode: "static" };

export async function getStaticPaths() {
  return {
    paths: posts.map((post) => ({
      params: { slug: post.slug },
      props: { post },
    })),
  };
}

export async function loader({ params }: { params: { slug?: string } }) {
  const post = posts.find((entry) => entry.slug === params.slug) || posts[0];
  return { post };
}

export default function BlogPost(props: {
  data?: { post: { title: string; body: string } };
}) {
  return (
    <article>
      <h2>{props.data?.post?.title}</h2>
      <p>{props.data?.post?.body}</p>
      <p>
        <a href="/blog">Back to blog</a>
      </p>
    </article>
  );
}
