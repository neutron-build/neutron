import { summarize } from "@neutron-build/cms";
import cms from "../cms";

export const config = { mode: "app", hydrate: false };

export async function loader() {
  return {
    posts: cms.posts().map((post) => ({
      slug: post.slug!,
      title: post.title,
      date: post.published_at!.slice(0, 10),
      summary: summarize(post),
    })),
  };
}

type Data = Awaited<ReturnType<typeof loader>>;

export default function Home(props: { data?: Data }) {
  const posts = props.data?.posts ?? [];
  return (
    <>
      <h1>Posts</h1>
      {posts.length ? (
        <ul className="posts">
          {posts.map((post) => (
            <li key={post.slug}>
              <time dateTime={post.date}>{post.date}</time>
              <h2><a href={`/posts/${post.slug}`}>{post.title}</a></h2>
              {post.summary ? <p>{post.summary}</p> : null}
            </li>
          ))}
        </ul>
      ) : (
        <p className="muted">No posts yet. Write the first one at <a href="/n-admin">/n-admin</a>.</p>
      )}
    </>
  );
}
