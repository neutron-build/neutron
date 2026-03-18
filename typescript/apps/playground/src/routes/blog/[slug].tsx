import type { GetStaticPathsResult } from "neutron";

export const config = { mode: "static" };

interface Post {
  slug: string;
  title: string;
  content: string;
  date: string;
}

// Simulated blog posts - in a real app, this would come from a CMS or filesystem
const posts: Post[] = [
  {
    slug: "getting-started",
    title: "Getting Started with Neutron",
    content: "Neutron is a modern web framework that combines the best of static and dynamic rendering...",
    date: "2024-01-15",
  },
  {
    slug: "static-vs-app-routes",
    title: "Static vs App Routes: When to Use Each",
    content: "Understanding when to use static vs app routes is key to building performant websites...",
    date: "2024-01-20",
  },
  {
    slug: "loaders-explained",
    title: "Loaders Explained: Server-Side Data Loading",
    content: "Loaders are the heart of data loading in Neutron. They run on the server...",
    date: "2024-02-01",
  },
];

export async function getStaticPaths(): Promise<GetStaticPathsResult> {
  return {
    paths: posts.map((post) => ({
      params: { slug: post.slug },
      props: { post },
    })),
  };
}

interface Props {
  data?: { post: Post };
  params?: { slug: string };
}

export default function BlogPost(props: Props) {
  const post = props?.data?.post;
  
  return (
    <article>
      <header>
        <h1>{post?.title || "Untitled"}</h1>
        <time datetime={post?.date} style="color: #888; font-size: 0.875rem;">
          {post?.date || ""}
        </time>
      </header>
      <p style="margin-top: 1rem;">{post?.content || ""}</p>
      <p style="margin-top: 2rem;">
        <a href="/blog">← Back to blog</a>
      </p>
    </article>
  );
}
