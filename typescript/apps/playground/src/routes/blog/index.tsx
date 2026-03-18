import type { GetStaticPathsResult } from "neutron";

export const config = { mode: "static" };

interface Post {
  slug: string;
  title: string;
  date: string;
}

const posts: Post[] = [
  { slug: "getting-started", title: "Getting Started with Neutron", date: "2024-01-15" },
  { slug: "static-vs-app-routes", title: "Static vs App Routes: When to Use Each", date: "2024-01-20" },
  { slug: "loaders-explained", title: "Loaders Explained: Server-Side Data Loading", date: "2024-02-01" },
];

export async function loader() {
  return { posts };
}

interface LoaderData {
  posts: Post[];
}

export default function BlogIndex({ data }: { data: LoaderData }) {
  return (
    <div>
      <h1>Blog</h1>
      <p style="margin-bottom: 2rem; color: #888;">
        Thoughts on web development, frameworks, and building for the modern web.
      </p>
      
      <ul style="list-style: none; padding: 0;">
        {data?.posts.map((post) => (
          <li key={post.slug} style="margin-bottom: 1.5rem; padding-bottom: 1.5rem; border-bottom: 1px solid #333;">
            <a href={`/blog/${post.slug}`} style="font-size: 1.25rem; color: #EDEDED; text-decoration: none;">
              {post.title}
            </a>
            <br />
            <time style="color: #888; font-size: 0.875rem;">{post.date}</time>
          </li>
        ))}
      </ul>
    </div>
  );
}
