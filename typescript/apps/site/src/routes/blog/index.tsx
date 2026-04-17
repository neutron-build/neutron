import { getCollection } from "@neutron-build/core";

interface BlogPost {
  slug: string;
  data: {
    title: string;
    description: string;
    pubDate: Date;
    author: string;
    tags: string[];
    draft?: boolean;
  };
}

function formatDate(date: Date): string {
  return date.toLocaleDateString("en-US", {
    year: "numeric",
    month: "long",
    day: "numeric",
  });
}

export async function loader() {
  const posts = (await getCollection('blog', ({ data }: any) => !data.draft))
    .sort((a: any, b: any) => b.data.pubDate.valueOf() - a.data.pubDate.valueOf());
  return { posts };
}

export default function BlogIndex({ data }: { data: { posts: BlogPost[] } }) {
  const posts = data?.posts ?? [];

  return (
    <main id="main-content">
      <section class="blog-hero">
        <div class="container">
          <p class="blog-hero__eyebrow section-label" data-animate>
            The Neutron Blog
          </p>
          <h1
            class="blog-hero__title"
            data-animate
            style={{ "--animate-delay": "0.1s" } as any}
          >
            Insights from the team
          </h1>
          <p
            class="blog-hero__sub"
            data-animate
            style={{ "--animate-delay": "0.2s" } as any}
          >
            Deep dives into framework development, database engineering,
            performance optimization, and building multi-language systems
            at scale.
          </p>
        </div>
      </section>

      <section class="blog-posts">
        <div class="container">
          {posts.length === 0 ? (
            <div class="blog-empty" data-animate>
              <h2 class="blog-empty__title">Coming soon</h2>
              <p class="blog-empty__text">
                We are working on our first posts. Check back soon for
                deep dives into Nucleus internals, framework patterns, and
                performance engineering.
              </p>
              <a href="/docs" class="btn btn--ghost">
                Read the docs in the meantime &rarr;
              </a>
            </div>
          ) : (
            <div class="post-grid">
              {posts.map((post, i) => (
                <a
                  href={`/blog/${post.slug}`}
                  class="post-card"
                  data-animate
                  style={{ "--animate-delay": `${(i * 0.07).toFixed(2)}s` } as any}
                  key={post.slug}
                >
                  <div
                    class="post-card__accent"
                    aria-hidden="true"
                  ></div>
                  <div class="post-card__body">
                    <div class="post-card__meta">
                      <time
                        class="post-card__date"
                        dateTime={post.data.pubDate.toISOString()}
                      >
                        {formatDate(post.data.pubDate)}
                      </time>
                      {post.data.author && (
                        <span class="post-card__author">
                          {post.data.author}
                        </span>
                      )}
                    </div>

                    <h2 class="post-card__title">
                      {post.data.title}
                    </h2>

                    {post.data.description && (
                      <p class="post-card__description">
                        {post.data.description}
                      </p>
                    )}

                    {post.data.tags && post.data.tags.length > 0 && (
                      <div
                        class="post-card__tags"
                        aria-label="Tags"
                      >
                        {post.data.tags.map((tag: string) => (
                          <span class="post-card__tag" key={tag}>
                            {tag}
                          </span>
                        ))}
                      </div>
                    )}

                    <span
                      class="post-card__read-more"
                      aria-hidden="true"
                    >
                      Read more &rarr;
                    </span>
                  </div>
                </a>
              ))}
            </div>
          )}
        </div>
      </section>
    </main>
  );
}
