import { getCollection, getEntry } from "@neutron-build/core";

export async function getStaticPaths() {
  const posts = await getCollection('blog', ({ data }: any) => !data.draft);
  return posts.map((post: any) => ({
    params: { slug: post.slug },
  }));
}

export async function loader({ params }: { params: { slug: string } }) {
  const post = await getEntry('blog', params.slug);
  const { html } = await post.render();
  const { title, description, pubDate, author, tags } = post.data;

  const formattedDate = pubDate.toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'long',
    day: 'numeric',
  });

  const jsonLd = JSON.stringify({
    '@context': 'https://schema.org',
    '@type': 'BlogPosting',
    headline: title,
    description: description,
    datePublished: pubDate.toISOString(),
    author: { '@type': 'Organization', name: author },
    publisher: { '@type': 'Organization', name: 'Neutron', url: 'https://neutron.build' },
    url: `https://neutron.build/blog/${params.slug}`,
  });

  return {
    title,
    description,
    pubDate: pubDate.toISOString(),
    author: author || 'Neutron Team',
    tags: tags || [],
    formattedDate,
    jsonLd,
    html,
  };
}

interface LoaderData {
  title: string;
  description: string;
  pubDate: string;
  author: string;
  tags: string[];
  formattedDate: string;
  jsonLd: string;
  html: string;
}

export default function BlogPost({ data }: { data: LoaderData }) {
  const { title, description, pubDate, author, tags, formattedDate, jsonLd, html } = data;

  return (
    <main id="main-content">
      <article class="post">
        <div class="post__header">
          <div class="container-narrow">
            <a href="/blog" class="back-link">
              &larr; All posts
            </a>

            {tags && tags.length > 0 && (
              <div class="tags">
                {tags.map((tag: string) => (
                  <span class="tag" key={tag}>
                    {tag}
                  </span>
                ))}
              </div>
            )}

            <h1 class="post__title">{title}</h1>
            <p class="post__desc">{description}</p>

            <div class="byline">
              <span class="byline__author">{author}</span>
              <span class="byline__sep">&middot;</span>
              <time dateTime={pubDate} class="byline__date">
                {formattedDate}
              </time>
            </div>
          </div>
        </div>

        <div
          class="post__body container-narrow"
          dangerouslySetInnerHTML={{ __html: html }}
        />
      </article>

      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: jsonLd }}
      />
    </main>
  );
}
