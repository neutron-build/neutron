interface BlogCardProps {
  title: string;
  description: string;
  pubDate: Date;
  author: string;
  tags: string[];
  slug: string;
  featured?: boolean;
}

export default function BlogCard({
  title,
  description,
  pubDate,
  author,
  tags,
  slug,
  featured = false,
}: BlogCardProps) {
  const formattedDate = pubDate.toLocaleDateString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
  });

  return (
    <a
      href={"/blog/" + slug}
      class={`blog-card${featured ? " featured" : ""}`}
    >
      <div class="card-top">
        <div class="tags">
          {tags.map((tag) => (
            <span class="tag" key={tag}>
              {tag}
            </span>
          ))}
        </div>
        <time class="date" dateTime={pubDate.toISOString()}>
          {formattedDate}
        </time>
      </div>

      <h2 class="title">{title}</h2>
      <p class="description">{description}</p>

      <div class="card-bottom">
        <span class="author">{author}</span>
        <span class="read-more">Read more &rarr;</span>
      </div>
    </a>
  );
}
