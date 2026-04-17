interface TagBadgeProps {
  tag: string;
  href?: string;
}

export default function TagBadge({ tag, href }: TagBadgeProps) {
  if (href) {
    return (
      <a href={href} class="tag-badge tag-badge--link">
        {tag}
      </a>
    );
  }

  return <span class="tag-badge">{tag}</span>;
}
