import { Link } from "@neutron-build/core/client";

interface PaginationLink {
  title: string;
  slug: string;
}

export function Footer({
  prev,
  next,
}: {
  prev?: PaginationLink;
  next?: PaginationLink;
}) {
  if (!prev && !next) return null;

  return (
    <footer class="docs-footer">
      {prev ? (
        <Link to={`/docs/${prev.slug}`} class="docs-footer-link">
          <span class="docs-footer-label">
            <svg
              width="14"
              height="14"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              stroke-width="2"
              stroke-linecap="round"
              stroke-linejoin="round"
              style="display:inline;vertical-align:middle;margin-right:0.25rem"
            >
              <line x1="19" y1="12" x2="5" y2="12" />
              <polyline points="12 19 5 12 12 5" />
            </svg>
            Previous
          </span>
          <span class="docs-footer-title">{prev.title}</span>
        </Link>
      ) : (
        <span />
      )}
      {next ? (
        <Link
          to={`/docs/${next.slug}`}
          class="docs-footer-link docs-footer-link--next"
        >
          <span class="docs-footer-label">
            Next
            <svg
              width="14"
              height="14"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              stroke-width="2"
              stroke-linecap="round"
              stroke-linejoin="round"
              style="display:inline;vertical-align:middle;margin-left:0.25rem"
            >
              <line x1="5" y1="12" x2="19" y2="12" />
              <polyline points="12 5 19 12 12 19" />
            </svg>
          </span>
          <span class="docs-footer-title">{next.title}</span>
        </Link>
      ) : (
        <span />
      )}
    </footer>
  );
}
