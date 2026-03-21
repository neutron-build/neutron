export interface PaginationLink {
  title: string;
  slug: string;
}

interface EntryInput {
  slug: string;
  data: {
    title: string;
    order?: number;
  };
}

export function getPagination(
  entries: EntryInput[],
  currentSlug: string,
): { prev?: PaginationLink; next?: PaginationLink } {
  const sorted = [...entries].sort(
    (a, b) => (a.data.order ?? 999) - (b.data.order ?? 999) || a.slug.localeCompare(b.slug),
  );

  const index = sorted.findIndex((e) => e.slug === currentSlug);

  if (index === -1) {
    return {};
  }

  const prev =
    index > 0
      ? { title: sorted[index - 1]!.data.title, slug: sorted[index - 1]!.slug }
      : undefined;

  const next =
    index < sorted.length - 1
      ? { title: sorted[index + 1]!.data.title, slug: sorted[index + 1]!.slug }
      : undefined;

  return { prev, next };
}
