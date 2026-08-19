import { Link } from "@neutron-build/core/client";

function toTitle(segment: string): string {
  return segment
    .replace(/[-_]/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

export function Breadcrumbs({ slug }: { slug: string }) {
  const parts = slug.split("/").filter(Boolean);
  if (parts.length === 0 || (parts.length === 1 && parts[0] === "index")) {
    return null;
  }

  const crumbs: { label: string; href: string }[] = [
    { label: "Docs", href: "/docs" },
  ];

  let path = "";
  for (let i = 0; i < parts.length; i++) {
    path += `/${parts[i]}`;
    const isLast = i === parts.length - 1;
    if (parts[i] !== "index") {
      crumbs.push({
        label: toTitle(parts[i]!),
        href: isLast ? "" : `/docs${path}`,
      });
    }
  }

  return (
    <nav class="docs-breadcrumbs" aria-label="Breadcrumb">
      {crumbs.map((crumb, i) => (
        <>
          {i > 0 && <span class="docs-breadcrumbs-sep">/</span>}
          {crumb.href ? (
            <Link to={crumb.href}>{crumb.label}</Link>
          ) : (
            <span class="docs-breadcrumbs-current" aria-current="page">
              {crumb.label}
            </span>
          )}
        </>
      ))}
    </nav>
  );
}
