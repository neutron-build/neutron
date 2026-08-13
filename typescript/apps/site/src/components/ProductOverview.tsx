import ProductPage from "./ProductPage";

interface ProductOverviewProps {
  title: string;
  description: string;
  category: "language" | "platform" | "database" | "tool";
  status?: "available" | "in-progress" | "planned";
  accent: string;
  accentRgb: string;
  facts: Array<{ label: string; value: string }>;
  links: Array<{ label: string; href: string }>;
  note?: string;
}

export default function ProductOverview({
  title,
  description,
  category,
  status = "available",
  accent,
  accentRgb,
  facts,
  links,
  note,
}: ProductOverviewProps) {
  const primaryAction = links.find((link) =>
    /documentation|quickstart|command reference|data overview|package source|source/i.test(link.label)
  );
  const sourceAction = links.find((link) =>
    /^(package )?source$/i.test(link.label) && link.href !== primaryAction?.href
  );

  return (
    <ProductPage
      title={title}
      description={description}
      category={category}
      status={status}
      accent={accent}
      heroAccentRgb={accentRgb}
      actions={[primaryAction, sourceAction].filter(Boolean) as Array<{ label: string; href: string }>}
    >
      <section class="language-overview">
        <h2>Overview</h2>
        <dl class="language-facts">
          {facts.map((fact) => (
            <div key={fact.label}>
              <dt>{fact.label}</dt>
              <dd>{fact.value}</dd>
            </div>
          ))}
        </dl>
        {note && <p class="language-overview__note">{note}</p>}
        <div class="language-overview__links">
          {links.map((link) => <a href={link.href} key={link.href}>{link.label}</a>)}
        </div>
      </section>
    </ProductPage>
  );
}
