import ProductOverview from "./ProductOverview";

interface LanguageOverviewProps {
  name: string;
  description: string;
  status?: "available" | "in-progress" | "planned";
  accent: string;
  accentRgb: string;
  docsHref: string;
  quickstartHref?: string;
  facts: Array<{ label: string; value: string }>;
  note?: string;
}

export default function LanguageOverview({
  name,
  description,
  status = "available",
  accent,
  accentRgb,
  docsHref,
  quickstartHref,
  facts,
  note,
}: LanguageOverviewProps) {
  return (
    <ProductOverview
      title={`Neutron ${name}`}
      description={description}
      category="language"
      status={status}
      accent={accent}
      accentRgb={accentRgb}
      facts={facts}
      note={note}
      links={[
        ...(quickstartHref ? [{ label: "Quickstart", href: quickstartHref }] : []),
        { label: "Documentation", href: docsHref },
        { label: "Source", href: `https://github.com/neutron-build/neutron/tree/main/${name.toLowerCase()}` },
      ]}
    />
  );
}
