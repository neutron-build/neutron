export interface SeoMetaInput {
  title?: string;
  description?: string;
  canonical?: string;
  noindex?: boolean;
  keywords?: string[];
  openGraph?: {
    title?: string;
    description?: string;
    image?: string;
    type?: string;
    url?: string;
  };
  twitter?: {
    card?: "summary" | "summary_large_image";
    title?: string;
    description?: string;
    image?: string;
  };
  /** JSON-LD structured data objects to inject as <script type="application/ld+json"> */
  jsonLd?: object | object[];
}

export type SeoTag =
  | { tag: "title"; content: string }
  | { tag: "meta"; attrs: Record<string, string> }
  | { tag: "link"; attrs: Record<string, string> };

export function buildMetaTags(input: SeoMetaInput): SeoTag[] {
  const tags: SeoTag[] = [];

  if (input.title) {
    tags.push({ tag: "title", content: input.title });
  }

  if (input.description) {
    tags.push({ tag: "meta", attrs: { name: "description", content: input.description } });
  }

  if (input.keywords && input.keywords.length > 0) {
    tags.push({
      tag: "meta",
      attrs: { name: "keywords", content: input.keywords.join(", ") },
    });
  }

  if (input.canonical) {
    tags.push({ tag: "link", attrs: { rel: "canonical", href: input.canonical } });
  }

  if (input.noindex) {
    tags.push({ tag: "meta", attrs: { name: "robots", content: "noindex, nofollow" } });
  }

  if (input.openGraph) {
    const og = input.openGraph;
    pushMeta(tags, "property", "og:title", og.title || input.title);
    pushMeta(tags, "property", "og:description", og.description || input.description);
    pushMeta(tags, "property", "og:image", og.image);
    pushMeta(tags, "property", "og:type", og.type || "website");
    pushMeta(tags, "property", "og:url", og.url || input.canonical);
  }

  if (input.twitter) {
    const tw = input.twitter;
    pushMeta(tags, "name", "twitter:card", tw.card || "summary_large_image");
    pushMeta(tags, "name", "twitter:title", tw.title || input.title);
    pushMeta(tags, "name", "twitter:description", tw.description || input.description);
    pushMeta(tags, "name", "twitter:image", tw.image || input.openGraph?.image);
  }

  return dedupeMetaTags(tags);
}

function pushMeta(
  tags: SeoTag[],
  keyName: "name" | "property",
  keyValue: string,
  content: string | undefined
): void {
  if (!content) {
    return;
  }
  tags.push({
    tag: "meta",
    attrs: {
      [keyName]: keyValue,
      content,
    },
  });
}

function dedupeMetaTags(tags: SeoTag[]): SeoTag[] {
  const seen = new Set<string>();
  const output: SeoTag[] = [];

  for (const tag of tags) {
    let key = tag.tag;
    if (tag.tag === "meta") {
      key += `:${tag.attrs.name || tag.attrs.property || ""}`;
    } else if (tag.tag === "link") {
      key += `:${tag.attrs.rel || ""}`;
    }
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    output.push(tag);
  }

  return output;
}

export function renderMetaTags(tags: SeoTag[]): string {
  return tags
    .map((tag) => {
      if (tag.tag === "title") {
        return `<title>${escapeHtml(tag.content)}</title>`;
      }
      const attrs = Object.entries(tag.attrs)
        .map(([name, value]) => `${name}="${escapeHtml(value)}"`)
        .join(" ");
      return `<${tag.tag} ${attrs}>`;
    })
    .join("\n");
}

export function mergeSeoMetaInput(
  base: SeoMetaInput | null | undefined,
  override: SeoMetaInput | null | undefined
): SeoMetaInput | null {
  if (!base && !override) {
    return null;
  }

  if (!base) {
    return cloneSeoMetaInput(override!);
  }

  if (!override) {
    return cloneSeoMetaInput(base);
  }

  const merged: SeoMetaInput = {
    ...base,
    ...override,
  };

  if (base.keywords || override.keywords) {
    merged.keywords = [...(override.keywords || base.keywords || [])];
  }

  const mergedOg = mergeSeoNested(base.openGraph, override.openGraph);
  if (mergedOg) {
    merged.openGraph = mergedOg;
  } else {
    delete merged.openGraph;
  }

  const mergedTwitter = mergeSeoNested(base.twitter, override.twitter);
  if (mergedTwitter) {
    merged.twitter = mergedTwitter;
  } else {
    delete merged.twitter;
  }

  // Concatenate jsonLd arrays from both base and override
  const baseJsonLd = base.jsonLd ? (Array.isArray(base.jsonLd) ? base.jsonLd : [base.jsonLd]) : [];
  const overrideJsonLd = override.jsonLd ? (Array.isArray(override.jsonLd) ? override.jsonLd : [override.jsonLd]) : [];
  const allJsonLd = [...baseJsonLd, ...overrideJsonLd];
  if (allJsonLd.length > 0) {
    merged.jsonLd = allJsonLd;
  } else {
    delete merged.jsonLd;
  }

  return merged;
}

export function inferPageTitle(pathname: string): string {
  const normalized = normalizePathname(pathname);
  if (normalized === "/") {
    return "Home";
  }
  return normalized.slice(1).replace(/\//g, " - ");
}

export function renderDocumentHead(
  pathname: string,
  seo: SeoMetaInput | null | undefined,
  headFragments: string[] = []
): string {
  const seoTags = seo ? buildMetaTags(seo) : [];
  const seoHtml = seoTags.length > 0 ? renderMetaTags(seoTags) : "";
  const extraHead = headFragments
    .map((fragment) => fragment.trim())
    .filter(Boolean)
    .join("\n");

  const hasTitle =
    /<title(?:\s|>)/i.test(seoHtml) ||
    /<title(?:\s|>)/i.test(extraHead);
  const titleTag = hasTitle
    ? ""
    : `<title>${escapeHtml(inferPageTitle(pathname))} - Neutron</title>`;

  // Render JSON-LD structured data scripts
  const jsonLdHtml = renderJsonLd(seo?.jsonLd);

  return [
    '<meta charset="UTF-8">',
    '<meta name="viewport" content="width=device-width, initial-scale=1.0">',
    titleTag,
    seoHtml,
    extraHead,
    jsonLdHtml,
  ]
    .filter(Boolean)
    .join("\n");
}

export interface SitemapEntry {
  url: string;
  lastmod?: string | Date;
  changefreq?:
    | "always"
    | "hourly"
    | "daily"
    | "weekly"
    | "monthly"
    | "yearly"
    | "never";
  priority?: number;
}

export interface SitemapOptions {
  xmlns?: string;
}

export function buildSitemapXml(entries: SitemapEntry[], options: SitemapOptions = {}): string {
  const xmlns = options.xmlns || "http://www.sitemaps.org/schemas/sitemap/0.9";
  const lines = [
    '<?xml version="1.0" encoding="UTF-8"?>',
    `<urlset xmlns="${escapeHtml(xmlns)}">`,
  ];

  for (const entry of entries) {
    lines.push("  <url>");
    lines.push(`    <loc>${escapeXml(entry.url)}</loc>`);
    if (entry.lastmod) {
      const normalized = normalizeDate(entry.lastmod);
      lines.push(`    <lastmod>${escapeXml(normalized)}</lastmod>`);
    }
    if (entry.changefreq) {
      lines.push(`    <changefreq>${entry.changefreq}</changefreq>`);
    }
    if (typeof entry.priority === "number") {
      lines.push(`    <priority>${Math.max(0, Math.min(1, entry.priority)).toFixed(1)}</priority>`);
    }
    lines.push("  </url>");
  }

  lines.push("</urlset>");
  return lines.join("\n");
}

export interface RobotsRule {
  userAgent: string;
  allow?: string[];
  disallow?: string[];
  crawlDelay?: number;
}

export interface RobotsOptions {
  host?: string;
  sitemap?: string | string[];
  rules: RobotsRule[];
}

export function buildRobotsTxt(options: RobotsOptions): string {
  const lines: string[] = [];

  for (const rule of options.rules) {
    lines.push(`User-agent: ${rule.userAgent}`);
    for (const allow of rule.allow || []) {
      lines.push(`Allow: ${allow}`);
    }
    for (const disallow of rule.disallow || []) {
      lines.push(`Disallow: ${disallow}`);
    }
    if (typeof rule.crawlDelay === "number") {
      lines.push(`Crawl-delay: ${rule.crawlDelay}`);
    }
    lines.push("");
  }

  if (options.host) {
    lines.push(`Host: ${options.host}`);
  }

  const sitemaps = Array.isArray(options.sitemap)
    ? options.sitemap
    : options.sitemap
      ? [options.sitemap]
      : [];
  for (const sitemap of sitemaps) {
    lines.push(`Sitemap: ${sitemap}`);
  }

  return lines.join("\n").trimEnd() + "\n";
}

function normalizeDate(input: string | Date): string {
  const date = input instanceof Date ? input : new Date(input);
  if (Number.isNaN(date.getTime())) {
    return String(input);
  }
  return date.toISOString();
}

function cloneSeoMetaInput(input: SeoMetaInput): SeoMetaInput {
  return {
    ...input,
    ...(input.keywords ? { keywords: [...input.keywords] } : {}),
    ...(input.openGraph ? { openGraph: { ...input.openGraph } } : {}),
    ...(input.twitter ? { twitter: { ...input.twitter } } : {}),
    ...(input.jsonLd ? { jsonLd: Array.isArray(input.jsonLd) ? [...input.jsonLd] : input.jsonLd } : {}),
  };
}

function renderJsonLd(jsonLd: object | object[] | undefined): string {
  if (!jsonLd) return "";
  const items = Array.isArray(jsonLd) ? jsonLd : [jsonLd];
  if (items.length === 0) return "";
  return items
    .map((item) => {
      const json = JSON.stringify(item)
        .replace(/</g, "\\u003c")
        .replace(/>/g, "\\u003e")
        .replace(/&/g, "\\u0026");
      return `<script type="application/ld+json">${json}</script>`;
    })
    .join("\n");
}

function mergeSeoNested<T extends Record<string, unknown>>(
  base: T | undefined,
  override: T | undefined
): T | undefined {
  if (!base && !override) {
    return undefined;
  }
  if (!base) {
    return { ...override } as T;
  }
  if (!override) {
    return { ...base };
  }
  return { ...base, ...override };
}

function normalizePathname(pathname: string): string {
  const value = String(pathname || "/");
  if (value === "/") {
    return "/";
  }
  const trimmed = value.replace(/\/+$/, "");
  return trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
}

function escapeXml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

// ── JSON-LD Schema Helpers ──

/**
 * Build a Schema.org BreadcrumbList JSON-LD object.
 */
export function breadcrumbListSchema(
  items: { name: string; url: string }[]
): object {
  return {
    "@context": "https://schema.org",
    "@type": "BreadcrumbList",
    itemListElement: items.map((item, index) => ({
      "@type": "ListItem",
      position: index + 1,
      name: item.name,
      item: item.url,
    })),
  };
}

/**
 * Build a Schema.org FAQPage JSON-LD object.
 */
export function faqPageSchema(
  items: { question: string; answer: string }[]
): object {
  return {
    "@context": "https://schema.org",
    "@type": "FAQPage",
    mainEntity: items.map((item) => ({
      "@type": "Question",
      name: item.question,
      acceptedAnswer: {
        "@type": "Answer",
        text: item.answer,
      },
    })),
  };
}

export interface ArticleSchemaInput {
  title: string;
  description: string;
  url: string;
  publishDate: string;
  dateModified?: string;
  author?: string;
  publisherName?: string;
  publisherLogo?: string;
}

/**
 * Build a Schema.org Article/BlogPosting JSON-LD object.
 */
export function articleSchema(data: ArticleSchemaInput): object {
  return {
    "@context": "https://schema.org",
    "@type": "BlogPosting",
    headline: data.title,
    description: data.description,
    datePublished: data.publishDate,
    dateModified: data.dateModified ?? data.publishDate,
    author: {
      "@type": data.author ? "Person" : "Organization",
      name: data.author ?? data.publisherName ?? "Unknown",
    },
    ...(data.publisherName
      ? {
          publisher: {
            "@type": "Organization",
            name: data.publisherName,
            ...(data.publisherLogo
              ? { logo: { "@type": "ImageObject", url: data.publisherLogo } }
              : {}),
          },
        }
      : {}),
    mainEntityOfPage: data.url,
  };
}

export interface OrganizationSchemaInput {
  name: string;
  url: string;
  logo?: string;
  description?: string;
  sameAs?: string[];
}

/**
 * Build a Schema.org Organization JSON-LD object.
 */
export function organizationSchema(data: OrganizationSchemaInput): object {
  return {
    "@context": "https://schema.org",
    "@type": "Organization",
    name: data.name,
    url: data.url,
    ...(data.logo ? { logo: data.logo } : {}),
    ...(data.description ? { description: data.description } : {}),
    ...(data.sameAs && data.sameAs.length > 0 ? { sameAs: data.sameAs } : {}),
  };
}

/**
 * Build a Schema.org WebSite JSON-LD object.
 */
export function websiteSchema(data: { name: string; url: string }): object {
  return {
    "@context": "https://schema.org",
    "@type": "WebSite",
    name: data.name,
    url: data.url,
  };
}
