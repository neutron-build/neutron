import { escapeHtml, escapeXml, nonceAttr } from "./escape.js";

export interface HeadScript {
  /** Inline JavaScript content. Must not contain </script>. */
  content: string;
  /** Script type attribute. Defaults to text/javascript if omitted. */
  type?: string;
  /** Optional id attribute for the script tag. */
  id?: string;
}

export interface SeoMetaInput {
  title?: string;
  /**
   * Template for page titles. Use %s as the placeholder.
   * Set at the layout level; child routes inherit it.
   * Set to null on a route to opt out of the inherited template.
   * @example "%s — Tebian"
   */
  titleTemplate?: string | null;
  /**
   * Title for a page in this route chain that sets no `title` of its own —
   * typically declared once in the root layout, as Next's `title.default`.
   * Rendered as written: `titleTemplate` is not applied to it. Inherited like
   * every field; a nested layout can set its own. Without it (and without a
   * `title`), the title is guessed from the URL path.
   * @example defaultTitle: "Tebian"
   */
  defaultTitle?: string;
  description?: string;
  /** Emitted exactly as given (`https://example.com` stays without a slash). */
  canonical?: string;
  /**
   * `<meta name="robots" content="noindex">`. Shorthand for
   * `robots: { index: false }`; `robots` wins when one head() sets both, and
   * a route's either one replaces its layout's.
   */
  noindex?: boolean;
  /**
   * `<meta name="robots">`: a string is sent as written; the object form
   * (Next's `robots`) lists what is set, in this order —
   * `{ index: true, follow: true }` → `index, follow`.
   */
  robots?: string | SeoRobots;
  keywords?: string[];
  /**
   * `<meta name="viewport">`, replacing the framework default
   * (`width=device-width, initial-scale=1.0`). A string is sent as written;
   * the object form is Next's `viewport` export.
   * @example viewport: { width: "device-width", initialScale: 1, interactiveWidget: "resizes-content" }
   */
  viewport?: string | SeoViewport;
  /**
   * `<meta name="theme-color">`: one color, or one tag per entry with its
   * `media` (Next's `themeColor`).
   * @example themeColor: [{ media: "(prefers-color-scheme: light)", color: "white" }, { media: "(prefers-color-scheme: dark)", color: "#0a0a0a" }]
   */
  themeColor?: string | SeoThemeColor | Array<string | SeoThemeColor>;
  /** `<meta name="author">` per name, `<link rel="author">` per url (Next's `authors`). */
  authors?: SeoAuthor | SeoAuthor[];
  /** `<meta name="creator">`. */
  creator?: string;
  /**
   * Site-ownership verification tags (Next's `verification`):
   * `google` → `google-site-verification`, `yandex` → `yandex-verification`,
   * `yahoo` → `y_key`, `me` → `me`, `other` → its own names.
   */
  verification?: SeoVerification;
  openGraph?: {
    title?: string;
    description?: string;
    /** One image URL; `images` takes over when both are set. */
    image?: string;
    /** og:image, each with its og:image:width/height/type/alt (Next's `openGraph.images`). */
    images?: string | SeoOpenGraphImage | Array<string | SeoOpenGraphImage>;
    type?: string;
    url?: string;
    /** og:locale, e.g. `en_US`. */
    locale?: string;
    /** og:site_name. */
    siteName?: string;
  };
  twitter?: {
    card?: "summary" | "summary_large_image";
    title?: string;
    description?: string;
    image?: string;
  };
  /** JSON-LD structured data objects to inject as <script type="application/ld+json"> */
  jsonLd?: object | object[];
  /** Trusted inline scripts to inject in <head>. Bypasses HTML sanitization. */
  headScripts?: HeadScript | HeadScript[];
  /**
   * Additional <link> tags — favicon, preconnect, manifest, alternate, etc.
   * Each entry is the tag's attributes.
   * @example link: { rel: "icon", type: "image/svg+xml", href: "/favicon.svg" }
   */
  link?: Record<string, string> | Record<string, string>[];
  /**
   * Any other <meta> tag, attributes escaped. An entry replaces every tag of
   * its key (`name`, `property` or `httpEquiv`) the fields above would emit,
   * and a route's entries replace its layouts' for the same key.
   * @example meta: [{ name: "format-detection", content: "telephone=no" }, { httpEquiv: "x-ua-compatible", content: "IE=edge" }]
   */
  meta?: SeoMetaTag | SeoMetaTag[];
  /** Attributes to set on the <html> element. lang defaults to "en" if not specified. */
  htmlAttrs?: Record<string, string>;
  /** Attributes to set on the <body> element. */
  bodyAttrs?: Record<string, string>;
}

export interface SeoRobots {
  index?: boolean;
  follow?: boolean;
  noarchive?: boolean;
  nosnippet?: boolean;
  noimageindex?: boolean;
  nocache?: boolean;
}

export interface SeoViewport {
  width?: string | number;
  height?: string | number;
  initialScale?: number;
  minimumScale?: number;
  maximumScale?: number;
  userScalable?: boolean;
  viewportFit?: "auto" | "cover" | "contain";
  interactiveWidget?: "resizes-visual" | "resizes-content" | "overlays-content";
}

export interface SeoThemeColor {
  color: string;
  media?: string;
}

export interface SeoAuthor {
  name?: string;
  url?: string;
}

export interface SeoVerification {
  google?: string | string[];
  yandex?: string | string[];
  yahoo?: string | string[];
  me?: string | string[];
  other?: Record<string, string | string[]>;
}

export interface SeoOpenGraphImage {
  url: string;
  width?: number | string;
  height?: number | string;
  type?: string;
  alt?: string;
}

export type SeoMetaTag = {
  content: string;
  media?: string;
} & (
  | { name: string; property?: never; httpEquiv?: never }
  | { property: string; name?: never; httpEquiv?: never }
  | { httpEquiv: string; name?: never; property?: never }
);

export type SeoTag =
  | { tag: "title"; content: string }
  | { tag: "meta"; attrs: Record<string, string> }
  | { tag: "link"; attrs: Record<string, string> };

export function buildMetaTags(input: SeoMetaInput): SeoTag[] {
  const tags: SeoTag[] = [];

  const resolvedTitle = input.title
    ? applyTitleTemplate(input.title, input.titleTemplate)
    : input.defaultTitle || undefined;

  if (resolvedTitle) {
    tags.push({ tag: "title", content: resolvedTitle });
  }

  const viewport = renderViewport(input.viewport);
  if (viewport) {
    tags.push({ tag: "meta", attrs: { name: "viewport", content: viewport } });
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

  for (const author of toList(input.authors)) {
    pushMeta(tags, "name", "author", author.name);
  }
  pushMeta(tags, "name", "creator", input.creator);

  for (const entry of toList(input.themeColor)) {
    const { color, media } = typeof entry === "string" ? { color: entry, media: undefined } : entry;
    if (color) {
      tags.push({ tag: "meta", attrs: media ? { name: "theme-color", media, content: color } : { name: "theme-color", content: color } });
    }
  }

  const robots = renderRobots(input.robots) ?? (input.noindex ? "noindex" : undefined);
  pushMeta(tags, "name", "robots", robots);

  const verification = input.verification;
  if (verification) {
    const names: Array<[string, string | string[] | undefined]> = [
      ["google-site-verification", verification.google],
      ["yandex-verification", verification.yandex],
      ["y_key", verification.yahoo],
      ["me", verification.me],
      ...Object.entries(verification.other ?? {}),
    ];
    for (const [name, values] of names) {
      for (const value of toList(values)) pushMeta(tags, "name", name, value);
    }
  }

  if (input.canonical) {
    tags.push({ tag: "link", attrs: { rel: "canonical", href: input.canonical } });
  }

  for (const author of toList(input.authors)) {
    if (author.url) tags.push({ tag: "link", attrs: { rel: "author", href: author.url } });
  }

  if (input.link) {
    const links = Array.isArray(input.link) ? input.link : [input.link];
    for (const attrs of links) {
      tags.push({ tag: "link", attrs });
    }
  }

  const ogImages = openGraphImages(input.openGraph);
  if (input.openGraph) {
    const og = input.openGraph;
    pushMeta(tags, "property", "og:title", og.title || resolvedTitle);
    pushMeta(tags, "property", "og:description", og.description || input.description);
    for (const image of ogImages) {
      pushMeta(tags, "property", "og:image", image.url);
      pushMeta(tags, "property", "og:image:width", image.width === undefined ? undefined : String(image.width));
      pushMeta(tags, "property", "og:image:height", image.height === undefined ? undefined : String(image.height));
      pushMeta(tags, "property", "og:image:type", image.type);
      pushMeta(tags, "property", "og:image:alt", image.alt);
    }
    pushMeta(tags, "property", "og:type", og.type || "website");
    pushMeta(tags, "property", "og:url", og.url || input.canonical);
    pushMeta(tags, "property", "og:locale", og.locale);
    pushMeta(tags, "property", "og:site_name", og.siteName);
  }

  if (input.twitter) {
    const tw = input.twitter;
    pushMeta(tags, "name", "twitter:card", tw.card || "summary_large_image");
    pushMeta(tags, "name", "twitter:title", tw.title || resolvedTitle);
    pushMeta(tags, "name", "twitter:description", tw.description || input.description);
    pushMeta(tags, "name", "twitter:image", tw.image || ogImages[0]?.url);
  }

  // The escape hatch replaces whatever the fields above emitted for its key.
  const extra = toList(input.meta).map(metaTagAttrs).filter((attrs): attrs is Record<string, string> => attrs !== null);
  const replaced = new Set(extra.map((attrs) => headTagKey({ tag: "meta", attrs })));
  const output = dedupeLinkTags(tags).filter((tag) => tag.tag !== "meta" || !replaced.has(headTagKey(tag)));
  for (const attrs of extra) output.push({ tag: "meta", attrs });
  return output;
}

function toList<T>(value: T | T[] | undefined | null): T[] {
  if (value === undefined || value === null) return [];
  return Array.isArray(value) ? value : [value];
}

function renderViewport(viewport: SeoMetaInput["viewport"]): string | undefined {
  if (viewport === undefined) return undefined;
  if (typeof viewport === "string") return viewport || undefined;
  const parts: string[] = [];
  const add = (name: string, value: string | number | boolean | undefined) => {
    if (value === undefined) return;
    parts.push(`${name}=${typeof value === "boolean" ? (value ? "yes" : "no") : value}`);
  };
  add("width", viewport.width);
  add("height", viewport.height);
  add("initial-scale", viewport.initialScale);
  add("minimum-scale", viewport.minimumScale);
  add("maximum-scale", viewport.maximumScale);
  add("user-scalable", viewport.userScalable);
  add("viewport-fit", viewport.viewportFit);
  add("interactive-widget", viewport.interactiveWidget);
  return parts.length > 0 ? parts.join(", ") : undefined;
}

function renderRobots(robots: SeoMetaInput["robots"]): string | undefined {
  if (robots === undefined) return undefined;
  if (typeof robots === "string") return robots || undefined;
  const parts: string[] = [];
  if (robots.index !== undefined) parts.push(robots.index ? "index" : "noindex");
  if (robots.follow !== undefined) parts.push(robots.follow ? "follow" : "nofollow");
  for (const flag of ["noarchive", "nosnippet", "noimageindex", "nocache"] as const) {
    if (robots[flag]) parts.push(flag);
  }
  return parts.length > 0 ? parts.join(", ") : undefined;
}

function openGraphImages(og: SeoMetaInput["openGraph"]): SeoOpenGraphImage[] {
  if (!og) return [];
  const images = og.images !== undefined ? toList(og.images) : og.image ? [og.image] : [];
  return images
    .map((image) => (typeof image === "string" ? { url: image } : image))
    .filter((image) => Boolean(image?.url));
}

function metaTagAttrs(tag: SeoMetaTag): Record<string, string> | null {
  const attrs: Record<string, string> = {};
  if (tag.name) attrs.name = tag.name;
  else if (tag.property) attrs.property = tag.property;
  else if (tag.httpEquiv) attrs["http-equiv"] = tag.httpEquiv;
  else return null;
  if (tag.media) attrs.media = tag.media;
  attrs.content = String(tag.content ?? "");
  return attrs;
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

// Links that name the document itself: one per head, the nearest layer wins.
const SINGLE_LINK_RELS = new Set(["canonical", "manifest"]);

/**
 * The identity two head tags share when one replaces the other: the title;
 * a meta by its name / property / http-equiv (so every `theme-color` of a
 * layer replaces every one of the layer under it); a canonical or manifest
 * link by its rel, any other link by rel and href.
 */
export function headTagKey(tag: SeoTag): string {
  if (tag.tag === "title") return "title";
  const attrs = lowerCaseKeys(tag.attrs);
  if (tag.tag === "meta") {
    if (attrs.charset !== undefined) return "meta:charset";
    const kind = attrs.name !== undefined ? "name" : attrs.property !== undefined ? "property" : "http-equiv";
    return `meta:${kind}:${(attrs[kind] ?? "").toLowerCase()}`;
  }
  const rel = (attrs.rel ?? "").toLowerCase().trim();
  return SINGLE_LINK_RELS.has(rel) ? `link:${rel}` : `link:${rel}:${attrs.href ?? ""}`;
}

function lowerCaseKeys(attrs: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [name, value] of Object.entries(attrs)) out[name.toLowerCase()] = value;
  return out;
}

function dedupeLinkTags(tags: SeoTag[]): SeoTag[] {
  const seen = new Set<string>();
  const output: SeoTag[] = [];

  for (const tag of tags) {
    // Repeated metas are deliberate (one theme-color per media, one og:image
    // group per image, one author per name); links repeat by accident.
    if (tag.tag === "link") {
      // Include href so multiple same-rel links (e.g. preconnect) aren't dropped.
      const key = `link:${tag.attrs.rel || ""}:${tag.attrs.href || ""}`;
      if (seen.has(key)) {
        continue;
      }
      seen.add(key);
    }
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
      // Attribute names can come from untrusted data (e.g. seo.link entry
      // keys); sanitize them exactly like renderAttrs — strip invalid
      // characters and drop event handlers — or a crafted key injects a
      // second attribute (`href="/x" onclick="..."`).
      const attrs = Object.entries(tag.attrs)
        .map(([name, value]) => {
          if (/^on/i.test(name)) return "";
          const safeName = escapeAttrName(name);
          return safeName ? `${safeName}="${escapeHtml(value)}"` : "";
        })
        .filter(Boolean)
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

  // Concatenate headScripts arrays from both base and override
  const baseScripts = base.headScripts
    ? (Array.isArray(base.headScripts) ? base.headScripts : [base.headScripts])
    : [];
  const overrideScripts = override.headScripts
    ? (Array.isArray(override.headScripts) ? override.headScripts : [override.headScripts])
    : [];
  const allScripts = [...baseScripts, ...overrideScripts];
  if (allScripts.length > 0) {
    merged.headScripts = allScripts;
  } else {
    delete merged.headScripts;
  }

  // Concatenate link arrays from both base and override (favicon, preconnect, …)
  const baseLinks = base.link ? (Array.isArray(base.link) ? base.link : [base.link]) : [];
  const overrideLinks = override.link ? (Array.isArray(override.link) ? override.link : [override.link]) : [];
  const allLinks = [...baseLinks, ...overrideLinks];
  if (allLinks.length > 0) {
    merged.link = allLinks;
  } else {
    delete merged.link;
  }

  // robots and noindex are one tag: whichever the override sets replaces
  // the other from the base.
  if (override.noindex !== undefined && override.robots === undefined) {
    delete merged.robots;
  }
  if (override.robots !== undefined && override.noindex === undefined) {
    delete merged.noindex;
  }

  // meta: the override's entries replace the base's for the same key.
  const overrideMeta = toList(override.meta);
  const overrideKeys = new Set(
    overrideMeta.map(metaTagAttrs).flatMap((attrs) => (attrs ? [headTagKey({ tag: "meta", attrs })] : []))
  );
  const allMeta = [
    ...toList(base.meta).filter((entry) => {
      const attrs = metaTagAttrs(entry);
      return !attrs || !overrideKeys.has(headTagKey({ tag: "meta", attrs }));
    }),
    ...overrideMeta,
  ];
  if (allMeta.length > 0) {
    merged.meta = allMeta;
  } else {
    delete merged.meta;
  }

  const mergedVerification = mergeSeoNested(
    base.verification as Record<string, unknown> | undefined,
    override.verification as Record<string, unknown> | undefined
  );
  if (mergedVerification) {
    merged.verification = mergedVerification as SeoVerification;
  } else {
    delete merged.verification;
  }

  // Merge htmlAttrs and bodyAttrs (route overrides layout per-attribute)
  const mergedHtmlAttrs = mergeSeoNested(base.htmlAttrs, override.htmlAttrs);
  if (mergedHtmlAttrs) {
    merged.htmlAttrs = mergedHtmlAttrs;
  } else {
    delete merged.htmlAttrs;
  }

  const mergedBodyAttrs = mergeSeoNested(base.bodyAttrs, override.bodyAttrs);
  if (mergedBodyAttrs) {
    merged.bodyAttrs = mergedBodyAttrs;
  } else {
    delete merged.bodyAttrs;
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

const DEFAULT_VIEWPORT = "width=device-width, initial-scale=1.0";

/** One tag of the app's index.html head (document-head.ts): its key and markup. */
export interface DocumentHeadTag {
  key: string;
  html: string;
}

/**
 * The document `<head>`.
 *
 * Layers, lowest first: the framework's defaults (charset, viewport), the
 * app's `index.html` head (`documentHead`, see document-head.ts), then the
 * route chain's `head()` — structured tags and raw-string fragments. A layer
 * replaces the tags of the layers under it that share a key (`headTagKey`):
 * a page's `themeColor` replaces every theme-color index.html declares, its
 * `title` the index.html title, its `viewport` the framework default.
 */
export function renderDocumentHead(
  pathname: string,
  seo: SeoMetaInput | null | undefined,
  headFragments: string[] = [],
  nonce?: string,
  documentHead: ReadonlyArray<DocumentHeadTag> = []
): string {
  const seoTags = seo ? buildMetaTags(seo) : [];
  const extraHead = headFragments
    .map((fragment) => fragment.trim())
    .filter(Boolean)
    .join("\n");

  // Keys the route chain sets, structured or in a raw fragment.
  const fragmentKeys = new Set(scanHeadTags(extraHead).map(headTagKey));
  const chainKeys = new Set([...seoTags.map(headTagKey), ...fragmentKeys]);
  const baseTags = documentHead.filter((tag) => !chainKeys.has(tag.key) && tag.key !== "meta:charset");

  // The viewport goes first, whichever layer set it.
  const VIEWPORT = "meta:name:viewport";
  const isViewport = (tag: SeoTag) => headTagKey(tag) === VIEWPORT;
  const chainViewport = seoTags.find(isViewport);
  const baseViewport = baseTags.find((tag) => tag.key === VIEWPORT);
  const viewportHtml = chainViewport
    ? renderMetaTags([chainViewport])
    : baseViewport
      ? baseViewport.html
      : fragmentKeys.has(VIEWPORT)
        ? ""
        : `<meta name="viewport" content="${DEFAULT_VIEWPORT}">`;
  // index.html's tags are the app's own markup, emitted as written (as Vite does).
  const baseHtml = baseTags
    .filter((tag) => tag.key !== VIEWPORT)
    .map((tag) => tag.html)
    .join("\n");
  const seoHtml = renderMetaTags(seoTags.filter((tag) => !isViewport(tag)));

  const hasTitle =
    /<title(?:\s|>)/i.test(seoHtml) ||
    baseTags.some((tag) => tag.key === "title") ||
    /<title(?:\s|>)/i.test(extraHead);
  let titleTag = "";
  // Last resort, reached only when the route chain declares neither a `title`
  // nor a `defaultTitle`, and index.html has no <title>: HTML requires one,
  // and a document without one shows its bare URL in tabs, bookmarks and
  // screen readers.
  if (!hasTitle) {
    const inferred = inferPageTitle(pathname);
    const template = seo?.titleTemplate;
    // No framework brand in the default — a page with no title gets just the
    // inferred title. Users add a site-name suffix via `titleTemplate`.
    const fallbackTitle = (template !== null && template !== undefined)
      ? template.replace(/%s/g, inferred)
      : inferred;
    titleTag = `<title>${escapeHtml(fallbackTitle)}</title>`;
  }

  // Render JSON-LD structured data scripts
  const jsonLdHtml = renderJsonLd(seo?.jsonLd, nonce);

  // Render trusted inline head scripts
  const headScriptsHtml = renderHeadScripts(seo?.headScripts, nonce);

  // Inline anti-flash style — paints the site background before app CSS parses,
  // so full-load navigations never flash white between pages. color-scheme must
  // match the background or a light-preference browser paints the inter-page
  // canvas white anyway (the flash this style exists to prevent). Defaults to a
  // dark canvas to match the default dark background; light-mode apps override
  // both via --neutron-bg and --neutron-color-scheme on :root.
  const antiFlashStyle =
    '<style>html{background:var(--neutron-bg,#0A0A0A);color-scheme:var(--neutron-color-scheme,dark)}</style>';

  // <neutron-island> (the hydration wrapper <Island> renders) is a custom
  // element with no built-in styling, so browsers treat it as inline: inside
  // a flex container with non-stretch alignment an inline/auto-sized item
  // shrinks to fit, which breaks percentage widths on its children. Any box
  // of its own changes layout somewhere — a full-width block breaks flex rows,
  // grids and inline text. `display: contents` gives it none: the island's
  // content lays out exactly as it would without the marker, as a direct
  // child of the island's parent. The runtime observes that content for
  // `client="visible"` (island-runtime.ts). Author CSS that follows can still
  // give the marker a box.
  const islandBaseStyle =
    '<style>neutron-island{display:contents}</style>';

  return [
    '<meta charset="UTF-8">',
    viewportHtml,
    antiFlashStyle,
    islandBaseStyle,
    titleTag,
    baseHtml,
    seoHtml,
    extraHead,
    jsonLdHtml,
    headScriptsHtml,
  ]
    .filter(Boolean)
    .join("\n");
}

/**
 * The `<title>`, `<meta>` and `<link>` tags of an HTML fragment, each with
 * its markup as written and its attributes (the common entities decoded —
 * enough to compute `headTagKey`). Comments,
 * and the contents of `<script>`, `<style>`, `<noscript>` and `<template>`,
 * are skipped. Tolerant, not a full HTML parser: it reads head markup.
 */
export function scanHeadTags(html: string): Array<SeoTag & { html: string }> {
  const tags: Array<SeoTag & { html: string }> = [];
  const source = html
    .replace(/<!--[\s\S]*?-->/g, "")
    .replace(/<(script|style|noscript|template)\b[\s\S]*?<\/\1\s*>/gi, "");
  const pattern = /<title\b[^>]*>([\s\S]*?)<\/title\s*>|<(meta|link)\b((?:[^>"']|"[^"]*"|'[^']*')*)\/?>/gi;
  let match: RegExpExecArray | null;
  while ((match = pattern.exec(source)) !== null) {
    if (match[1] !== undefined) {
      tags.push({ tag: "title", content: decodeHtmlEntities(match[1].trim()), html: match[0] });
      continue;
    }
    const attrs = parseTagAttributes(match[3] ?? "");
    tags.push({ tag: match[2]!.toLowerCase() as "meta" | "link", attrs, html: match[0] });
  }
  return tags;
}

function parseTagAttributes(source: string): Record<string, string> {
  const attrs: Record<string, string> = {};
  const pattern = /([^\s"'<>\/=]+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'=<>`]+)))?/g;
  let match: RegExpExecArray | null;
  while ((match = pattern.exec(source)) !== null) {
    const name = match[1]!.toLowerCase();
    if (name in attrs) continue;
    attrs[name] = decodeHtmlEntities(match[2] ?? match[3] ?? match[4] ?? "");
  }
  return attrs;
}

const NAMED_ENTITIES: Record<string, string> = { amp: "&", lt: "<", gt: ">", quot: '"', apos: "'", nbsp: "\u00a0" };

function decodeHtmlEntities(value: string): string {
  return value.replace(/&(#x[0-9a-f]+|#[0-9]+|[a-z]+);/gi, (entity, body: string) => {
    if (body[0] === "#") {
      const code = body[1] === "x" || body[1] === "X" ? parseInt(body.slice(2), 16) : parseInt(body.slice(1), 10);
      return Number.isFinite(code) && code > 0 && code <= 0x10ffff ? String.fromCodePoint(code) : entity;
    }
    return NAMED_ENTITIES[body.toLowerCase()] ?? entity;
  });
}

/**
 * Build the `<html>` opening tag with merged attributes.
 * Defaults to `lang="en"` if not specified in htmlAttrs.
 */
export function buildHtmlOpenTag(htmlAttrs?: Record<string, string>): string {
  const attrs = { lang: "en", ...htmlAttrs };
  return `<html${renderAttrs(attrs)}>`;
}

/**
 * Build the `<body>` opening tag with optional attributes.
 */
export function buildBodyOpenTag(bodyAttrs?: Record<string, string>): string {
  if (!bodyAttrs || Object.keys(bodyAttrs).length === 0) {
    return "<body>";
  }
  return `<body${renderAttrs(bodyAttrs)}>`;
}

/**
 * Render an attribute map to an HTML attribute string (with leading space).
 */
export function renderAttrs(attrs: Record<string, string> | undefined): string {
  if (!attrs || Object.keys(attrs).length === 0) return "";
  return Object.entries(attrs)
    .map(([key, value]) => {
      // Drop event-handler attributes (onload, onclick, …). escapeAttrName
      // permits them character-wise, so a name built from untrusted data could
      // otherwise inject an inline handler on <html>/<body>.
      if (/^on/i.test(key)) return "";
      const name = escapeAttrName(key);
      return name ? ` ${name}="${escapeHtml(value)}"` : "";
    })
    .filter(Boolean)
    .join("");
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
    ...(input.openGraph
      ? {
          openGraph: {
            ...input.openGraph,
            ...(Array.isArray(input.openGraph.images) ? { images: [...input.openGraph.images] } : {}),
          },
        }
      : {}),
    ...(Array.isArray(input.meta) ? { meta: [...input.meta] } : {}),
    ...(Array.isArray(input.themeColor) ? { themeColor: [...input.themeColor] } : {}),
    ...(Array.isArray(input.authors) ? { authors: [...input.authors] } : {}),
    ...(input.verification ? { verification: { ...input.verification } } : {}),
    ...(input.twitter ? { twitter: { ...input.twitter } } : {}),
    ...(input.jsonLd ? { jsonLd: Array.isArray(input.jsonLd) ? [...input.jsonLd] : input.jsonLd } : {}),
    ...(input.headScripts ? { headScripts: Array.isArray(input.headScripts) ? [...input.headScripts] : input.headScripts } : {}),
    ...(input.link ? { link: Array.isArray(input.link) ? [...input.link] : input.link } : {}),
    ...(input.htmlAttrs ? { htmlAttrs: { ...input.htmlAttrs } } : {}),
    ...(input.bodyAttrs ? { bodyAttrs: { ...input.bodyAttrs } } : {}),
  };
}

function applyTitleTemplate(title: string, template: string | null | undefined): string {
  if (template === null || template === undefined) {
    return title;
  }
  return template.replace(/%s/g, title);
}

function renderHeadScripts(
  headScripts: HeadScript | HeadScript[] | undefined,
  nonce?: string
): string {
  if (!headScripts) return "";
  const items = Array.isArray(headScripts) ? headScripts : [headScripts];
  if (items.length === 0) return "";
  return items
    .map((script) => {
      // Reject content that can close the inline <script> element. `</script>`
      // alone is insufficient — HTML5 ends the element on `</script` followed by
      // whitespace, `/`, or `>`. This is the real breakout vector; we don't also
      // block `<script`/`<!--`, which would false-positive on legitimate inline
      // JS (e.g. a string containing "<script>") in otherwise-trusted content.
      if (/<\/script[\s/>]/i.test(script.content)) {
        console.warn("[neutron] headScripts content contains a </script> close — skipping to prevent injection.");
        return "";
      }
      const typeAttr = script.type ? ` type="${escapeHtml(script.type)}"` : "";
      const idAttr = script.id ? ` id="${escapeHtml(script.id)}"` : "";
      return `<script${typeAttr}${idAttr}${nonceAttr(nonce)} data-neutron-head="true">${script.content}</script>`;
    })
    .filter(Boolean)
    .join("\n");
}

function escapeAttrName(name: string): string {
  return name.replace(/[^a-zA-Z0-9\-_:]/g, "");
}

function renderJsonLd(
  jsonLd: object | object[] | undefined,
  nonce?: string
): string {
  if (!jsonLd) return "";
  const items = Array.isArray(jsonLd) ? jsonLd : [jsonLd];
  if (items.length === 0) return "";
  return items
    .map((item) => {
      const json = JSON.stringify(item)
        .replace(/</g, "\\u003c")
        .replace(/>/g, "\\u003e")
        .replace(/&/g, "\\u0026");
      return `<script type="application/ld+json"${nonceAttr(nonce)}>${json}</script>`;
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
