import { h } from "preact";
import type { AnyJsxElement } from "./jsx-agnostic.js";
import { selectImageCandidates } from "../core/image-widths.js";
const DEFAULT_FORMATS = ["avif", "webp"];

const FORMAT_MIME: Record<string, string> = {
  avif: "image/avif",
  webp: "image/webp",
  jpeg: "image/jpeg",
  png: "image/png",
};

export interface PictureProps {
  src: string;
  alt: string;
  /** Width ladder override; see `ImageProps.widths`. */
  widths?: number[];
  formats?: string[];
  sizes?: string;
  width?: number;
  height?: number;
  /** Natural pixel width of the source; larger candidates are dropped. */
  intrinsicWidth?: number;
  class?: string;
  loading?: "lazy" | "eager";
}

function buildSrcSet(src: string, widths: number[], format: string): string {
  return widths
    .map((w) => {
      const params = new URLSearchParams();
      params.set("src", src);
      params.set("w", String(w));
      params.set("fmt", format);
      return `/_neutron/image?${params.toString()} ${w}w`;
    })
    .join(", ");
}

/** Returns {@link AnyJsxElement}: renders from preact- and React-typed JSX. */
export function Picture({
  src,
  alt,
  widths,
  formats = DEFAULT_FORMATS,
  sizes,
  width,
  height,
  intrinsicWidth,
  class: className,
  loading = "lazy",
}: PictureProps): AnyJsxElement | null {
  const candidates = selectImageCandidates({ widths, width, sizes, intrinsicWidth });

  const sources = formats.map((fmt) =>
    h("source", {
      type: FORMAT_MIME[fmt] || `image/${fmt}`,
      srcSet: buildSrcSet(src, candidates.widths, fmt),
      sizes: candidates.sizes,
    })
  );

  const fallbackParams = new URLSearchParams();
  fallbackParams.set("src", src);
  fallbackParams.set("w", String(candidates.src));
  const fallbackSrc = `/_neutron/image?${fallbackParams.toString()}`;

  return h(
    "picture",
    { class: className },
    ...sources,
    h("img", {
      src: fallbackSrc,
      alt,
      width,
      height,
      loading,
      decoding: "async",
    })
  );
}
