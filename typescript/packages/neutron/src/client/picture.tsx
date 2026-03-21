import { h } from "preact";

const DEFAULT_WIDTHS = [320, 640, 960, 1200, 1600];
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
  widths?: number[];
  formats?: string[];
  sizes?: string;
  width?: number;
  height?: number;
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

export function Picture({
  src,
  alt,
  widths,
  formats = DEFAULT_FORMATS,
  sizes = "100vw",
  width,
  height,
  class: className,
  loading = "lazy",
}: PictureProps): any {
  const candidateWidths = widths || DEFAULT_WIDTHS;

  const sources = formats.map((fmt) =>
    h("source", {
      type: FORMAT_MIME[fmt] || `image/${fmt}`,
      srcSet: buildSrcSet(src, candidateWidths, fmt),
      sizes,
    })
  );

  const fallbackWidth = width || candidateWidths[candidateWidths.length - 1];
  const fallbackParams = new URLSearchParams();
  fallbackParams.set("src", src);
  fallbackParams.set("w", String(fallbackWidth));
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
