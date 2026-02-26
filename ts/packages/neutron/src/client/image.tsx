import { h } from "preact";
import type { JSX } from "preact";

const DEFAULT_WIDTHS = [320, 640, 960, 1200, 1600];

export interface ImageLoaderArgs {
  src: string;
  width: number;
  quality?: number;
  format?: "webp" | "avif" | "jpeg" | "png";
}

export type ImageLoader = (args: ImageLoaderArgs) => string;

export interface ImageProps
  extends Omit<
    JSX.HTMLAttributes<HTMLImageElement>,
    "src" | "srcSet" | "alt" | "width" | "height" | "loading" | "decoding"
  > {
  src: string;
  alt: string;
  width?: number;
  height?: number;
  widths?: number[];
  sizes?: string;
  quality?: number;
  format?: "webp" | "avif" | "jpeg" | "png";
  loader?: ImageLoader;
  priority?: boolean;
}

export function Image(props: ImageProps): any {
  const {
    src,
    alt,
    width,
    height,
    widths,
    sizes,
    quality = 75,
    format,
    loader = defaultImageLoader,
    priority = false,
    ...rest
  } = props;

  if (!canOptimizeSrc(src)) {
    return h("img", {
      ...rest,
      src,
      alt,
      width,
      height,
      loading: priority ? "eager" : "lazy",
      decoding: "async",
    });
  }

  const candidateWidths = pickCandidateWidths(widths || DEFAULT_WIDTHS, width);
  const baseWidth = width || candidateWidths[candidateWidths.length - 1];
  const resolvedSrc = loader({ src, width: baseWidth, quality, format });
  const srcSet = candidateWidths
    .map((candidate) => `${loader({ src, width: candidate, quality, format })} ${candidate}w`)
    .join(", ");

  return h("img", {
    ...rest,
    src: resolvedSrc,
    srcSet,
    sizes: sizes || "100vw",
    alt,
    width,
    height,
    loading: priority ? "eager" : "lazy",
    decoding: "async",
  });
}

function canOptimizeSrc(src: string): boolean {
  return !(src.startsWith("data:") || src.startsWith("blob:"));
}

function pickCandidateWidths(widths: number[], maxWidth?: number): number[] {
  const filtered = Array.from(new Set(widths.filter((value) => Number.isFinite(value) && value > 0)))
    .sort((left, right) => left - right);

  if (filtered.length === 0) {
    return [640];
  }

  if (!maxWidth || maxWidth <= 0) {
    return filtered;
  }

  const withinBounds = filtered.filter((value) => value <= maxWidth);
  if (withinBounds.length === 0) {
    return [maxWidth];
  }
  if (withinBounds[withinBounds.length - 1] !== maxWidth) {
    withinBounds.push(maxWidth);
  }
  return withinBounds;
}

export function defaultImageLoader(args: ImageLoaderArgs): string {
  const params = new URLSearchParams();
  params.set("src", args.src);
  params.set("w", String(args.width));
  if (args.quality != null) {
    params.set("q", String(args.quality));
  }
  if (args.format) {
    params.set("fmt", args.format);
  }
  return `/_neutron/image?${params.toString()}`;
}
