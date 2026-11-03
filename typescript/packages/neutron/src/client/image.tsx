import { h } from "preact";
import type { JSX } from "preact";
import type { AnyJsxElement } from "./jsx-agnostic.js";
import { selectImageCandidates } from "../core/image-widths.js";

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
  /**
   * Width ladder override. Defaults to `images.widths`. With the default
   * loader, an entry not in `images.widths` is served at the next ladder
   * width up.
   */
  widths?: number[];
  sizes?: string;
  /** Natural pixel width of the source; larger candidates are dropped. */
  intrinsicWidth?: number;
  quality?: number;
  format?: "webp" | "avif" | "jpeg" | "png";
  loader?: ImageLoader;
  priority?: boolean;
}

/** Returns {@link AnyJsxElement}: renders from preact- and React-typed JSX. */
export function Image(props: ImageProps): AnyJsxElement | null {
  const {
    src,
    alt,
    width,
    height,
    widths,
    sizes,
    intrinsicWidth,
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

  const candidates = selectImageCandidates({ widths, width, sizes, intrinsicWidth });
  const resolvedSrc = loader({ src, width: candidates.src, quality, format });
  const srcSet = candidates.widths
    .map((candidate) => `${loader({ src, width: candidate, quality, format })} ${candidate}w`)
    .join(", ");

  return h("img", {
    ...rest,
    src: resolvedSrc,
    srcSet,
    sizes: candidates.sizes,
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
