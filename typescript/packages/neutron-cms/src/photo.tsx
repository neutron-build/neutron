import type { ComponentProps } from "preact";
import { photoAttributes } from "./images.js";

/** Fast by default: the right size for the screen, no layout jump, loaded when scrolled to. */
export function Photo({ src, sizes, alt, ...rest }: { src: string; alt: string; sizes?: string } & Omit<ComponentProps<"img">, "src" | "sizes" | "alt" | "role">) {
  return <img {...rest} {...photoAttributes(src, sizes)} alt={alt} loading="lazy" decoding="async" />;
}
