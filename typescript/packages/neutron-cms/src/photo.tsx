import { photoAttributes } from "./images.js";

/** Fast by default: the right size for the screen, no layout jump, loaded when scrolled to. */
export function Photo(props: { src: string; alt: string; sizes?: string; class?: string }) {
  return <img {...photoAttributes(props.src, props.sizes)} alt={props.alt} class={props.class} loading="lazy" decoding="async" />;
}
