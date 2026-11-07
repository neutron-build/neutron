export const THUMB_WIDTH = 800;

const SIZED = /-(\d+)x(\d+)\.\w+$/;

/** An upload's size is in its name, so showing an image needs no lookup. Older images have none. */
export function imageSize(url: string): { width: number; height: number } | null {
  const match = url.match(SIZED);
  return match ? { width: Number(match[1]), height: Number(match[2]) } : null;
}

export function thumbUrl(url: string): string {
  const size = imageSize(url);
  return size && size.width > THUMB_WIDTH ? url.replace(/(\.\w+)$/, "-t$1") : url;
}

export interface PhotoAttributes {
  src: string;
  width?: number;
  height?: number;
  srcset?: string;
  sizes?: string;
}

export function photoAttributes(src: string, sizes = "100vw"): PhotoAttributes {
  const size = imageSize(src);
  if (!size) return { src };
  const thumb = thumbUrl(src);
  return {
    src: thumb,
    ...size,
    ...(thumb === src ? {} : { srcset: `${thumb} ${THUMB_WIDTH}w, ${src} ${size.width}w`, sizes }),
  };
}
