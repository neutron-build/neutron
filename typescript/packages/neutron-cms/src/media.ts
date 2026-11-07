import { createHash } from "node:crypto";
import { mkdirSync, renameSync, writeFileSync, existsSync } from "node:fs";
import { stat } from "node:fs/promises";
import { createReadStream } from "node:fs";
import { Readable } from "node:stream";
import { join } from "node:path";
import { dataDir } from "./store.js";
import { THUMB_WIDTH } from "./images.js";

export const MAX_UPLOAD_BYTES = 12 * 1024 * 1024;

const TYPES = {
  jpg: "image/jpeg",
  png: "image/png",
  webp: "image/webp",
  gif: "image/gif",
} as const;
type Extension = keyof typeof TYPES;

const NAME = /^[a-f0-9]{32}(-\d{1,5}x\d{1,5}(-t)?)?\.(jpg|png|webp|gif)$/;

/** By content, never by what the client claims. SVG is excluded because it can carry script. */
export function sniffImage(bytes: Uint8Array): Extension | null {
  const ascii = (start: number, end: number) => String.fromCharCode(...bytes.subarray(start, end));
  if (bytes[0] === 0xff && bytes[1] === 0xd8 && bytes[2] === 0xff) return "jpg";
  if (ascii(0, 8) === "\x89PNG\r\n\x1a\n") return "png";
  if (ascii(0, 4) === "RIFF" && ascii(8, 12) === "WEBP") return "webp";
  if (ascii(0, 6) === "GIF87a" || ascii(0, 6) === "GIF89a") return "gif";
  return null;
}

function store(name: string, bytes: Uint8Array): void {
  const directory = join(dataDir(), "media");
  mkdirSync(directory, { recursive: true });
  const path = join(directory, name);
  if (existsSync(path)) return;
  const temporary = `${path}.tmp`;
  writeFileSync(temporary, bytes);
  renameSync(temporary, path);
}

interface Resized {
  width: number;
  height: number;
  thumb?: Uint8Array;
}

/**
 * Content-addressed: the same image is stored once and a URL's bytes never change.
 * A name carries a size only when its thumbnail exists, so `thumbUrl` can rely on it.
 */
export function saveImage(bytes: Uint8Array, resized?: Resized): string | null {
  const extension = sniffImage(bytes);
  if (!extension) return null;
  const hash = createHash("sha256").update(bytes).digest("hex").slice(0, 32);

  const sized = resized && Number.isInteger(resized.width) && Number.isInteger(resized.height)
    && resized.width >= 1 && resized.width <= 99999 && resized.height >= 1 && resized.height <= 99999;
  const needsThumb = sized && resized.width > THUMB_WIDTH;
  const thumbUsable = needsThumb && resized.thumb !== undefined && sniffImage(resized.thumb) === extension;
  if (!sized || (needsThumb && !thumbUsable)) {
    store(`${hash}.${extension}`, bytes);
    return `${hash}.${extension}`;
  }

  const stem = `${hash}-${resized.width}x${resized.height}`;
  if (thumbUsable) store(`${stem}-t.${extension}`, resized.thumb!);
  store(`${stem}.${extension}`, bytes);
  return `${stem}.${extension}`;
}

export async function serveImage(name: string): Promise<Response> {
  if (!NAME.test(name)) return new Response("Not found", { status: 404 });
  try {
    const path = join(dataDir(), "media", name);
    const { size } = await stat(path);
    const body = Readable.toWeb(createReadStream(path)) as unknown as ReadableStream<Uint8Array>;
    return new Response(body, {
      headers: {
        "Content-Length": String(size),
        "Content-Type": TYPES[name.split(".")[1] as Extension],
        "Cache-Control": "public, max-age=31536000, immutable",
        "Cross-Origin-Resource-Policy": "cross-origin",
        "X-Content-Type-Options": "nosniff",
      },
    });
  } catch {
    return new Response("Not found", { status: 404 });
  }
}
