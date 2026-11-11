import { createHash } from "node:crypto";
import { appendFileSync, mkdirSync, readdirSync, renameSync, rmSync, statSync, writeFileSync, existsSync } from "node:fs";
import { stat } from "node:fs/promises";
import { createReadStream } from "node:fs";
import { Readable } from "node:stream";
import { join } from "node:path";
import { dataDir } from "./store.js";
import { imageSize, THUMB_WIDTH } from "./images.js";
import { FILE_TYPES, MAX_FILE_BYTES, fileLabel, fileSlug, isFileName, type FileExtension } from "./files.js";

export const MAX_UPLOAD_BYTES = 12 * 1024 * 1024;

const TYPES = {
  jpg: "image/jpeg",
  png: "image/png",
  webp: "image/webp",
  gif: "image/gif",
} as const;
type Extension = keyof typeof TYPES;

const NAME = /^[a-f0-9]{32}(-\d{1,5}x\d{1,5}(-t)?)?\.(jpg|png|webp|gif)$/;

/** Only a name `saveImage` could have written, and never its thumbnail, which goes with it. */
export const isImageName = (name: string) => NAME.test(name) && !name.endsWith(`-t.${name.split(".")[1]}`);

/** An image or a file: anything Media holds under its own name. */
export const isMediaName = (name: string) => isImageName(name) || isFileName(name);

/** The thumbnail's name, for an image that has one. */
const thumbName = (name: string) => name.replace(/(-\d+x\d+)(\.\w+)$/, "$1-t$2");

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

const NOT_FOUND = () => new Response("Not found", { status: 404 });

/** One byte range, as audio and video players ask for. Anything else gets the whole file. */
function byteRange(header: string | null, size: number): { start: number; end: number } | null {
  const match = header?.match(/^bytes=(\d*)-(\d*)$/);
  if (!match || (!match[1] && !match[2])) return null;
  const start = match[1] ? Number(match[1]) : Math.max(0, size - Number(match[2]));
  const end = match[1] && match[2] ? Math.min(Number(match[2]), size - 1) : size - 1;
  return start <= end && start < size ? { start, end } : null;
}

export async function serveMedia(name: string, request?: Request): Promise<Response> {
  const image = NAME.test(name);
  if (!image && !isFileName(name)) return NOT_FOUND();
  try {
    const path = join(dataDir(), "media", name);
    const { size } = await stat(path);
    const range = image ? null : byteRange(request?.headers.get("range") ?? null, size);
    const body = Readable.toWeb(createReadStream(path, range ?? undefined)) as unknown as ReadableStream<Uint8Array>;
    const headers: Record<string, string> = {
      "Content-Length": String(range ? range.end - range.start + 1 : size),
      "Content-Type": image ? TYPES[name.split(".")[1] as Extension] : FILE_TYPES[name.slice(name.lastIndexOf(".") + 1) as FileExtension],
      "Cache-Control": "public, max-age=31536000, immutable",
      "Cross-Origin-Resource-Policy": "cross-origin",
      "X-Content-Type-Options": "nosniff",
    };
    if (!image) {
      // A file is only ever downloaded or played, never opened as a page of this site.
      headers["Content-Disposition"] = `attachment; filename="${fileLabel(name)}"`;
      headers["Content-Security-Policy"] = "default-src 'none'; sandbox";
      headers["Accept-Ranges"] = "bytes";
      if (range) headers["Content-Range"] = `bytes ${range.start}-${range.end}/${size}`;
    }
    return new Response(body, { status: range ? 206 : 200, headers });
  } catch {
    return NOT_FOUND();
  }
}

export const serveImage = serveMedia;

// Files arrive in parts, so a large one needs no large request and is never held in memory whole.

const PART = /^[a-f0-9]{32}$/;
const partPath = (id: string) => join(dataDir(), "media", `.part-${id}`);
const STALE_PART_MS = 86_400_000;

/** Adds a part where the last one ended. Returns the size so far, or why it was refused. */
export function appendPart(id: string, at: number, bytes: Uint8Array): number | "bad" | "too-large" {
  if (!PART.test(id) || !Number.isInteger(at) || at < 0) return "bad";
  const directory = join(dataDir(), "media");
  mkdirSync(directory, { recursive: true });
  const path = partPath(id);
  if (at === 0) {
    // An upload given up on leaves its parts behind; a new one clears out the old.
    for (const name of readdirSync(directory)) {
      if (!name.startsWith(".part-")) continue;
      try {
        if (Date.now() - statSync(join(directory, name)).mtimeMs > STALE_PART_MS) rmSync(join(directory, name), { force: true });
      } catch {}
    }
    rmSync(path, { force: true });
  }
  const have = at === 0 ? 0 : existsSync(path) ? statSync(path).size : -1;
  if (have !== at) return "bad";
  if (at + bytes.length > MAX_FILE_BYTES) {
    rmSync(path, { force: true });
    return "too-large";
  }
  appendFileSync(path, bytes);
  return at + bytes.length;
}

/** Content-addressed like an image, with the name it was uploaded under kept for the download. */
export async function finishFile(id: string, original: string, extension: FileExtension): Promise<string | null> {
  if (!PART.test(id) || !existsSync(partPath(id))) return null;
  const path = partPath(id);
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(path)) hash.update(chunk as Buffer);
  const name = `${hash.digest("hex").slice(0, 32)}-${fileSlug(original)}.${extension}`;
  const final = join(dataDir(), "media", name);
  if (existsSync(final)) rmSync(path, { force: true });
  else renameSync(path, final);
  return name;
}

export interface StoredImage {
  name: string;
  width?: number;
  height?: number;
  bytes: number;
  added: string;
  /** For a file that is not an image: the name it downloads as. */
  file?: string;
}

/** Every uploaded image, newest first. */
export const listImages = (): StoredImage[] => listMedia().filter((entry) => !entry.file);

/** Every uploaded image and file, newest first, from one read of the folder. Thumbnails go with their image. */
export function listMedia(): StoredImage[] {
  const directory = join(dataDir(), "media");
  let names: string[];
  try {
    names = readdirSync(directory);
  } catch {
    return [];
  }
  const images: (StoredImage & { at: number })[] = [];
  for (const name of names) {
    const file = fileLabel(name);
    if (!file && !isImageName(name)) continue;
    try {
      const { size, mtimeMs } = statSync(join(directory, name));
      images.push({ name, ...(file ? { file } : imageSize(name)), bytes: size, added: new Date(mtimeMs).toISOString(), at: mtimeMs });
    } catch {}
  }
  images.sort((a, b) => b.at - a.at || a.name.localeCompare(b.name));
  return images.map(({ at, ...image }) => image);
}

/** Removes an image and its thumbnail, or a file. False for a name Media could not have written, or one not there. */
export function deleteImage(name: string): boolean {
  if (!isMediaName(name)) return false;
  const directory = join(dataDir(), "media");
  if (!existsSync(join(directory, name))) return false;
  if (!isFileName(name)) rmSync(join(directory, thumbName(name)), { force: true });
  rmSync(join(directory, name), { force: true });
  return true;
}
