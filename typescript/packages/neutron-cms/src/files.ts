/** What Media takes besides images. A fixed list: anything a browser would run as a page stays out. */
export const FILE_TYPES = {
  pdf: "application/pdf",
  zip: "application/zip",
  "7z": "application/x-7z-compressed",
  gz: "application/gzip",
  tar: "application/x-tar",
  mp3: "audio/mpeg",
  m4a: "audio/mp4",
  wav: "audio/wav",
  ogg: "audio/ogg",
  flac: "audio/flac",
  mp4: "video/mp4",
  webm: "video/webm",
  mov: "video/quicktime",
  txt: "text/plain; charset=utf-8",
  csv: "text/csv; charset=utf-8",
  json: "application/json",
  epub: "application/epub+zip",
  docx: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  xlsx: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  pptx: "application/vnd.openxmlformats-officedocument.presentationml.presentation",
} as const;
export type FileExtension = keyof typeof FILE_TYPES;

export const MAX_FILE_BYTES = 100 * 1024 * 1024;

const EXTENSIONS = Object.keys(FILE_TYPES).join("|");
const FILE = new RegExp(`^[a-f0-9]{32}-([a-z0-9-]{1,60})\\.(${EXTENSIONS})$`);

/** A stored file's name anywhere in a text: its content's hash, then the name it was uploaded under. */
export const FILE_NAME_IN_TEXT = new RegExp(`(?<![a-f0-9])([a-f0-9]{32}-[a-z0-9-]{1,60})\\.(${EXTENSIONS})(?![a-z0-9])`, "g");

export const isFileName = (name: string) => FILE.test(name);

/** The name a download is saved under, from a stored file's name or its address. Null for anything else. */
export function fileLabel(nameOrUrl: string): string | null {
  const match = nameOrUrl.slice(nameOrUrl.lastIndexOf("/") + 1).match(FILE);
  return match ? `${match[1]}.${match[2]}` : null;
}

/** The extension of an uploaded file's own name, when Media takes it. */
export function fileExtension(original: string): FileExtension | null {
  const extension = original.slice(original.lastIndexOf(".") + 1).toLowerCase();
  return original.includes(".") && Object.hasOwn(FILE_TYPES, extension) ? (extension as FileExtension) : null;
}

/** An uploaded file's own name, cut down to what is safe in an address. */
export function fileSlug(original: string): string {
  const stem = original.includes(".") ? original.slice(0, original.lastIndexOf(".")) : original;
  const slug = stem.normalize("NFKD").replace(/\p{M}+/gu, "").toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 60).replace(/-+$/, "");
  return slug || "file";
}

export const FILE_KINDS = "PDF, ZIP, 7z, audio (MP3, M4A, WAV, OGG, FLAC), video (MP4, WebM, MOV), text, CSV, JSON, EPUB, Word, Excel or PowerPoint";
