import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";

export interface ImageParams {
  src: string;
  width: number;
  quality: number;
  format: "webp" | "avif" | "jpeg" | "png";
  /** Present (true) only when `src` is an allowlisted absolute URL fetched
   *  over the network rather than read from a public dir. */
  remote?: boolean;
}

export interface ImageValidationError {
  error: string;
  status: number;
}

/** An allowlisted remote image origin (`images.remotePatterns` in the app
 *  config). `hostname` matches exactly; `protocol` (when set) must match
 *  the URL's scheme. */
export interface RemoteImagePattern {
  hostname: string;
  protocol?: string;
}

export interface ImageOptimizerOptions {
  publicDirs: string[];
  cacheDir: string;
  remotePatterns?: RemoteImagePattern[];
}

const VALID_FORMATS = new Set(["webp", "avif", "jpeg", "png"]);
const MIN_WIDTH = 16;
const MAX_WIDTH = 3840;
const MIN_QUALITY = 1;
const MAX_QUALITY = 100;
const DEFAULT_QUALITY = 75;
const DEFAULT_FORMAT = "webp";

/**
 * Negotiate the best image format based on the request Accept header.
 * If an explicit format is provided (via `fmt` query param), it takes priority.
 * Otherwise: prefer AVIF > WebP > JPEG based on what the client accepts
 * WITH a non-zero quality factor — `image/avif;q=0` explicitly forbids AVIF
 * and must not select it (TS-15).
 */
export function negotiateFormat(request: Request, requestedFormat?: string): { format: string; negotiated: boolean } {
  if (requestedFormat && VALID_FORMATS.has(requestedFormat)) {
    return { format: requestedFormat, negotiated: false };
  }

  const accept = request.headers.get("accept") || "";

  const accepts = (type: string): boolean => {
    let wildcard = -1; // -1 = unseen, else explicit q of the coding
    let explicit = -1;
    for (const item of accept.split(",")) {
      const [name, ...params] = item.split(";");
      const trimmed = name.trim().toLowerCase();
      if (trimmed !== type && trimmed !== "*") {
        continue;
      }
      let q = 1;
      for (const param of params) {
        const [key, ...rest] = param.trim().split("=");
        if (key.trim().toLowerCase() === "q") {
          const parsed = Number(rest.join("="));
          q = Number.isFinite(parsed) ? Math.min(Math.max(parsed, 0), 1) : 0;
        }
      }
      if (trimmed === "*") {
        wildcard = wildcard === -1 ? q : Math.min(wildcard, q);
      } else {
        explicit = explicit === -1 ? q : Math.min(explicit, q);
      }
    }
    if (explicit !== -1) return explicit > 0;
    if (wildcard !== -1) return wildcard > 0;
    return false;
  };

  if (accepts("image/avif")) {
    return { format: "avif", negotiated: true };
  }

  if (accepts("image/webp")) {
    return { format: "webp", negotiated: true };
  }

  return { format: "jpeg", negotiated: true };
}

let sharpModule: any = undefined;
let sharpPromise: Promise<any | null> | undefined;
let sharpWarningLogged = false;

export function validateImageParams(
  searchParams: URLSearchParams,
  remotePatterns: RemoteImagePattern[] = []
): ImageParams | ImageValidationError {
  const src = searchParams.get("src");
  if (!src) {
    return { error: "Missing 'src' parameter", status: 400 };
  }

  // SECURITY: Decode URL encoding to prevent traversal bypass
  let decodedSrc: string;
  try {
    decodedSrc = decodeURIComponent(src);
  } catch {
    return { error: "Invalid URL encoding", status: 400 };
  }

  // Remote images: only http(s) absolute URLs, only origins the app
  // explicitly allowlisted. Everything else — including any absolute URL
  // with no allowlist configured — is refused with the config named, so the
  // failure is fixable instead of a mystery 400.
  const asUrl = /^https?:\/\//i.test(decodedSrc)
    ? (() => {
        try {
          return new URL(decodedSrc);
        } catch {
          return null;
        }
      })()
    : null;
  if (asUrl) {
    const allowed = remotePatterns.some(
      (pattern) =>
        pattern.hostname === asUrl.hostname &&
        (pattern.protocol === undefined ||
          pattern.protocol === asUrl.protocol.replace(":", ""))
    );
    if (!allowed) {
      return {
        error:
          `Remote image host "${asUrl.hostname}" is not allowlisted. Add it to ` +
          `images.remotePatterns in neutron.config.ts (e.g. { hostname: "${asUrl.hostname}" }) ` +
          `to permit optimization of remote images from this origin.`,
        status: 400,
      };
    }
    const widthRemote = parseWidth(searchParams);
    if (typeof widthRemote !== "number") {
      return widthRemote;
    }
    const qualityRemote = parseQuality(searchParams);
    if (typeof qualityRemote !== "number") {
      return qualityRemote;
    }
    const formatRemote = parseFormat(searchParams);
    if (typeof formatRemote !== "string") {
      return formatRemote;
    }
    return {
      src: decodedSrc,
      width: widthRemote,
      quality: qualityRemote,
      format: formatRemote,
      remote: true,
    };
  }

  if (!decodedSrc.startsWith("/")) {
    return { error: "Image src must start with '/'", status: 400 };
  }

  // SECURITY: backslash and NUL are never legal in a URL path; on Windows a
  // decoded `..\` component is a separator and escapes the serving root even
  // though it is not a slash-delimited `..` segment (TS-10).
  if (/[\\\0]/.test(decodedSrc)) {
    return { error: "Invalid characters in image path", status: 400 };
  }

  // SECURITY: Check for path traversal BEFORE normalization
  // (path.normalize resolves ".." so checking after is useless!)
  if (decodedSrc.split("/").includes("..")) {
    return { error: "Path traversal not allowed", status: 400 };
  }

  // SECURITY: Normalize path after validation (use posix for URL paths)
  // URL paths always use forward slashes, not OS-specific separators
  const normalizedSrc = path.posix.normalize(decodedSrc);

  // Double-check after normalization (defense in depth)
  if (!normalizedSrc.startsWith("/")) {
    return { error: "Path traversal not allowed", status: 400 };
  }

  if (/^\/\//.test(decodedSrc) || /^\/[a-z]+:/i.test(decodedSrc)) {
    return { error: "Absolute URLs not allowed", status: 400 };
  }

  const width = parseWidth(searchParams);
  if (typeof width !== "number") {
    return width;
  }
  const quality = parseQuality(searchParams);
  if (typeof quality !== "number") {
    return quality;
  }
  const format = parseFormat(searchParams);
  if (typeof format !== "string") {
    return format;
  }

  return { src: normalizedSrc, width, quality, format };
}

function parseWidth(searchParams: URLSearchParams): number | ImageValidationError {
  const wParam = searchParams.get("w");
  if (!wParam) {
    return { error: "Missing 'w' (width) parameter", status: 400 };
  }
  // Strict decimal integer (TS-15): `parseInt("200junk")` silently produced
  // 200, accepting malformed input.
  if (!/^\d+$/.test(wParam)) {
    return { error: "Width must be a decimal integer", status: 400 };
  }
  const width = Number(wParam);
  if (!Number.isSafeInteger(width) || width < MIN_WIDTH || width > MAX_WIDTH) {
    return {
      error: `Width must be between ${MIN_WIDTH} and ${MAX_WIDTH}`,
      status: 400,
    };
  }
  return width;
}

function parseQuality(searchParams: URLSearchParams): number | ImageValidationError {
  const qParam = searchParams.get("q");
  if (!qParam) {
    return DEFAULT_QUALITY;
  }
  if (!/^\d+$/.test(qParam)) {
    return { error: "Quality must be a decimal integer", status: 400 };
  }
  const quality = Number(qParam);
  if (!Number.isSafeInteger(quality) || quality < MIN_QUALITY || quality > MAX_QUALITY) {
    return {
      error: `Quality must be between ${MIN_QUALITY} and ${MAX_QUALITY}`,
      status: 400,
    };
  }
  return quality;
}

function parseFormat(
  searchParams: URLSearchParams
): ImageParams["format"] | ImageValidationError {
  const fmtParam = searchParams.get("fmt");
  if (!fmtParam) {
    return DEFAULT_FORMAT as ImageParams["format"];
  }
  if (!VALID_FORMATS.has(fmtParam)) {
    return {
      error: `Format must be one of: ${[...VALID_FORMATS].join(", ")}`,
      status: 400,
    };
  }
  return fmtParam as ImageParams["format"];
}

export async function resolveSourceFile(
  src: string,
  publicDirs: string[]
): Promise<string | null> {
  // Filesystem-resolved containment (TS-10): the previous check was purely
  // lexical, so a symlink inside a serving root that pointed elsewhere was
  // followed without complaint. Resolve both root and target through the
  // filesystem and require the target to live under the REAL root.
  for (const dir of publicDirs) {
    const decodedPath = src.startsWith("/") ? src : `/${src}`;
    if (/[\\\0]/.test(decodedPath)) {
      continue;
    }
    try {
      const realRoot = await fs.promises.realpath(dir);
      const target = await fs.promises.realpath(
        path.resolve(realRoot, "." + decodedPath)
      );
      const rel = path.relative(realRoot, target);
      if (!rel || rel === ".." || rel.startsWith(`..${path.sep}`) || path.isAbsolute(rel)) {
        continue;
      }
      const stat = await fs.promises.stat(target);
      if (stat.isFile()) {
        return target;
      }
    } catch {
      // Missing file / broken symlink in this root — try the next.
      continue;
    }
  }

  return null;
}

export function buildCacheKey(params: ImageParams): string {
  const hash = crypto
    .createHash("sha256")
    .update(
      JSON.stringify({
        src: params.src,
        width: params.width,
        quality: params.quality,
        format: params.format,
      })
    )
    .digest("hex");

  return hash;
}

export function buildCachePath(cacheDir: string, params: ImageParams): string {
  const hash = buildCacheKey(params);
  return path.join(cacheDir, hash.slice(0, 2), `${hash}.${params.format}`);
}

async function loadSharp(): Promise<any> {
  // Cache the PROMISE, not the attempt flag (TS-15): `sharpLoadAttempted`
  // was set before the dynamic import resolved, so concurrent first
  // requests observed an undefined module and fell into the raw-bytes
  // fallback. Every caller now awaits the same in-flight import.
  sharpPromise ??= (async () => {
    try {
      const sharpId = "sharp";
      return (await import(/* @vite-ignore */ sharpId)).default;
    } catch {
      if (!sharpWarningLogged) {
        console.warn(
          "[neutron] sharp is not installed. The image endpoint will fail " +
            "closed (503) until it is: npm install sharp"
        );
        sharpWarningLogged = true;
      }
      return null;
    }
  })();
  sharpModule = await sharpPromise;
  return sharpModule;
}

const FORMAT_TO_CONTENT_TYPE: Record<string, string> = {
  webp: "image/webp",
  avif: "image/avif",
  jpeg: "image/jpeg",
  png: "image/png",
};

/** Hard caps on fetching a remote image: a slow or huge origin must not pin
 *  a request or fill the disk-backed cache with attacker-chosen bulk. */
const REMOTE_FETCH_TIMEOUT_MS = 10_000;
const REMOTE_FETCH_MAX_BYTES = 20 * 1024 * 1024;

/** Read a body stream up to `limit` bytes (TS-12): the cap is enforced on
 *  ACTUAL bytes as they arrive — never allocated first — and the upstream is
 *  cancelled when the cap is exceeded or the read fails. */
async function readBounded(
  body: ReadableStream<Uint8Array> | null,
  limit: number
): Promise<Uint8Array> {
  if (body === null) {
    return new Uint8Array();
  }
  const reader = body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > limit) {
        throw new RangeError("body exceeds byte limit");
      }
      chunks.push(value);
    }
    const out = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) {
      out.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return out;
  } catch (error) {
    void reader.cancel(error).catch(() => {});
    throw error;
  } finally {
    reader.releaseLock();
  }
}

async function fetchRemoteImage(
  src: string,
  opts: ImageOptimizerOptions
): Promise<Buffer | { error: string; status: number }> {
  // Credentials in the URL are rejected outright (TS-11).
  let url: URL;
  try {
    url = new URL(src);
  } catch {
    return { error: `Invalid remote image URL: ${src}`, status: 400 };
  }
  if (url.username || url.password) {
    return { error: "Remote image URL must not carry credentials", status: 400 };
  }
  try {
    // Redirects are refused, not followed (TS-11): `redirect: "follow"` let
    // an allowlisted origin with an open redirect aim the fetch at any
    // disallowed origin, port, or internal service. Supporting redirects
    // safely would mean re-validating the allowlist at every hop; refusing
    // is the containment.
    const response = await fetch(url.toString(), {
      signal: AbortSignal.timeout(REMOTE_FETCH_TIMEOUT_MS),
      redirect: "error",
    });
    if (!response.ok) {
      return { error: `Remote image returned ${response.status}`, status: 502 };
    }
    const contentType = response.headers.get("content-type") || "";
    if (!contentType.startsWith("image/")) {
      return {
        error: `Remote image at ${src} is "${contentType}", not an image`,
        status: 415,
      };
    }

    // Enforce the cap on the actual byte stream (TS-12): a chunked,
    // omitted, or lying Content-Length used to allocate the full body
    // before the size was ever checked.
    let bytes: Uint8Array;
    try {
      bytes = await readBounded(response.body, REMOTE_FETCH_MAX_BYTES);
    } catch (error) {
      if (error instanceof RangeError) {
        return { error: "Remote image exceeds the 20 MB fetch cap", status: 413 };
      }
      return { error: `Failed to read remote image: ${src}`, status: 502 };
    }
    return Buffer.from(bytes);
  } catch {
    return { error: `Failed to fetch remote image: ${src}`, status: 502 };
  }
}

export async function optimizeImage(
  params: ImageParams,
  opts: ImageOptimizerOptions
): Promise<{ buffer: Buffer; contentType: string } | { error: string; status: number }> {
  const cachePath = buildCachePath(opts.cacheDir, params);

  try {
    await fs.promises.access(cachePath);
    const buffer = await fs.promises.readFile(cachePath);
    return {
      buffer: Buffer.from(buffer),
      contentType: FORMAT_TO_CONTENT_TYPE[params.format] || "application/octet-stream",
    };
  } catch {
    // cache miss, continue to optimize
  }

  // Remote (allowlisted) source: fetch the bytes; the sharp pipeline below
  // runs on the buffer instead of a local path.
  let sourceBytes: Buffer | null = null;
  let sourcePath: string | null = null;
  if (params.remote) {
    const remote = await fetchRemoteImage(params.src, opts);
    if ("error" in remote) {
      return remote;
    }
    sourceBytes = remote;
  } else {
    sourcePath = await resolveSourceFile(params.src, opts.publicDirs);
    if (!sourcePath) {
      return { error: "Image not found", status: 404 };
    }
  }

  // Fail closed (TS-13): with sharp unavailable there is no way to verify
  // the bytes are an image at the requested transform — the old fallback
  // served the RAW source file through this endpoint, turning the image
  // optimizer into an arbitrary-file reader for anything inside (or
  // symlinked into) the serving roots. Optimization support being missing
  // is a service error, not a reason to skip validation.
  const sharp = await loadSharp();
  if (!sharp) {
    return { error: "Image optimization is unavailable (sharp not installed)", status: 503 };
  }

  try {
    // Validate that the source decodes as a supported image before
    // transforming it (TS-13): a readable non-image inside an allowed root
    // (a .txt, a manifest) must be a 415, never a served file.
    const pipeline = sharp(sourceBytes ?? sourcePath, {
      limitInputPixels: 40_000_000,
    });
    const metadata = await pipeline.metadata();
    if (
      !metadata.format ||
      !new Set(["jpeg", "png", "webp", "avif", "gif", "svg"]).has(metadata.format)
    ) {
      return { error: "Unsupported image", status: 415 };
    }

    let transformed = sharp(sourceBytes ?? sourcePath, {
      limitInputPixels: 40_000_000,
    }).resize(params.width);

    switch (params.format) {
      case "webp":
        transformed = transformed.webp({ quality: params.quality });
        break;
      case "avif":
        transformed = transformed.avif({ quality: params.quality });
        break;
      case "jpeg":
        transformed = transformed.jpeg({ quality: params.quality });
        break;
      case "png":
        transformed = transformed.png({ quality: params.quality });
        break;
    }

    const buffer = await transformed.toBuffer();

    // Atomic publication (TS-14): write to a same-directory temp file and
    // rename it over the final path, so a concurrent reader never sees a
    // partially written entry. Asynchronous IO throughout — the sync writes
    // blocked the event loop.
    await fs.promises.mkdir(path.dirname(cachePath), { recursive: true });
    const tempPath = `${cachePath}.${process.pid}.${Date.now()}.${Math.random()
      .toString(36)
      .slice(2)}.tmp`;
    try {
      await fs.promises.writeFile(tempPath, buffer);
      await fs.promises.rename(tempPath, cachePath);
    } catch {
      await fs.promises.unlink(tempPath).catch(() => {});
    }

    return {
      buffer,
      contentType: FORMAT_TO_CONTENT_TYPE[params.format] || "application/octet-stream",
    };
  } catch {
    // Decode/transform failure is a 415 (TS-13): serving the original bytes
    // would bypass every guarantee this endpoint makes about what it emits.
    return { error: "Invalid or unsupported image", status: 415 };
  }
}

export async function handleImageRequest(
  request: Request,
  opts: ImageOptimizerOptions
): Promise<Response> {
  const url = new URL(request.url);
  const validated = validateImageParams(url.searchParams, opts.remotePatterns ?? []);

  if ("error" in validated) {
    return new Response(validated.error, { status: validated.status });
  }

  // Content negotiation: if no explicit fmt was requested, pick from Accept header
  const explicitFmt = url.searchParams.get("fmt");
  const { format, negotiated } = negotiateFormat(request, explicitFmt || undefined);
  validated.format = format as ImageParams["format"];

  const result = await optimizeImage(validated, opts);

  if ("error" in result) {
    return new Response(result.error, { status: result.status });
  }

  const headers: Record<string, string> = {
    "Content-Type": result.contentType,
    "Cache-Control": "public, max-age=31536000, immutable",
    "Content-Length": String(result.buffer.length),
  };

  if (negotiated) {
    headers["Vary"] = "Accept";
  }

  return new Response(result.buffer as unknown as BodyInit, { headers });
}
