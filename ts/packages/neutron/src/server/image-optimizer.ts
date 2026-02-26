import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";

export interface ImageParams {
  src: string;
  width: number;
  quality: number;
  format: "webp" | "avif" | "jpeg" | "png";
}

export interface ImageValidationError {
  error: string;
  status: number;
}

export interface ImageOptimizerOptions {
  publicDirs: string[];
  cacheDir: string;
}

const VALID_FORMATS = new Set(["webp", "avif", "jpeg", "png"]);
const MIN_WIDTH = 16;
const MAX_WIDTH = 3840;
const MIN_QUALITY = 1;
const MAX_QUALITY = 100;
const DEFAULT_QUALITY = 75;
const DEFAULT_FORMAT = "webp";

let sharpModule: any = undefined;
let sharpLoadAttempted = false;
let sharpWarningLogged = false;

export function validateImageParams(
  searchParams: URLSearchParams
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

  if (!decodedSrc.startsWith("/")) {
    return { error: "Image src must start with '/'", status: 400 };
  }

  // SECURITY: Check for path traversal BEFORE normalization
  // (path.normalize resolves ".." so checking after is useless!)
  if (decodedSrc.includes("..")) {
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

  const wParam = searchParams.get("w");
  if (!wParam) {
    return { error: "Missing 'w' (width) parameter", status: 400 };
  }

  const width = parseInt(wParam, 10);
  if (!Number.isFinite(width) || width < MIN_WIDTH || width > MAX_WIDTH) {
    return {
      error: `Width must be between ${MIN_WIDTH} and ${MAX_WIDTH}`,
      status: 400,
    };
  }

  const qParam = searchParams.get("q");
  let quality = DEFAULT_QUALITY;
  if (qParam) {
    quality = parseInt(qParam, 10);
    if (
      !Number.isFinite(quality) ||
      quality < MIN_QUALITY ||
      quality > MAX_QUALITY
    ) {
      return {
        error: `Quality must be between ${MIN_QUALITY} and ${MAX_QUALITY}`,
        status: 400,
      };
    }
  }

  const fmtParam = searchParams.get("fmt");
  let format = DEFAULT_FORMAT as ImageParams["format"];
  if (fmtParam) {
    if (!VALID_FORMATS.has(fmtParam)) {
      return {
        error: `Format must be one of: ${[...VALID_FORMATS].join(", ")}`,
        status: 400,
      };
    }
    format = fmtParam as ImageParams["format"];
  }

  return { src: normalizedSrc, width, quality, format };
}

export function resolveSourceFile(
  src: string,
  publicDirs: string[]
): string | null {
  for (const dir of publicDirs) {
    const resolved = path.resolve(dir, src.slice(1));
    const normalizedResolved = path.normalize(resolved);
    const normalizedDir = path.normalize(dir);

    // SECURITY: Fix logic error - use OR instead of AND
    if (!normalizedResolved.startsWith(normalizedDir + path.sep) || normalizedResolved === normalizedDir) {
      continue;
    }

    // SECURITY: Additional check using path.relative to prevent traversal
    const relative = path.relative(normalizedDir, normalizedResolved);
    if (relative.startsWith("..") || path.isAbsolute(relative)) {
      continue;
    }

    if (fs.existsSync(resolved) && fs.statSync(resolved).isFile()) {
      return resolved;
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
  if (sharpLoadAttempted) {
    return sharpModule;
  }

  sharpLoadAttempted = true;
  try {
    const sharpId = "sharp";
    sharpModule = (await import(sharpId)).default;
  } catch {
    sharpModule = null;
    if (!sharpWarningLogged) {
      console.warn(
        "[neutron] sharp is not installed. Images will be served without optimization. " +
          "Install sharp for image optimization: npm install sharp"
      );
      sharpWarningLogged = true;
    }
  }

  return sharpModule;
}

const FORMAT_TO_CONTENT_TYPE: Record<string, string> = {
  webp: "image/webp",
  avif: "image/avif",
  jpeg: "image/jpeg",
  png: "image/png",
};

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

  const sourcePath = resolveSourceFile(params.src, opts.publicDirs);
  if (!sourcePath) {
    return { error: "Image not found", status: 404 };
  }

  const sharp = await loadSharp();

  if (!sharp) {
    const buffer = fs.readFileSync(sourcePath);
    const ext = path.extname(sourcePath).toLowerCase().slice(1);
    const contentType =
      FORMAT_TO_CONTENT_TYPE[ext] ||
      FORMAT_TO_CONTENT_TYPE[params.format] ||
      "application/octet-stream";
    return { buffer, contentType };
  }

  try {
    let pipeline = sharp(sourcePath).resize(params.width);

    switch (params.format) {
      case "webp":
        pipeline = pipeline.webp({ quality: params.quality });
        break;
      case "avif":
        pipeline = pipeline.avif({ quality: params.quality });
        break;
      case "jpeg":
        pipeline = pipeline.jpeg({ quality: params.quality });
        break;
      case "png":
        pipeline = pipeline.png({ quality: params.quality });
        break;
    }

    const buffer = await pipeline.toBuffer();

    const cacheParentDir = path.dirname(cachePath);
    fs.mkdirSync(cacheParentDir, { recursive: true });
    fs.writeFileSync(cachePath, buffer);

    return {
      buffer,
      contentType: FORMAT_TO_CONTENT_TYPE[params.format] || "application/octet-stream",
    };
  } catch (err) {
    console.error("[neutron] Image optimization failed:", err);
    const buffer = fs.readFileSync(sourcePath);
    const ext = path.extname(sourcePath).toLowerCase().slice(1);
    const contentType =
      FORMAT_TO_CONTENT_TYPE[ext] ||
      FORMAT_TO_CONTENT_TYPE[params.format] ||
      "application/octet-stream";
    return { buffer, contentType };
  }
}

export async function handleImageRequest(
  request: Request,
  opts: ImageOptimizerOptions
): Promise<Response> {
  const url = new URL(request.url);
  const validated = validateImageParams(url.searchParams);

  if ("error" in validated) {
    return new Response(validated.error, { status: validated.status });
  }

  const result = await optimizeImage(validated, opts);

  if ("error" in result) {
    return new Response(result.error, { status: result.status });
  }

  return new Response(result.buffer as unknown as BodyInit, {
    headers: {
      "Content-Type": result.contentType,
      "Cache-Control": "public, max-age=31536000, immutable",
      "Content-Length": String(result.buffer.length),
    },
  });
}
