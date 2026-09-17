import { createHash } from "node:crypto";

/**
 * Build a weak ETag over a response body. Byte-exact for Uint8Array bodies
 * (TS-06); the string overload sizes via UTF-8 to match the encoded bytes.
 */
export function createEntityTag(body: string | Uint8Array): string {
  const bytes = typeof body === "string" ? Buffer.from(body, "utf-8") : Buffer.from(body);
  const size = bytes.byteLength;
  const digest = createHash("sha1").update(bytes).digest("hex").slice(0, 16);
  return `W/"${size.toString(16)}-${digest}"`;
}

function normalizeEtagValue(value: string): string {
  return value.trim().replace(/^W\//i, "");
}

export function requestHasMatchingEtag(request: Request, etag: string): boolean {
  const ifNoneMatch = request.headers.get("If-None-Match");
  if (!ifNoneMatch) {
    return false;
  }
  if (ifNoneMatch.trim() === "*") {
    return true;
  }

  const normalizedEtag = normalizeEtagValue(etag);
  return ifNoneMatch
    .split(",")
    .map((part) => normalizeEtagValue(part))
    .some((part) => part === normalizedEtag);
}
