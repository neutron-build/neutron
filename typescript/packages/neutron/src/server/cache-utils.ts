import { createHash } from "node:crypto";

export function createEntityTag(body: string): string {
  const size = Buffer.byteLength(body, "utf-8");
  const digest = createHash("sha1").update(body).digest("hex").slice(0, 16);
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
