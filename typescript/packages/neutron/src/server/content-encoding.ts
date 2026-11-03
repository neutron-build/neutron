export type ContentCoding = "br" | "gzip";

/**
 * Pick the content coding to send from `Accept-Encoding` (RFC 9110 §12.5.3)
 * among `available`, in server preference order (br before gzip). Returns
 * null for identity: no header, nothing acceptable, or `identity` explicitly
 * preferred over every available coding. `q=0` excludes a coding, `*` covers
 * codings the header does not name.
 */
export function negotiateContentEncoding(
  header: string | null | undefined,
  available: readonly ContentCoding[]
): ContentCoding | null {
  if (!header || available.length === 0) return null;
  let wildcard: number | undefined;
  let identity: number | undefined;
  let br: number | undefined;
  let gzip: number | undefined;
  for (const part of header.split(",")) {
    const semicolon = part.indexOf(";");
    const token = (semicolon === -1 ? part : part.slice(0, semicolon)).trim().toLowerCase();
    if (!token) continue;
    let q = 1;
    if (semicolon !== -1) {
      const match = /(?:^|;)\s*q\s*=\s*([0-9.]+)/i.exec(part.slice(semicolon));
      if (match) {
        q = Number(match[1]);
        if (!Number.isFinite(q)) q = 0;
      }
    }
    if (token === "br") br = q;
    else if (token === "gzip" || token === "x-gzip") gzip = q;
    else if (token === "identity") identity = q;
    else if (token === "*") wildcard = q;
  }
  let best: ContentCoding | null = null;
  let bestQ = 0;
  for (const coding of available) {
    const q = (coding === "br" ? br : gzip) ?? wildcard ?? 0;
    if (q > bestQ) {
      best = coding;
      bestQ = q;
    }
  }
  if (best && identity !== undefined && identity > bestQ) return null;
  return best;
}
