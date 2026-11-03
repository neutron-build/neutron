import * as fs from "node:fs";
import * as path from "node:path";
import { Readable } from "node:stream";
import type { Context, MiddlewareHandler, Next } from "hono";
import { getMimeType } from "hono/utils/mime";
import { negotiateContentEncoding, type ContentCoding } from "./content-encoding.js";
import { requestHasMatchingEtag } from "./cache-utils.js";

/**
 * Files under the build output (client assets, public files). Serves the
 * `.br` / `.gz` siblings `neutron-ts build` writes (core/precompress.ts) when
 * the client accepts them, with a strong ETag per representation and 304s
 * for `If-None-Match`. A file without siblings (an older build) is sent as
 * is, and the dynamic compression middleware still applies to it.
 *
 * Like the prerendered-HTML cache, the output directory is treated as
 * immutable while the server runs: a file's metadata, and the bytes of small
 * files, are kept after the first request.
 */

interface Representation {
  file: string;
  size: number;
  etag: string;
  body?: Uint8Array<ArrayBuffer>;
}

interface StaticFile {
  type: string;
  identity: Representation;
  br?: Representation;
  gzip?: Representation;
  available: ContentCoding[];
}

export interface StaticFileHandlerOptions {
  root: string;
  /** Serve precompressed siblings. Off when the server's compression is. */
  precompressed: boolean;
  /** Answers Range requests (identity bytes only). */
  rangeFallback: MiddlewareHandler;
}

const MAX_CACHED_FILE_BYTES = 512 * 1024;
const MAX_CACHED_TOTAL_BYTES = 64 * 1024 * 1024;

export function createStaticFileHandler(
  options: StaticFileHandlerOptions
): (c: Context, next: Next, cacheControl?: string) => Promise<Response | void> {
  const { root, precompressed, rangeFallback } = options;
  const files = new Map<string, StaticFile>();
  let cachedBytes = 0;

  const lookup = (filename: string): StaticFile | null => {
    if (/(?:^|[/\\])\.{1,2}(?:$|[/\\])|[/\\]{2,}|\0/.test(filename)) return null;
    const known = files.get(filename);
    if (known) return known;

    let file = path.join(root, filename);
    let stat = statFile(file);
    if (stat?.isDirectory()) {
      file = path.join(file, "index.html");
      stat = statFile(file);
    }
    if (!stat?.isFile()) return null;
    const identity = representation(file, stat, "");
    const entry: StaticFile = { type: getMimeType(file) || "application/octet-stream", identity, available: [] };
    for (const [coding, suffix] of [["br", ".br"], ["gzip", ".gz"]] as const) {
      const sibling = statFile(file + suffix);
      // A sibling older than its source is from an earlier build.
      if (sibling?.isFile() && sibling.mtimeMs >= stat.mtimeMs) {
        entry[coding] = representation(file + suffix, sibling, `-${coding}`);
        entry.available.push(coding);
      }
    }
    files.set(filename, entry);
    return entry;
  };

  const readBody = async (rep: Representation): Promise<Uint8Array<ArrayBuffer> | ReadableStream<Uint8Array>> => {
    if (rep.body) return rep.body;
    if (rep.size > MAX_CACHED_FILE_BYTES) {
      return Readable.toWeb(fs.createReadStream(rep.file)) as ReadableStream<Uint8Array>;
    }
    const body = await fs.promises.readFile(rep.file);
    if (cachedBytes + body.byteLength <= MAX_CACHED_TOTAL_BYTES) {
      rep.body = body;
      cachedBytes += body.byteLength;
    }
    return body;
  };

  return async (c, next, cacheControl) => {
    if (c.finalized) return next();
    const method = c.req.method;
    if (method !== "GET" && method !== "HEAD") return next();
    let filename: string;
    try {
      filename = decodeURI(c.req.path);
    } catch {
      return next();
    }
    const entry = lookup(filename);
    if (!entry) return next();
    if (method === "GET" && c.req.raw.headers.has("range")) {
      return rangeFallback(c, next);
    }

    const encoding = precompressed
      ? negotiateContentEncoding(c.req.raw.headers.get("accept-encoding"), entry.available)
      : null;
    const rep = encoding ? entry[encoding]! : entry.identity;
    const headers = new Headers({ "Content-Type": entry.type, ETag: rep.etag });
    if (cacheControl) headers.set("Cache-Control", cacheControl);
    if (entry.available.length > 0 && precompressed) headers.set("Vary", "Accept-Encoding");
    if (requestHasMatchingEtag(c.req.raw, rep.etag)) {
      return new Response(null, { status: 304, headers });
    }
    if (encoding) headers.set("Content-Encoding", encoding);
    headers.set("Content-Length", String(rep.size));
    if (method === "HEAD") return new Response(null, { status: 200, headers });
    try {
      return new Response(await readBody(rep), { status: 200, headers });
    } catch {
      files.delete(filename);
      return next();
    }
  };
}

function statFile(file: string): fs.Stats | null {
  try {
    return fs.statSync(file);
  } catch {
    return null;
  }
}

function representation(file: string, stat: fs.Stats, suffix: string): Representation {
  return {
    file,
    size: stat.size,
    etag: `"${stat.size.toString(16)}-${Math.floor(stat.mtimeMs).toString(16)}${suffix}"`,
  };
}
