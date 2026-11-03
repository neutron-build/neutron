import * as crypto from "node:crypto";
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
 * A file's metadata and ETag, and the bytes of small files, are kept after
 * the first request. Every request still stats the file and rebuilds the
 * entry when its size, mtime, ctime or inode changed, so a build run in place
 * under a live server never sends a cached length for different bytes.
 * ETags hash the content: reproducible builds give every file the same mtime.
 */

interface Representation {
  file: string;
  size: number;
  etag: string;
  body?: Uint8Array<ArrayBuffer>;
}

interface StaticFile {
  /** Identity file's size/mtime/ctime/inode when the entry was built. */
  signature: string;
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

  const forget = (entry: StaticFile) => {
    for (const rep of [entry.identity, entry.br, entry.gzip]) {
      if (rep?.body) cachedBytes -= rep.body.byteLength;
    }
  };

  const lookup = async (filename: string): Promise<StaticFile | null> => {
    if (/(?:^|[/\\])\.{1,2}(?:$|[/\\])|[/\\]{2,}|\0/.test(filename)) return null;
    const known = files.get(filename);
    let file = known?.identity.file ?? path.join(root, filename);
    let stat = await statFile(file);
    if (!known && stat?.isDirectory()) {
      file = path.join(file, "index.html");
      stat = await statFile(file);
    }
    if (!stat?.isFile()) {
      if (known) {
        forget(known);
        files.delete(filename);
      }
      return null;
    }
    // ctime too: tools can set mtime (reproducible builds) but not ctime, and
    // a deleted file's inode is reused at once on ext4.
    const signature = `${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}:${stat.ino}`;
    if (known?.signature === signature) return known;
    if (known) forget(known);

    const identity = await representation(file, stat, "");
    if (!identity) return null;
    const entry: StaticFile = { signature, type: getMimeType(file) || "application/octet-stream", identity, available: [] };
    for (const [coding, suffix] of [["br", ".br"], ["gzip", ".gz"]] as const) {
      const sibling = await statFile(file + suffix);
      // A sibling older than its source is from an earlier build.
      if (sibling?.isFile() && sibling.mtimeMs >= stat.mtimeMs) {
        const rep = await representation(file + suffix, sibling, `-${coding}`, identity.etag);
        if (rep) {
          entry[coding] = rep;
          entry.available.push(coding);
        }
      }
    }
    files.set(filename, entry);
    return entry;
  };

  const readBody = async (rep: Representation): Promise<Uint8Array<ArrayBuffer> | ReadableStream<Uint8Array>> => {
    if (rep.body) return rep.body;
    if (rep.size > MAX_CACHED_FILE_BYTES) {
      return Readable.toWeb(fs.createReadStream(rep.file, { start: 0, end: Math.max(0, rep.size - 1) })) as ReadableStream<Uint8Array>;
    }
    const body = await fs.promises.readFile(rep.file);
    if (body.byteLength !== rep.size) throw new Error("changed while serving");
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
    const entry = await lookup(filename);
    if (!entry) return next();
    if (method === "GET" && c.req.raw.headers.has("range")) {
      const partial = await rangeFallback(c, next);
      const response = partial ?? c.res;
      if (cacheControl && response && !response.headers.has("Cache-Control")) {
        response.headers.set("Cache-Control", cacheControl);
      }
      return partial;
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
      forget(entry);
      files.delete(filename);
      return next();
    }
  };
}

async function statFile(file: string): Promise<fs.Stats | null> {
  try {
    return await fs.promises.stat(file);
  } catch {
    return null;
  }
}

/**
 * A representation with a content-hash ETag. A precompressed sibling's tag is
 * its source's tag plus the coding: the two always change together.
 */
async function representation(
  file: string,
  stat: fs.Stats,
  suffix: string,
  sourceEtag?: string
): Promise<Representation | null> {
  if (sourceEtag) {
    return { file, size: stat.size, etag: `${sourceEtag.slice(0, -1)}${suffix}"` };
  }
  try {
    const hash = crypto.createHash("sha256");
    for await (const chunk of fs.createReadStream(file)) hash.update(chunk as Buffer);
    return { file, size: stat.size, etag: `"${hash.digest("base64url").slice(0, 27)}${suffix}"` };
  } catch {
    return null;
  }
}
