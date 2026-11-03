import * as zlib from "node:zlib";
import type { MiddlewareHandler } from "hono";
import { negotiateContentEncoding, type ContentCoding } from "./content-encoding.js";

/**
 * Dynamic response compression on node:zlib: brotli (quality 4) or gzip
 * (level 6), negotiated like the precompressed files (content-encoding.ts).
 *
 * A body that is complete when the handler returns — a rendered page, a JSON
 * payload — is compressed in one native call and sent with its length; one
 * under the threshold is sent as is. A body still being produced (streaming
 * SSR, a long-poll, anything chunked) is compressed chunk by chunk with a
 * flush after each, so every chunk reaches the client when it is written
 * rather than when the compressor's window fills. Event streams are never
 * compressed: proxies and clients expect them unencoded.
 */

const THRESHOLD = 1024;
const CODINGS: readonly ContentCoding[] = ["br", "gzip"];
const NO_TRANSFORM = /(?:^|,)\s*no-transform\s*(?:,|$)/i;
// hono's list of compressible types (hono/utils/compress), minus event streams.
const COMPRESSIBLE =
  /^\s*(?:text\/(?!event-stream(?:[;\s]|$))[^;\s]+|application\/(?:javascript|json|xml|xml-dtd|ecmascript|dart|postscript|rtf|tar|toml|vnd\.dart|vnd\.ms-fontobject|vnd\.ms-opentype|wasm|x-httpd-php|x-javascript|x-ns-proxy-autoconfig|x-sh|x-tar|x-virtualbox-hdd|x-virtualbox-ova|x-virtualbox-ovf|x-virtualbox-vbox|x-virtualbox-vdi|x-virtualbox-vhd|x-virtualbox-vmdk|x-www-form-urlencoded)|font\/(?:otf|ttf)|image\/(?:bmp|vnd\.adobe\.photoshop|vnd\.microsoft\.icon|vnd\.ms-dds|x-icon|x-ms-bmp)|message\/rfc822|model\/gltf-binary|x-shader\/x-fragment|x-shader\/x-vertex|[^;\s]+?\+(?:json|text|xml|yaml))(?:[;\s]|$)/i;

const BROTLI_OPTIONS: zlib.BrotliOptions = {
  params: {
    [zlib.constants.BROTLI_PARAM_QUALITY]: 4,
    [zlib.constants.BROTLI_PARAM_MODE]: zlib.constants.BROTLI_MODE_TEXT,
  },
};
const GZIP_OPTIONS: zlib.ZlibOptions = { level: 6 };

const brotliOnce = (input: Uint8Array, size: number) =>
  new Promise<Buffer>((resolve, reject) =>
    zlib.brotliCompress(
      input,
      { params: { ...BROTLI_OPTIONS.params, [zlib.constants.BROTLI_PARAM_SIZE_HINT]: size } },
      (error, out) => (error ? reject(error) : resolve(out))
    )
  );
const gzipOnce = (input: Uint8Array) =>
  new Promise<Buffer>((resolve, reject) =>
    zlib.gzip(input, GZIP_OPTIONS, (error, out) => (error ? reject(error) : resolve(out)))
  );

type ReadResult = ReadableStreamReadResult<Uint8Array>;

/** The read's result if it is already settled, else undefined (still pending). */
function settledRead(read: Promise<ReadResult>): Promise<ReadResult | undefined> {
  return Promise.race([read, Promise.resolve().then(() => undefined)]);
}

export function compression(): MiddlewareHandler {
  return async function neutronCompression(c, next) {
    await next();
    const res = c.res;
    const headers = res.headers;
    if (
      !res.body ||
      c.req.method === "HEAD" ||
      headers.has("Content-Encoding") ||
      headers.has("Transfer-Encoding")
    ) {
      return;
    }
    const declaredLength = headers.get("Content-Length");
    if (declaredLength !== null && Number(declaredLength) < THRESHOLD) return;
    const type = headers.get("Content-Type");
    if (!type || !COMPRESSIBLE.test(type)) return;
    const cacheControl = headers.get("Cache-Control");
    if (cacheControl && NO_TRANSFORM.test(cacheControl)) return;
    const encoding = negotiateContentEncoding(c.req.raw.headers.get("accept-encoding"), CODINGS);
    if (!encoding) return;

    // Drain what the body already holds without waiting on its producer.
    const reader = res.body.getReader();
    const chunks: Uint8Array[] = [];
    let size = 0;
    let pending: Promise<ReadResult> | undefined = reader.read();
    let result = await settledRead(pending);
    while (result !== undefined) {
      if (result.done) {
        pending = undefined;
        break;
      }
      chunks.push(result.value);
      size += result.value.byteLength;
      pending = reader.read();
      result = await settledRead(pending);
    }

    const replace = (body: BodyInit | null, length: number | null, encoded: boolean) => {
      const out = new Headers(headers);
      if (length === null) out.delete("Content-Length");
      else out.set("Content-Length", String(length));
      if (encoded) {
        out.set("Content-Encoding", encoding);
        const etag = out.get("ETag");
        if (etag && !etag.startsWith("W/")) out.set("ETag", `W/${etag}`);
      }
      c.res = undefined;
      c.res = new Response(body, { status: res.status, statusText: res.statusText, headers: out });
    };

    if (pending === undefined) {
      const whole = chunks.length === 1 ? chunks[0] : Buffer.concat(chunks, size);
      if (size < THRESHOLD) {
        replace(size === 0 ? null : (whole as Uint8Array<ArrayBuffer>), size, false);
        return;
      }
      const compressed = encoding === "br" ? await brotliOnce(whole, size) : await gzipOnce(whole);
      replace(compressed as Uint8Array<ArrayBuffer>, compressed.byteLength, true);
      return;
    }

    replace(streamCompressed(reader, chunks, pending, encoding), null, true);
  };
}

function streamCompressed(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  buffered: Uint8Array[],
  firstPending: Promise<ReadResult>,
  encoding: ContentCoding
): ReadableStream<Uint8Array> {
  const compressor = encoding === "br" ? zlib.createBrotliCompress(BROTLI_OPTIONS) : zlib.createGzip(GZIP_OPTIONS);
  const flushKind =
    encoding === "br" ? zlib.constants.BROTLI_OPERATION_FLUSH : zlib.constants.Z_SYNC_FLUSH;
  let pending: Promise<ReadResult> | undefined = firstPending;
  let initial: Uint8Array[] | undefined = buffered;

  const writeAndFlush = (chunks: Uint8Array[]) =>
    new Promise<void>((resolve) => {
      for (const chunk of chunks) compressor.write(chunk);
      compressor.flush(flushKind, () => resolve());
    });

  return new ReadableStream<Uint8Array>({
    start(controller) {
      compressor.on("data", (chunk: Buffer) => controller.enqueue(chunk));
      compressor.on("end", () => controller.close());
      compressor.on("error", (error) => controller.error(error));
    },
    async pull() {
      if (initial) {
        const chunks = initial;
        initial = undefined;
        if (chunks.length > 0) return writeAndFlush(chunks);
      }
      if (!pending) return;
      const result = await pending;
      if (result.done) {
        pending = undefined;
        compressor.end();
        return;
      }
      pending = reader.read();
      return writeAndFlush([result.value]);
    },
    cancel(reason) {
      compressor.destroy();
      return reader.cancel(reason);
    },
  });
}
