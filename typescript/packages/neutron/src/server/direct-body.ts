import type { ServerResponse } from "node:http";
import type { Http2ServerResponse } from "node:http2";

type Outgoing = ServerResponse | Http2ServerResponse;

const KNOWN_BODIES = new WeakMap<Response, Uint8Array>();

/**
 * Remember the bytes a response was built from, so `sendKnownBody` can write
 * them to the socket without reading them back out of the body stream.
 */
export function rememberBody<T extends Response>(response: T, body: Uint8Array): T {
  KNOWN_BODIES.set(response, body);
  return response;
}

/**
 * Write a response whose bytes are already in hand straight to the Node
 * response, and report whether it did.
 *
 * `@hono/node-server` sends a native `Response` by acquiring a reader on its
 * body and awaiting each chunk, which costs about a tenth of a prebuilt
 * page's request time for bytes the server already holds. The entry is keyed
 * by the response object, so a middleware that replaces the response (dynamic
 * compression, a rewritten body) simply misses and takes the ordinary path;
 * headers a middleware set on the same object are read here, at send time.
 */
export function sendKnownBody(response: Response, outgoing: Outgoing | undefined): boolean {
  const body = KNOWN_BODIES.get(response);
  if (
    body === undefined ||
    outgoing === undefined ||
    response.bodyUsed ||
    outgoing.headersSent ||
    outgoing.destroyed ||
    !outgoing.writable
  ) {
    return false;
  }
  const headers: Record<string, string | string[]> = {};
  response.headers.forEach((value, name) => {
    if (name !== "set-cookie") headers[name] = value;
  });
  const cookies = response.headers.getSetCookie();
  if (cookies.length > 0) headers["set-cookie"] = cookies;
  if (headers["transfer-encoding"] !== undefined) return false;
  headers["content-length"] = String(body.byteLength);
  (outgoing as ServerResponse).writeHead(response.status, headers);
  (outgoing as ServerResponse).end(body);
  return true;
}
