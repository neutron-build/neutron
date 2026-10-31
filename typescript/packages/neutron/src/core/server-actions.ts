/**
 * Server Actions: the request half.
 *
 * A `"use server"` module imported by client code compiles, in the client
 * graph, to stubs that POST to {@link SERVER_ACTION_PATH} with the action's
 * id (vite/server-actions.ts generates them, client/server-actions.ts sends
 * them). This module answers that request — for `neutron-ts start`, the dev
 * server and the generated adapter runtime alike — given a way to resolve an
 * id to the function the build (or, in dev, Vite) registered for it.
 *
 * In order: method (POST only), the same-origin check (Sec-Fetch-Site, else
 * Origin against Host), the body cap, then the app's global middleware with
 * the action invoked inside it, so an auth gate covers actions exactly as it
 * covers pages. Unknown ids answer 404 from inside the chain. Errors follow
 * FRAMEWORK_CONTRACT §2: a thrown `ProblemError` answers as its problem, a
 * thrown (or returned) `Response` answers as itself, anything else is a
 * generic 500 whose message never reaches the client and is reported through
 * `onError`. A 3xx answer becomes the data protocol's 204 +
 * `X-Neutron-Redirect`, so the client navigates instead of fetch following.
 *
 * Kept free of `node:` imports except the action context's AsyncLocalStorage
 * (core/action-context.ts), which the edge runtimes provide (Cloudflare via
 * `nodejs_compat`, which the adapter enables when the bundle needs it).
 */
import { parse, stringify } from "devalue";
import { runMiddlewareChain } from "./middleware.js";
import { isResponse, mutableResponse } from "./response.js";
import { ProblemError, badRequest, forbidden, internalError, isProblemError, notFoundError } from "./problem.js";
import { toDataRedirectResponse } from "./data-redirect.js";
import { runInServerActionContext } from "./action-context.js";
import { capRequestBody, RequestBodyTooLargeError } from "../server/input-limits.js";
import {
  DEFAULT_SERVER_ACTION_BODY_LIMIT,
  SERVER_ACTION_ARGS_FIELD,
  SERVER_ACTION_BLOB_PREFIX,
  SERVER_ACTION_FORM_PREFIX,
  SERVER_ACTION_HEADER,
  SERVER_ACTION_PATH,
  SERVER_ACTION_RESULT_HEADER,
} from "./server-action-protocol.js";
import type { AppContext, MiddlewareFn } from "./types.js";

/** `server.serverActions` in neutron.config.ts / `createServer({ serverActions })`. */
export interface ServerActionsOptions {
  /**
   * Largest accepted action request body, in bytes (FormData uploads count).
   * @default 1048576 (1 MiB)
   */
  bodySizeLimit?: number;
  /**
   * Origins (`https://app.example.com`) allowed to call actions in addition
   * to the server's own — for a proxy that rewrites `Host`, or a sibling
   * origin that legitimately posts here. Exact match, no wildcards.
   */
  allowedOrigins?: string[];
}

export interface ResolvedServerAction {
  fn: (...args: unknown[]) => unknown;
  name: string;
}

export interface ServerActionHandlerOptions extends ServerActionsOptions {
  /** The registered function for `id`, or null (404). May throw (500, reported). */
  resolve: (id: string) => Promise<ResolvedServerAction | null> | ResolvedServerAction | null;
  globalMiddleware?: MiddlewareFn[];
  /** A failure the client sees only as a generic 500: an action throwing, an unserializable result. */
  onError?: (error: Error, action: { id: string; name?: string }) => void;
}

/**
 * Whether an action request comes from the server's own pages. A browser's
 * `Sec-Fetch-Site` is authoritative where present (it is computed against
 * the URL the browser sees, so a TLS-terminating proxy does not confuse it);
 * otherwise `Origin` must name this host. A request with neither is not a
 * browser's and cannot be a cross-site forgery. `allowedOrigins` admits the
 * listed origins whatever the other headers say.
 */
export function isSameOriginActionRequest(request: Request, allowedOrigins: string[] = []): boolean {
  const origin = request.headers.get("origin");
  if (origin && allowedOrigins.includes(origin)) return true;
  const site = request.headers.get("sec-fetch-site");
  if (site) return site === "same-origin";
  if (origin) {
    let originHost: string;
    try {
      originHost = new URL(origin).host;
    } catch {
      return false; // "null" (sandboxed/opaque origins) and garbage
    }
    let requestHost = request.headers.get("host");
    if (!requestHost) {
      try {
        requestHost = new URL(request.url).host;
      } catch {
        return false;
      }
    }
    return originHost.toLowerCase() === requestHost.toLowerCase();
  }
  return true;
}

function isBodyTooLarge(error: unknown): boolean {
  for (let current = error, depth = 0; current && depth < 4; depth++) {
    if (current instanceof RequestBodyTooLargeError) return true;
    current = (current as { cause?: unknown }).cause;
  }
  return false;
}

function payloadTooLarge(limit: number): ProblemError {
  return new ProblemError(
    413,
    "payload-too-large",
    "Payload Too Large",
    `The server action request body exceeds ${limit} bytes (server.serverActions.bodySizeLimit).`
  );
}

async function decodeArguments(request: Request): Promise<unknown[]> {
  const type = (request.headers.get("content-type") || "").toLowerCase();
  let form: FormData | null = null;
  let serialized: string;
  if (type.startsWith("multipart/form-data")) {
    form = await request.formData();
    const raw = form.get(SERVER_ACTION_ARGS_FIELD);
    if (typeof raw !== "string") throw new TypeError(`missing ${SERVER_ACTION_ARGS_FIELD}`);
    serialized = raw;
  } else {
    serialized = await request.text();
  }
  const args = parse(serialized, {
    FormData: (index: unknown) => {
      if (!form) throw new TypeError("FormData argument outside a multipart body");
      const prefix = `${SERVER_ACTION_FORM_PREFIX}${String(index)}:`;
      const data = new FormData();
      form.forEach((value, key) => {
        if (key.startsWith(prefix)) data.append(key.slice(prefix.length), value);
      });
      return data;
    },
    Blob: (index: unknown) => {
      const value = form?.get(`${SERVER_ACTION_BLOB_PREFIX}${String(index)}`);
      if (!(value instanceof Blob)) throw new TypeError("missing Blob argument");
      return value;
    },
  });
  if (!Array.isArray(args)) throw new TypeError("arguments are not a list");
  return args;
}

function withHeaders(response: Response, extra: Headers): Response {
  const mutable = mutableResponse(response);
  extra.forEach((value, name) => {
    if (name.toLowerCase() === "set-cookie") return;
    mutable.headers.set(name, value);
  });
  for (const cookie of extra.getSetCookie()) {
    mutable.headers.append("set-cookie", cookie);
  }
  if (!mutable.headers.has("cache-control")) {
    mutable.headers.set("cache-control", "no-store");
  }
  return mutable;
}

function methodNotAllowed(): Response {
  const response = new ProblemError(
    405,
    "method-not-allowed",
    "Method Not Allowed",
    "Server actions are invoked with POST."
  ).toResponse(SERVER_ACTION_PATH);
  response.headers.set("Allow", "POST");
  return response;
}

/** Answer one request to {@link SERVER_ACTION_PATH}. */
export async function handleServerActionRequest(
  request: Request,
  options: ServerActionHandlerOptions
): Promise<Response> {
  const limit = options.bodySizeLimit ?? DEFAULT_SERVER_ACTION_BODY_LIMIT;
  const responseHeaders = new Headers();
  const finish = (response: Response) =>
    toDataRedirectResponse(request, withHeaders(response, responseHeaders));

  if (request.method.toUpperCase() !== "POST") {
    return finish(methodNotAllowed());
  }
  if (!isSameOriginActionRequest(request, options.allowedOrigins)) {
    return finish(forbidden("Cross-site server action request rejected.").toResponse(SERVER_ACTION_PATH));
  }
  const id = request.headers.get(SERVER_ACTION_HEADER);
  if (!id) {
    return finish(badRequest(`Missing the ${SERVER_ACTION_HEADER} header.`).toResponse(SERVER_ACTION_PATH));
  }
  const declared = request.headers.get("content-length");
  if (declared !== null && Number(declared) > limit) {
    return finish(payloadTooLarge(limit).toResponse(SERVER_ACTION_PATH));
  }

  const capped = capRequestBody(request, limit);
  const context: AppContext = {};
  let actionName: string | undefined;

  const invoke = async (): Promise<Response> => {
    const action = await options.resolve(id);
    if (!action) {
      throw notFoundError("No server action is registered under this id.");
    }
    actionName = action.name;
    let args: unknown[];
    try {
      args = await decodeArguments(capped);
    } catch (error) {
      if (isBodyTooLarge(error)) throw payloadTooLarge(limit);
      throw badRequest("Malformed server action arguments.");
    }
    let result: unknown;
    try {
      result = await runInServerActionContext(
        { request: capped, context, responseHeaders, action: { id, name: action.name } },
        () => action.fn(...args)
      );
    } catch (error) {
      if (isResponse(error)) return error;
      throw error;
    }
    if (isResponse(result)) return result;
    let body: string;
    try {
      body = stringify(result);
    } catch (error) {
      throw new TypeError(
        `Server action "${action.name}" returned a value the serializer cannot encode ` +
          `(${(error as Error)?.message ?? String(error)}). Return plain data: objects, arrays, ` +
          "primitives, Date, Map, Set, BigInt, RegExp, URL."
      );
    }
    return new Response(body, {
      status: 200,
      headers: {
        "Content-Type": "application/json; charset=utf-8",
        [SERVER_ACTION_RESULT_HEADER]: "1",
      },
    });
  };

  try {
    const response = await runMiddlewareChain(options.globalMiddleware ?? [], capped, context, invoke);
    return finish(response);
  } catch (error) {
    if (isResponse(error)) return finish(error);
    if (isProblemError(error)) return finish(error.toResponse(SERVER_ACTION_PATH));
    if (isBodyTooLarge(error)) return finish(payloadTooLarge(limit).toResponse(SERVER_ACTION_PATH));
    options.onError?.(error instanceof Error ? error : new Error(String(error)), { id, name: actionName });
    return finish(internalError("The server action failed.").toResponse(SERVER_ACTION_PATH));
  }
}

/**
 * Where a server action response's `self` invalidation points: the same-origin
 * page that invoked it (its `Referer`), or null.
 */
export function serverActionReferrerPath(request: Request): string | null {
  const referer = request.headers.get("referer");
  if (!referer) return null;
  try {
    const from = new URL(referer);
    return from.origin === new URL(request.url).origin ? from.pathname : null;
  } catch {
    return null;
  }
}
