/**
 * Server Actions: the browser half. A client import of a `"use server"`
 * module compiles to one {@link createServerActionReference} per export
 * (vite/server-actions.ts); calling it POSTs the arguments to the server's
 * action endpoint and resolves with the action's return value.
 *
 * Arguments and results use the loader-data serializer (devalue: Date, Map,
 * Set, BigInt, RegExp, URL, repeated references). FormData and Blob/File
 * arguments are sent as multipart. A value the serializer cannot encode
 * (a function, a class instance) rejects before anything is sent.
 */
import { parse, stringify } from "devalue";
import { dataRedirectTarget, followDataRedirect, navigate } from "./navigate.js";
import {
  SERVER_ACTION_ARGS_FIELD,
  SERVER_ACTION_BLOB_PREFIX,
  SERVER_ACTION_FORM_PREFIX,
  SERVER_ACTION_HEADER,
  SERVER_ACTION_PATH,
  SERVER_ACTION_RESULT_HEADER,
} from "../core/server-action-protocol.js";

/** A server action that failed on the server: its HTTP status and RFC 7807 problem, when there is one. */
export class ServerActionError extends Error {
  override name = "ServerActionError";
  constructor(
    message: string,
    readonly status: number,
    /** The problem document (`type`, `title`, `status`, `detail`, …) the server answered with. */
    readonly problem?: Record<string, unknown>
  ) {
    super(message);
  }
}

function encodeArguments(name: string, args: unknown[]): { body: BodyInit; contentType?: string } {
  const forms: FormData[] = [];
  const blobs: Blob[] = [];
  let serialized: string;
  try {
    serialized = stringify(args, {
      FormData: (value: unknown) => value instanceof FormData && String(forms.push(value) - 1),
      Blob: (value: unknown) => value instanceof Blob && String(blobs.push(value) - 1),
    });
  } catch (error) {
    const where = (error as { path?: string })?.path;
    throw new TypeError(
      `Server action "${name}": an argument cannot be serialized` +
        (where ? ` (at arguments${where})` : "") +
        `: ${(error as Error)?.message ?? String(error)}. Pass plain data, FormData or Blobs.`
    );
  }
  if (forms.length === 0 && blobs.length === 0) {
    return { body: serialized, contentType: "application/json" };
  }
  const body = new FormData();
  body.append(SERVER_ACTION_ARGS_FIELD, serialized);
  forms.forEach((form, index) => {
    form.forEach((value, key) => {
      body.append(`${SERVER_ACTION_FORM_PREFIX}${index}:${key}`, value);
    });
  });
  blobs.forEach((blob, index) => body.append(`${SERVER_ACTION_BLOB_PREFIX}${index}`, blob));
  // The browser sets the multipart boundary itself.
  return { body };
}

function csrfToken(): string | null {
  if (typeof document === "undefined") return null;
  return document.querySelector<HTMLMetaElement>('meta[name="csrf-token"]')?.content || null;
}

function routerActive(): boolean {
  return typeof window !== "undefined" && (window as { __NEUTRON_ROUTER_ACTIVE__?: boolean }).__NEUTRON_ROUTER_ACTIVE__ === true;
}

/** Whether an `x-neutron-invalidate` directive covers the page on screen. */
function invalidatesCurrentPage(directive: string): boolean {
  const here = window.location.pathname.replace(/\/+$/, "") || "/";
  return directive
    .split(",")
    .map((token) => token.trim())
    .some((token) => token === "*" || token === "self" || (token.replace(/\/+$/, "") || "/") === here);
}

async function readError(name: string, response: Response): Promise<ServerActionError> {
  const type = response.headers.get("content-type") || "";
  if (type.includes("json")) {
    try {
      const problem = (await response.json()) as Record<string, unknown>;
      const detail = typeof problem.detail === "string" ? problem.detail : undefined;
      const title = typeof problem.title === "string" ? problem.title : undefined;
      return new ServerActionError(
        `Server action "${name}" failed (${response.status}): ${detail ?? title ?? response.statusText}`,
        response.status,
        problem
      );
    } catch {
      // fall through to the text form
    }
  }
  const text = await response.text().catch(() => "");
  return new ServerActionError(
    `Server action "${name}" failed (${response.status})${text ? `: ${text.slice(0, 200)}` : ""}`,
    response.status
  );
}

/**
 * Call the action `id`. Resolves with its return value; with the `Response`
 * itself when the action answered with a non-redirect Response of its own;
 * with `undefined` once a redirect it answered with has started navigating.
 * Rejects with {@link ServerActionError} when the server answered an error.
 */
export async function callServerAction(id: string, name: string, args: unknown[]): Promise<unknown> {
  const { body, contentType } = encodeArguments(name, args);
  const headers: Record<string, string> = {
    [SERVER_ACTION_HEADER]: id,
    Accept: "application/json",
    // Redirects come back as 204 + X-Neutron-Redirect (core/data-redirect.ts).
    "X-Neutron-Data": "true",
  };
  if (contentType) headers["Content-Type"] = contentType;
  const token = csrfToken();
  if (token) headers["x-csrf-token"] = token;

  const response = await fetch(SERVER_ACTION_PATH, {
    method: "POST",
    headers,
    body,
    credentials: "same-origin",
  });

  const redirectTo = dataRedirectTarget(response);
  if (redirectTo !== null) {
    if (routerActive()) {
      followDataRedirect(redirectTo);
    } else {
      window.location.assign(redirectTo);
    }
    return undefined;
  }

  if (!response.ok) {
    throw await readError(name, response);
  }

  let value: unknown = response;
  if (response.headers.get(SERVER_ACTION_RESULT_HEADER) === "1") {
    value = parse(await response.text());
  }

  const invalidate = response.headers.get("x-neutron-invalidate");
  if (invalidate && routerActive() && invalidatesCurrentPage(invalidate)) {
    // Same URL: the router revalidates the page on screen in place.
    navigate((window.location.pathname + window.location.search) as never);
  }
  return value;
}

/** The client stand-in for one export of a `"use server"` module. */
export function createServerActionReference(id: string, name: string): (...args: unknown[]) => Promise<unknown> {
  const reference = (...args: unknown[]) => callServerAction(id, name, args);
  Object.defineProperty(reference, "name", { value: name });
  Object.defineProperty(reference, "$$neutronAction", { value: id });
  return reference;
}
