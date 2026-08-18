/**
 * RFC 7807 Problem Details errors (FRAMEWORK_CONTRACT.md §2).
 *
 * The contract says every Neutron framework MUST return errors as Problem
 * Details JSON served as `application/problem+json`, with the standard error
 * taxonomy of §2. Loaders, actions, middleware and user-mounted routes can
 * throw a {@link ProblemError} (usually via the taxonomy constructors below);
 * the render pipeline and the server convert it into the wire response, so a
 * thrown problem behaves like `throw redirect(...)` does for redirects.
 */
import type { ZodType } from "zod";

/** Media type for RFC 7807 Problem Details responses. */
export const PROBLEM_CONTENT_TYPE = "application/problem+json";

/** Base for `type` URIs, per FRAMEWORK_CONTRACT.md §2. */
export const PROBLEM_TYPE_BASE = "https://neutron.dev/errors/";

/** One entry of a validation problem's `errors[]` array (§2 validation format). */
export interface ProblemFieldError {
  field: string;
  message: string;
  value?: unknown;
}

export class ProblemError extends Error {
  override name = "ProblemError";
  readonly status: number;
  /** `type` URI suffix, e.g. "not-found" → https://neutron.dev/errors/not-found */
  readonly code: string;
  readonly title: string;
  readonly detail: string;
  /** Present on validation problems (422): one entry per failing field. */
  readonly fields?: ProblemFieldError[];

  constructor(
    status: number,
    code: string,
    title: string,
    detail: string,
    fields?: ProblemFieldError[]
  ) {
    super(detail);
    this.status = status;
    this.code = code;
    this.title = title;
    this.detail = detail;
    this.fields = fields;
  }

  /** The RFC 7807 response for this problem. `instance` is the request path. */
  toResponse(instance?: string): Response {
    const body: Record<string, unknown> = {
      type: `${PROBLEM_TYPE_BASE}${this.code}`,
      title: this.title,
      status: this.status,
      detail: this.detail,
    };
    if (instance !== undefined) {
      body.instance = instance;
    }
    if (this.fields && this.fields.length > 0) {
      body.errors = this.fields;
    }
    return new Response(JSON.stringify(body), {
      status: this.status,
      headers: { "Content-Type": PROBLEM_CONTENT_TYPE },
    });
  }
}

export function isProblemError(value: unknown): value is ProblemError {
  // Structural, not `instanceof`: a route module can carry its own copy of
  // this class (pnpm-aliased installs, or a test server running from src
  // while fixture routes import the built dist). The contract is the shape,
  // not the identity.
  return (
    value instanceof Error &&
    typeof (value as ProblemError).status === "number" &&
    typeof (value as ProblemError).code === "string" &&
    typeof (value as ProblemError).title === "string" &&
    typeof (value as ProblemError).detail === "string" &&
    typeof (value as ProblemError).toResponse === "function"
  );
}

// --- Taxonomy constructors (FRAMEWORK_CONTRACT.md §2 standard codes) ---

export function badRequest(detail: string): ProblemError {
  return new ProblemError(400, "bad-request", "Bad Request", detail);
}

export function unauthorized(detail: string): ProblemError {
  return new ProblemError(401, "unauthorized", "Unauthorized", detail);
}

export function forbidden(detail: string): ProblemError {
  return new ProblemError(403, "forbidden", "Forbidden", detail);
}

/**
 * A 404 ProblemError. Not named `notFound` — that is the HTML-page Response
 * helper in core/response.ts, and the two must not be confused.
 */
export function notFoundError(detail: string): ProblemError {
  return new ProblemError(404, "not-found", "Not Found", detail);
}

export function conflict(detail: string): ProblemError {
  return new ProblemError(409, "conflict", "Conflict", detail);
}

export function validationError(
  detail: string,
  fields?: ProblemFieldError[]
): ProblemError {
  return new ProblemError(422, "validation", "Validation Failed", detail, fields);
}

export function rateLimited(detail: string): ProblemError {
  return new ProblemError(429, "rate-limited", "Rate Limited", detail);
}

export function internalError(detail: string): ProblemError {
  return new ProblemError(500, "internal", "Internal Server Error", detail);
}

// --- Typed request-body validation ---

/**
 * Parse a request body as JSON and validate it against a zod schema.
 *
 * On success returns the parsed (typed) value. On failure throws the §2
 * problems: a body that is not valid JSON is a 400 `bad-request` (a syntax
 * error, not a schema violation); a schema failure is a 422 `validation`
 * problem whose `errors[]` carries one `{field, message, value?}` per failing
 * path, in the contract's validation format.
 */
export async function validateJsonBody<T>(
  request: Request,
  schema: ZodType<T>
): Promise<T> {
  let raw: unknown;
  try {
    raw = JSON.parse(await request.text());
  } catch {
    throw badRequest("Request body must be valid JSON");
  }

  const result = await schema.safeParseAsync(raw);
  if (!result.success) {
    throw validationError(
      "Request body failed validation",
      result.error.issues.map((issue) => {
        const field =
          issue.path.length > 0 ? issue.path.join(".") : "(body)";
        const fieldError: ProblemFieldError = { field, message: issue.message };
        const value = valueAtPath(raw, issue.path);
        if (value !== undefined) {
          fieldError.value = value;
        }
        return fieldError;
      })
    );
  }
  return result.data;
}

function valueAtPath(input: unknown, path: (string | number | symbol)[]): unknown {
  let current: unknown = input;
  for (const key of path) {
    if (
      current === null ||
      typeof current !== "object" ||
      !(key in (current as Record<string | number | symbol, unknown>))
    ) {
      return undefined;
    }
    current = (current as Record<string | number | symbol, unknown>)[key];
  }
  // Only echo primitives back: an object or array at the failing path would
  // bloat the problem document with arbitrary attacker-supplied structure.
  if (
    typeof current === "string" ||
    typeof current === "number" ||
    typeof current === "boolean" ||
    current === null
  ) {
    return current;
  }
  return undefined;
}
