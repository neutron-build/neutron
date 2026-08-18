// Forced-error endpoints for the §2 error dimensions (error.rfc7807,
// error.contenttype, error.codes). One dynamic route covers the whole
// taxonomy: GET /errors/{code} throws the matching SDK taxonomy constructor,
// so what the conformance runner observes is the framework's RFC 7807 error
// pipeline (ProblemError -> problem+json), not an adapter-crafted Response.
//
// Mirrors the Go, Rust and Python conformance apps, which expose the same
// GET /errors/{code} surface from the same taxonomy.
import {
  badRequest,
  conflict,
  forbidden,
  internalError,
  notFoundError,
  rateLimited,
  unauthorized,
  type LoaderArgs,
} from "@neutron-build/core";

export const config = { mode: "app" };

const FORCERS: Record<string, () => never> = {
  "bad-request": () => {
    throw badRequest("forced bad-request");
  },
  unauthorized: () => {
    throw unauthorized("forced unauthorized");
  },
  forbidden: () => {
    throw forbidden("forced forbidden");
  },
  "not-found": () => {
    throw notFoundError("forced not-found");
  },
  conflict: () => {
    throw conflict("forced conflict");
  },
  "rate-limited": () => {
    throw rateLimited("forced rate-limited");
  },
  internal: () => {
    throw internalError("forced internal");
  },
};

export async function loader({ params }: LoaderArgs) {
  const force = FORCERS[params.code];
  if (!force) {
    throw notFoundError(`no forced error for ${params.code}`);
  }
  force();
}
