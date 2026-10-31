/**
 * The redirect half of the data protocol (`X-Neutron-Data: true`).
 *
 * A client navigation fetches the target URL's loader data. If that request
 * is redirected — a loader's `redirect()`, an auth gate in middleware, a
 * `routes.redirects` rule — fetch's default `redirect: "follow"` hands back
 * the TARGET's data, and the router rendered it under the URL it asked for:
 * the address bar kept `/a` while showing `/b`, and any query on `/b` was
 * lost. So a 3xx answering a data request is not sent as a 3xx: it becomes a
 * `204` carrying the location in `X-Neutron-Redirect`, which fetch does not
 * follow, and the client router navigates there itself.
 *
 * Everything else on the redirect response (Set-Cookie above all) is kept.
 */
export const DATA_REDIRECT_HEADER = "X-Neutron-Redirect";

function isDataRequest(request: Request): boolean {
  return request.headers.get("X-Neutron-Data") === "true";
}

/**
 * The location a redirect answering `request` should report: same-origin
 * targets as a path (the client resolves them against its own origin, which
 * behind a proxy is not the origin the server saw), others as written.
 */
function reportedLocation(request: Request, location: string): string {
  let resolved: URL;
  try {
    resolved = new URL(location, request.url);
  } catch {
    return location;
  }
  if (resolved.origin === new URL(request.url).origin) {
    return resolved.pathname + resolved.search + resolved.hash;
  }
  return location;
}

/** `response` as a data-protocol redirect when it redirects a data request; otherwise unchanged. */
export function toDataRedirectResponse(request: Request, response: Response): Response {
  if (!isDataRequest(request)) return response;
  if (response.status < 300 || response.status > 399) return response;
  const location = response.headers.get("location");
  if (!location) return response;

  const headers = new Headers(response.headers);
  headers.delete("location");
  headers.delete("content-type");
  headers.delete("content-length");
  headers.delete("content-encoding");
  headers.set(DATA_REDIRECT_HEADER, reportedLocation(request, location));
  if (!headers.has("cache-control")) {
    headers.set("cache-control", "no-store");
  }
  return new Response(null, { status: 204, headers });
}
