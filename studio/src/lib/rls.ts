// Row-level security makes Nucleus's specialty stores (KV, document, graph,
// FTS, time series, blob, streams, columnar, datalog, CDC, pub/sub) fail
// closed: they have no policy-aware access path, so every store function is
// denied for a non-superuser while any RLS policy is active. The engine error
// already says this; these helpers let the UI recognize it and explain it
// once, calmly, instead of stacking raw error toasts.

const RLS_DENIED_RE = /unavailable while row-level security is active/i

export function isRlsDenied(message: string | undefined | null): boolean {
  return !!message && RLS_DENIED_RE.test(message)
}

export const RLS_EXPLANATION =
  'This store is unavailable because row-level security (RLS) is active for your session. ' +
  "Nucleus's specialty stores have no policy-aware access path, so they fail closed " +
  'while any RLS policy is enabled — there is no CREATE POLICY surface for them. ' +
  'Connect as a superuser (RLS does not engage for superuser sessions) or disable RLS to browse this store.'

/** Map an engine error to a friendlier message when it is the known RLS denial. */
export function friendlyError(message: string | undefined | null): string {
  if (isRlsDenied(message)) return RLS_EXPLANATION
  return message ?? ''
}
