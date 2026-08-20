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
  'while any RLS policy is enabled. No CREATE POLICY can open them — policies only cover ' +
  'SQL tables. The missing grant is the superuser role (RLS never engages for superuser ' +
  'sessions). A security admin can also disable RLS on the protected table ' +
  '(ALTER TABLE <table> DISABLE ROW LEVEL SECURITY) or remove the policy outright ' +
  '(DROP POLICY <name> ON <table>). Run SELECT * FROM pg_policies to find the table and ' +
  'policy names — the view is populated from the live RLS engine.'

// The exact statements that restore specialty-store access, as rendered in
// the RlsNotice panel. Real engine DDL: ALTER TABLE ... DISABLE ROW LEVEL
// SECURITY (executor/ddl.rs) and DROP POLICY (executor/policy.rs). The first
// statement fills in the placeholders in the other two: pg_policies is served
// from the live RLS engine, so it names every active policy and its table.
export const RLS_FIX_SQL = [
  'SELECT * FROM pg_policies;',
  'ALTER TABLE <table> DISABLE ROW LEVEL SECURITY;',
  'DROP POLICY <name> ON <table>;',
]

/** Map an engine error to a friendlier message when it is the known RLS denial. */
export function friendlyError(message: string | undefined | null): string {
  if (isRlsDenied(message)) return RLS_EXPLANATION
  return message ?? ''
}
