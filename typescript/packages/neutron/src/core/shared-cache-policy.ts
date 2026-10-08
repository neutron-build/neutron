/** Keyed by BOTH loader and response caches. Unknown Vary dimensions bypass. */
export function sharedResponseMaxAge(requestPolicy: string | null, response: Response, maxAge: number): number | null {
  if (!Number.isFinite(maxAge) || maxAge <= 0 || response.status !== 200 || response.headers.has('set-cookie') || response.headers.has('access-control-allow-origin')) return null;
  const parse = (value: string | null) => new Map((value ?? '').split(',').filter(Boolean).map(part => {
    const [name, ...value] = part.trim().split('=');
    return [name.trim().toLowerCase(), value.join('=').replace(/^"|"$/g, '')];
  }));
  const request = parse(requestPolicy), policy = parse(response.headers.get('cache-control'));
  if (['no-cache', 'no-store'].some(name => request.has(name)) || ['private', 'no-cache', 'no-store'].some(name => policy.has(name))) return null;
  const keyed = new Set(['accept', 'accept-language', 'x-neutron-data', 'x-neutron-routes']);
  if ((response.headers.get('vary') ?? '').split(',').map(field => field.trim().toLowerCase()).filter(Boolean).some(field => !keyed.has(field))) return null;
  const declared = policy.get('s-maxage') ?? policy.get('max-age');
  if (declared !== undefined) {
    if (!/^\d+$/.test(declared) || !Number.isSafeInteger(Number(declared)) || Number(declared) <= 0) return null;
    return Math.min(maxAge, Number(declared));
  }
  return maxAge;
}
