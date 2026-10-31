/** Stores decode exactly once; callers already hold a decoded canonical path. */
export function encodeCacheInvalidationPath(pathname: string): string {
  return pathname.split('/').map(segment => encodeURIComponent(segment)).join('/');
}

interface MutationCacheStore { deleteByPath(pathname: string): Promise<void> }

/** Shared by source and generated handlers: invalidate before action and in finally. */
export async function beginCacheMutation(
  mutation: boolean,
  pathname: string,
  stores: MutationCacheStore[]
): Promise<() => Promise<void>> {
  if (!mutation) return async () => {};
  const encoded = encodeCacheInvalidationPath(pathname);
  const invalidate = async () => {
    // Try every store even when one remote store is unavailable.
    const results = await Promise.allSettled(stores.map(store => Promise.resolve().then(() => store.deleteByPath(encoded))));
    for (const result of results) if (result.status === 'rejected') throw result.reason;
  };
  await invalidate();
  let completion: Promise<void> | undefined;
  return () => completion ??= invalidate();
}
