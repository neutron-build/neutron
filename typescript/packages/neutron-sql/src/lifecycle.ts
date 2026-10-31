/** Small driver-independent ownership, cancellation and error contract. */
import { ConnectionFailedError } from './errors.js';

export type CloseResource = () => unknown | Promise<unknown>;
export interface ConnectionCapabilities {
  readonly cancellation: 'server-attempt' | 'response-only' | 'pre-dispatch' | 'unsupported' | 'unknown';
  readonly mutationOutcome: 'unknown-error' | 'begin-unknown' | 'driver-error';
  readonly automaticMutationReplay: false;
}
/** Missing custom metadata is unknown; require only the guarantees you use. */
export function admitConnection<T extends { capabilities?: Readonly<ConnectionCapabilities> }>(
  connection: T, required: Partial<ConnectionCapabilities>,
): T {
  for (const [key, value] of Object.entries(required)) {
    if (!connection.capabilities || connection.capabilities[key as keyof ConnectionCapabilities] !== value)
      throw new Error(`Connection capability not admitted: ${key}`);
  }
  return connection;
}
export async function closeResources(resources: readonly CloseResource[]): Promise<unknown[]> {
  const settled = await Promise.allSettled(resources.map(close => Promise.resolve().then(close)));
  return settled.flatMap(result => result.status === 'rejected' ? [result.reason] : []);
}
export async function cleanupAfterFailure(error: unknown, resources: readonly CloseResource[]): Promise<never> {
  const failures = await closeResources(resources);
  if (failures.length) throw new AggregateError([error, ...failures], 'Startup and cleanup failed', { cause: error });
  throw error;
}
export function ownedClose(resources: readonly CloseResource[]): () => Promise<void> {
  let closing: Promise<void> | undefined;
  return () => closing ??= closeResources(resources).then(failures => {
    if (failures.length) throw new AggregateError(failures, 'Resource cleanup failed', { cause: failures[0] });
  });
}
/** Owned termination is terminal even on failure; never retry ambiguous disposal.
 * Borrowed termination leaves the resource usable and under its owner's control. */
export function resourceLifecycle(ownership: 'owned' | 'borrowed', close: CloseResource) {
  let terminated = false;
  let closing: Promise<void> | undefined;
  return {
    ownership,
    get terminated() { return terminated; },
    get closing() { return closing !== undefined; },
    assertOpen() { if (closing) throw new ConnectionFailedError('Resource is closed'); },
    terminate(): Promise<void> {
      if (ownership === 'borrowed') return Promise.resolve();
      return closing ??= Promise.resolve().then(close).then(() => {}).finally(() => { terminated = true; });
    },
  };
}
