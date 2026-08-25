/**
 * Signal utilities for optimized reactive state
 * Re-exports and enhances @preact/signals with SolidJS-inspired utilities
 */

import { signal, computed, effect, batch, untracked, Signal } from '@preact/signals-core';

// Re-export core signal primitives
export { signal, computed, effect, batch, Signal };

/**
 * Read a signal's value without subscribing to it
 * Useful when you need the current value but don't want reactivity
 *
 * Delegates to signals-core's `untracked`: wrapping in `batch()` alone would
 * still record dependencies, so effects would re-run on every untracked read.
 *
 * @example
 * ```typescript
 * const count = signal(0);
 *
 * effect(() => {
 *   const current = untrack(() => count.value);
 *   console.log('Current value:', current);
 *   // This effect won't re-run when count changes
 * });
 * ```
 */
export function untrack<T>(fn: () => T): T {
  return untracked(fn);
}

/**
 * Create a readonly computed signal
 * Same as computed() but with clearer intent
 *
 * @example
 * ```typescript
 * const count = signal(0);
 * const doubled = createMemo(() => count.value * 2);
 * ```
 */
export function createMemo<T>(fn: () => T): Signal<T> {
  return computed(fn);
}

interface RootScope {
  disposes: Array<() => void>;
}

// Innermost-first stack of active createRoot scopes.
const rootStack: RootScope[] = [];

/**
 * Create an effect that runs immediately
 *
 * When called inside `createRoot`, the effect's dispose function is
 * registered with that root so the root can dispose it later.
 *
 * @example
 * ```typescript
 * createEffect(() => {
 *   console.log('Count:', count.value);
 * });
 * ```
 */
export function createEffect(fn: () => void | (() => void)): () => void {
  const dispose = effect(fn);
  const scope = rootStack[rootStack.length - 1];
  if (scope) {
    scope.disposes.push(dispose);
  }
  return dispose;
}

/**
 * Create a root scope for signal tracking
 * Useful for cleaning up effects when unmounting
 *
 * Effects created via `createEffect` while `fn` runs are disposed together
 * when the returned disposer is called.
 *
 * @example
 * ```typescript
 * const dispose = createRoot(() => {
 *   createEffect(() => {
 *     console.log(count.value);
 *   });
 * });
 *
 * // Later: clean up all effects
 * dispose();
 * ```
 */
export function createRoot(fn: () => void): () => void {
  const scope: RootScope = { disposes: [] };
  rootStack.push(scope);
  try {
    fn();
  } finally {
    rootStack.pop();
  }
  return () => {
    for (const dispose of scope.disposes) {
      dispose();
    }
    scope.disposes.length = 0;
  };
}

/**
 * Helper type for signal value extraction
 */
export type SignalValue<T> = T extends Signal<infer U> ? U : never;
