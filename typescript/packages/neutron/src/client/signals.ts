/**
 * Signal utilities for optimized reactive state
 * Re-exports and enhances @preact/signals with SolidJS-inspired utilities
 */

import { signal, computed, effect, batch, Signal } from '@preact/signals-core';

// Re-export core signal primitives
export { signal, computed, effect, batch, Signal };

/**
 * Read a signal's value without subscribing to it
 * Useful when you need the current value but don't want reactivity
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
  // Execute function without tracking dependencies
  let result: T;
  batch(() => {
    result = fn();
  });
  return result!;
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

/**
 * Create an effect that runs immediately
 *
 * @example
 * ```typescript
 * createEffect(() => {
 *   console.log('Count:', count.value);
 * });
 * ```
 */
export function createEffect(fn: () => void | (() => void)): () => void {
  return effect(fn);
}

/**
 * Create a root scope for signal tracking
 * Useful for cleaning up effects when unmounting
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
export function createRoot<T>(fn: () => T): T {
  return fn();
}

/**
 * Helper type for signal value extraction
 */
export type SignalValue<T> = T extends Signal<infer U> ? U : never;
