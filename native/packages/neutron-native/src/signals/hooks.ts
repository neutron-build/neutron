/**
 * Signal hooks for React — wraps @preact/signals-core with React hook semantics.
 *
 * On web (Preact), import useSignal/useComputed from '@preact/signals' directly.
 * On mobile (React Native), these wrappers provide equivalent behavior.
 */

import { useRef } from 'react'
import { signal, computed, type Signal, type ReadonlySignal } from '@preact/signals-core'

/**
 * Create a signal that persists across renders (like useRef but reactive).
 */
export function useSignal<T>(initialValue: T): Signal<T> {
  const ref = useRef<Signal<T> | null>(null)
  if (ref.current === null) ref.current = signal(initialValue)
  return ref.current
}

/**
 * Create a computed signal that derives from other signals.
 */
export function useComputed<T>(compute: () => T): ReadonlySignal<T> {
  const ref = useRef<ReadonlySignal<T> | null>(null)
  if (ref.current === null) ref.current = computed(compute)
  return ref.current
}
