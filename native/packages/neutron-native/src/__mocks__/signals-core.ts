/**
 * Mock for @preact/signals-core used in Jest tests.
 * Provides minimal signal/computed/effect behavior.
 */

type Listener<T> = (value: T) => void

interface MockSignal<T> {
  value: T
  peek: () => T
  subscribe: (fn: Listener<T>) => () => void
  valueOf: () => T
  toString: () => string
}

interface MockReadonlySignal<T> {
  readonly value: T
  peek: () => T
  subscribe: (fn: Listener<T>) => () => void
  valueOf: () => T
  toString: () => string
}

function signal<T>(initial: T): MockSignal<T> {
  const listeners = new Set<Listener<T>>()
  let currentValue = initial

  return {
    get value() { return currentValue },
    set value(v: T) {
      currentValue = v
      for (const fn of listeners) fn(v)
    },
    peek: () => currentValue,
    subscribe: (fn: Listener<T>) => {
      listeners.add(fn)
      // Call immediately like real signals-core
      fn(currentValue)
      return () => { listeners.delete(fn) }
    },
    valueOf: () => currentValue,
    toString: () => String(currentValue),
  }
}

function computed<T>(fn: () => T): MockReadonlySignal<T> {
  // Simple eager evaluation -- re-evaluates on access
  return {
    get value() { return fn() },
    peek: () => fn(),
    subscribe: (listener: Listener<T>) => {
      listener(fn())
      return () => {}
    },
    valueOf: () => fn(),
    toString: () => String(fn()),
  }
}

function effect(fn: () => void | (() => void)): () => void {
  const cleanup = fn()
  return () => {
    if (typeof cleanup === 'function') cleanup()
  }
}

function batch(fn: () => void): void {
  fn()
}

function untracked<T>(fn: () => T): T {
  return fn()
}

export { signal, computed, effect, batch, untracked }
export type { MockSignal as Signal, MockReadonlySignal as ReadonlySignal }
