/**
 * Mock for react used in Jest tests.
 * Provides minimal React API stubs needed by hooks and components.
 */

// Simple ref implementation
function useRef<T>(initial: T) {
  const ref = { current: initial }
  return ref
}

function useState<T>(initial: T | (() => T)): [T, (v: T | ((prev: T) => T)) => void] {
  const value = typeof initial === 'function' ? (initial as () => T)() : initial
  const setter = jest.fn()
  return [value, setter]
}

// Effect cleanups are retained (not run) until flushed — matching React's
// ownership model closely enough for lifecycle assertions.
const effectCleanups: Array<() => void> = []
function useEffect(fn: () => void | (() => void), _deps?: unknown[]) {
  // Execute synchronously in tests
  const cleanup = fn()
  if (typeof cleanup === 'function') effectCleanups.push(cleanup)
}
// @internal test hook: run and drain pending effect cleanups (unmount).
;(useEffect as unknown as { __flushCleanups: () => void; __flushOneCleanup: () => void }).__flushCleanups = () => {
  while (effectCleanups.length) effectCleanups.pop()!()
}
;(useEffect as unknown as { __flushOneCleanup: () => void }).__flushOneCleanup = () => {
  if (effectCleanups.length) effectCleanups.pop()!()
}

function useMemo<T>(fn: () => T, _deps?: unknown[]): T {
  return fn()
}

function useCallback<T extends Function>(fn: T, _deps?: unknown[]): T {
  return fn
}

function useReducer<S, A>(_reducer: (state: S, action: A) => S, initialState: S): [S, (action: A) => void] {
  return [initialState, jest.fn()]
}

function useContext<T>(_context: { _currentValue: T }): T {
  return _context._currentValue
}

function useLayoutEffect(fn: () => void | (() => void), _deps?: unknown[]) {
  fn()
}

function useImperativeHandle(_ref: unknown, _create: () => unknown, _deps?: unknown[]) {}

function forwardRef<T, P>(render: (props: P, ref: T) => unknown) {
  return render
}

function memo<T>(component: T): T {
  return component
}

function createContext<T>(defaultValue: T) {
  return {
    Provider: ({ children }: { children: unknown }) => children,
    Consumer: ({ children }: { children: (val: T) => unknown }) => children(defaultValue),
    _currentValue: defaultValue,
  }
}

function createElement(type: unknown, props: Record<string, unknown> | null, ...children: unknown[]) {
  return { type, props: { ...(props ?? {}), children: children.length === 1 ? children[0] : children } }
}

const Fragment = 'Fragment'

// Minimal class component base: enough to `extend` (RouteLoadBoundary) and to
// keep state merges working if a test constructs one directly.
class Component<P = Record<string, unknown>, S = Record<string, unknown>> {
  props: P
  state: S
  constructor(props: P) { this.props = props; this.state = {} as S }
  setState(partial: Partial<S>, callback?: () => void): void {
    this.state = { ...this.state, ...partial }
    if (callback) callback()
  }
  render(): unknown { return null }
}

function Suspense(props: { children?: unknown; fallback?: unknown }) { return props.children }

function useSyncExternalStore<T>(subscribe: (listener: () => void) => () => void, getSnapshot: () => T): T {
  // Mock: register once so subscriptions are observable, then return the
  // current snapshot (the real hook re-renders on listener fire).
  subscribe(() => {})
  return getSnapshot()
}

function lazy(load: () => Promise<{ default: unknown }>) {
  let pending: Promise<void> | null = null
  const Lazy = function LazyRoute() {
    if (!pending) pending = load().then(() => undefined)
    return null
  }
  return Lazy
}

export {
  useRef,
  useState,
  useEffect,
  useMemo,
  useCallback,
  useReducer,
  useContext,
  useLayoutEffect,
  useImperativeHandle,
  forwardRef,
  memo,
  createContext,
  createElement,
  Fragment,
  Component,
  Suspense,
  lazy,
  useSyncExternalStore,
}

export default {
  useRef,
  useState,
  useEffect,
  useMemo,
  useCallback,
  useReducer,
  useContext,
  useLayoutEffect,
  useImperativeHandle,
  forwardRef,
  memo,
  createContext,
  createElement,
  Fragment,
  Component,
  Suspense,
  lazy,
  useSyncExternalStore,
}
