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

function useEffect(fn: () => void | (() => void), _deps?: unknown[]) {
  // Execute synchronously in tests
  fn()
}

function useMemo<T>(fn: () => T, _deps?: unknown[]): T {
  return fn()
}

function useCallback<T extends Function>(fn: T, _deps?: unknown[]): T {
  return fn
}

function useReducer<S, A>(reducer: (state: S, action: A) => S, initialState: S): [S, (action: A) => void] {
  return [initialState, jest.fn()]
}

function useContext<T>(_context: { _currentValue: T }): T {
  return _context._currentValue
}

function useLayoutEffect(fn: () => void | (() => void), _deps?: unknown[]) {
  fn()
}

function useImperativeHandle(_ref: unknown, _create: () => unknown, _deps?: unknown[]) {}

function forwardRef<T, P>(render: (props: P, ref: unknown) => unknown) {
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
}
