/**
 * preact/compat shim — re-exports Preact under React's API surface.
 *
 * WEB BUILDS ONLY. On mobile, `react` is real React (from React Native).
 *
 * This enables using React ecosystem libraries (react-query, react-hook-form,
 * zustand, etc.) with the 3KB Preact runtime on web instead of the 40KB
 * React runtime.
 *
 * Note: Most React libs work out of the box with preact/compat. Libraries
 * that use internal React fiber fields directly (react-devtools, some testing
 * utilities) may need additional shims.
 */

// preact/compat uses `export =` so we import the namespace and re-export individually.
// eslint-disable-next-line @typescript-eslint/no-require-imports
const compat = require('preact/compat') as typeof import('preact/compat')
export default compat
export const {
  useState,
  useReducer,
  useEffect,
  useLayoutEffect,
  useRef,
  useCallback,
  useMemo,
  useContext,
  createContext,
  forwardRef,
  memo,
  createElement,
  Component,
  Fragment,
  Children,
  cloneElement,
  createRef,
  isValidElement,
  Suspense,
  lazy,
  startTransition,
} = compat
