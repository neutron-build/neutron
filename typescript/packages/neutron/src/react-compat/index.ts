/**
 * The `react` that `runtime: "react-compat"` resolves to: preact/compat, plus
 * the React 19 APIs preact/compat does not ship yet.
 *
 * Only the bare `react` specifier comes here (vitePreactAliases); `react/*`
 * subpaths and `react-dom` keep resolving through preact/compat itself.
 */
import compat, { useContext } from "preact/compat";
import type { Context } from "preact";

// preact/compat's types are `export =`, which `export *` cannot re-export;
// the runtime re-export is what matters (apps type against @types/react).
// @ts-ignore TS2498
export * from "preact/compat";

/** React's thenable protocol: React records a promise's outcome on the promise itself. */
type TrackedThenable<T> = PromiseLike<T> & {
  status?: "pending" | "fulfilled" | "rejected";
  value?: T;
  reason?: unknown;
};

export type Usable<T> = PromiseLike<T> | Context<T>;

/**
 * React 19 `use()`: read a promise's value or a context.
 *
 * A promise is tracked under React's protocol (`status`/`value`/`reason` on
 * the promise object). A settled one returns its value or throws its reason;
 * a pending one suspends — it is thrown, so the nearest `<Suspense>` shows its
 * fallback and renders again once it settles, on the client and in streaming
 * SSR alike. As in React, the promise must be the same object across those
 * renders (create it outside the component, in a loader, or cache it): a new
 * promise per render never settles from `use()`'s point of view.
 *
 * A context is read with `useContext`. Unlike React's, that is a hook call, so
 * a context read with `use()` must not be conditional.
 */
export function use<T>(usable: Usable<T>): T {
  if (usable !== null && (typeof usable === "object" || typeof usable === "function")) {
    if (typeof (usable as PromiseLike<T>).then === "function") {
      return useThenable(usable as TrackedThenable<T>);
    }
    if ("Provider" in usable && "Consumer" in usable) {
      return useContext(usable as Context<T>);
    }
  }
  throw new Error(`An unsupported type was passed to use(): ${String(usable)}`);
}

function useThenable<T>(thenable: TrackedThenable<T>): T {
  switch (thenable.status) {
    case "fulfilled":
      return thenable.value as T;
    case "rejected":
      throw thenable.reason;
    case "pending":
      throw thenable;
    default:
      thenable.status = "pending";
      thenable.then(
        (value) => {
          if (thenable.status === "pending") {
            thenable.status = "fulfilled";
            thenable.value = value;
          }
        },
        (reason) => {
          if (thenable.status === "pending") {
            thenable.status = "rejected";
            thenable.reason = reason;
          }
        }
      );
      throw thenable;
  }
}

const React = { ...compat, use };
export default React;
