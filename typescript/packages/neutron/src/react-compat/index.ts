/**
 * The `react` that `runtime: "react-compat"` resolves to: preact/compat, plus
 * the React 19 APIs preact/compat does not ship yet.
 *
 * Only the bare `react` specifier comes here (vitePreactAliases); `react/*`
 * subpaths and `react-dom` keep resolving through preact/compat itself.
 */
import compat, { useContext } from "preact/compat";
import { options, type Context, type VNode } from "preact";

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

/**
 * React 19 form actions: `<form action={fn}>` calls `fn(formData)` on submit
 * instead of navigating, and resets the form once `fn` has settled
 * successfully. preact/compat would set the function as the DOM `action`
 * attribute (a submit to a garbage URL). Here a `form` vnode whose `action`
 * is a function gets a submit handler in its place — an `onSubmit` of the
 * element's own runs first and can `preventDefault()` to cancel. A server
 * action (`"use server"` import) is such a function. What React's version
 * adds and this does not: submission before hydration (the server-rendered
 * form carries no action), `formAction` on buttons, `useFormStatus`, and
 * `useActionState`.
 */
const FORM_ACTIONS_INSTALLED = Symbol.for("@neutron-build/core/react-compat/form-actions");
type HookedOptions = typeof options & { [FORM_ACTIONS_INSTALLED]?: true };
if (!(options as HookedOptions)[FORM_ACTIONS_INSTALLED]) {
  (options as HookedOptions)[FORM_ACTIONS_INSTALLED] = true;
  const previous = options.vnode;
  options.vnode = (vnode: VNode) => {
    const props = vnode.props as Record<string, unknown> | null;
    if (vnode.type === "form" && props && typeof props.action === "function") {
      const { action, onSubmit, ...rest } = props as {
        action: (data: FormData) => unknown;
        onSubmit?: (event: SubmitEvent) => void;
      } & Record<string, unknown>;
      (vnode as { props: unknown }).props = {
        ...rest,
        onSubmit: (event: SubmitEvent) => {
          onSubmit?.(event);
          if (event.defaultPrevented) return;
          event.preventDefault();
          const form = event.currentTarget as HTMLFormElement;
          let data: FormData;
          try {
            data = new FormData(form, event.submitter ?? undefined);
          } catch {
            data = new FormData(form);
          }
          void Promise.resolve(action(data)).then(() => form.reset());
        },
      };
    }
    previous?.(vnode);
  };
}

const React = { ...compat, use };
export default React;
