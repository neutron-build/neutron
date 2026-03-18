import { h, ComponentChildren } from "preact";
import { useState, useEffect } from "preact/hooks";

export interface AwaitProps<T = unknown> {
  resolve: Promise<T> | T;
  errorElement?: ComponentChildren;
  children: (data: T) => ComponentChildren;
}

interface AwaitState<T> {
  status: "pending" | "resolved" | "rejected";
  data?: T;
  error?: Error;
}

export function Await<T = unknown>({ resolve, errorElement, children }: AwaitProps<T>): any {
  const [state, setState] = useState<AwaitState<T>>(() => {
    // Check if it's already resolved (not a promise)
    if (!(resolve instanceof Promise)) {
      return { status: "resolved", data: resolve as T };
    }
    return { status: "pending" };
  });

  useEffect(() => {
    // If not a promise, nothing to do
    if (!(resolve instanceof Promise)) {
      return;
    }

    let cancelled = false;

    resolve.then(
      (data) => {
        if (!cancelled) {
          setState({ status: "resolved", data });
        }
      },
      (error) => {
        if (!cancelled) {
          setState({ status: "rejected", error });
        }
      }
    );

    return () => {
      cancelled = true;
    };
  }, [resolve]);

  if (state.status === "pending") {
    // Throw promise for Suspense to catch
    if (resolve instanceof Promise) {
      throw resolve;
    }
  }

  if (state.status === "rejected") {
    if (errorElement) {
      return errorElement;
    }
    throw state.error;
  }

  if (state.status === "resolved") {
    return children(state.data!);
  }

  return null;
}
