import { AsyncLocalStorage } from "node:async_hooks";
import type { AppContext } from "./types.js";

/**
 * What a server action can reach of the request that invoked it: the request
 * itself (cookies, headers), the context global middleware filled in (the
 * session an auth gate attached), and the headers its response will carry
 * (`Set-Cookie`, `x-neutron-invalidate`).
 */
export interface ServerActionContext {
  request: Request;
  context: AppContext;
  /** Merged into the action's response (`append` for repeatable headers such as `Set-Cookie`). */
  responseHeaders: Headers;
  /** The action's stable id and the export it names. */
  action: { id: string; name: string };
}

// One storage per process, whichever copy of core an action module imports:
// the start server bundle and the dev SSR graph each evaluate their own copy
// of this file next to the server's native one.
const storageKey = Symbol.for("@neutron-build/core/server-action/storage");
const realm = globalThis as unknown as Record<symbol, AsyncLocalStorage<ServerActionContext> | undefined>;

function storage(): AsyncLocalStorage<ServerActionContext> {
  return (realm[storageKey] ??= new AsyncLocalStorage<ServerActionContext>());
}

export function runInServerActionContext<T>(scope: ServerActionContext, callback: () => T): T {
  return storage().run(scope, callback);
}

/**
 * The request scope of the server action that is running. Throws when called
 * outside one — a plain server-side call of the same function has no action
 * request to read.
 */
export function getActionContext(): ServerActionContext {
  const scope = storage().getStore();
  if (!scope) {
    throw new Error(
      "getActionContext() was called outside a server action request. It is only available " +
        "while a \"use server\" export runs because the client called it."
    );
  }
  return scope;
}
