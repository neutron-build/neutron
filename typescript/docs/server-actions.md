# Server Actions

> **Terminology note:** This page documents **Neutron TypeScript**.

A server action is an async function that lives on the server and that client
code calls like any other async function. Mark a module with `"use server"` as
its first statement, import it from a component, and call it:

```ts
// src/todos.actions.ts
"use server";
import { getActionContext, forbidden, redirect } from "@neutron-build/core";
import { db } from "./db.server";

export async function addTodo(title: string) {
  const { context, responseHeaders } = getActionContext();
  if (!context.user) throw forbidden("Sign in first.");
  const todo = await db.todos.insert({ title, owner: context.user.id });
  responseHeaders.set("x-neutron-invalidate", "/todos");
  return todo; // { id, title, createdAt: Date } arrives as such
}

export async function deleteAll() {
  await db.todos.clear();
  throw redirect("/todos?cleared=1");
}
```

```tsx
// src/routes/todos.tsx (an island or an app route)
import { addTodo } from "../todos.actions";

export default function NewTodo() {
  return (
    <form
      onSubmit={async (event) => {
        event.preventDefault();
        const data = new FormData(event.currentTarget);
        await addTodo(String(data.get("title")));
      }}
    >
      <input name="title" />
      <button>Add</button>
    </form>
  );
}
```

## How it works

- **Opt-in, per module.** Only a module whose directive prologue holds
  `"use server"` is an action module. In the client graph it is replaced by
  one stub per export; the module itself, and everything it imports (database
  clients, secrets), never reaches the browser.
- **Stable ids.** Each export is invoked by an id: a hash of the module path
  (relative to the app root) and the export name, fixed at build time. The
  same source builds the same ids, so a tab open across a deploy keeps working
  while the action still exists.
- **Only what the client reaches is callable.** `neutron-ts build` registers
  the action modules its client passes (the app bundle and the islands
  bundle) reached and writes them to `dist/.neutron-server-actions.json`; the
  server serves exactly those. `neutron-ts dev` serves the modules Vite has
  transformed for the browser. Any other id answers `404`. A `"use server"`
  module that only server code imports is never exposed.
- **Server-side imports are plain calls.** A loader, resource route or other
  server module importing the same file gets the real functions.

## Rules the build enforces

Every export of a `"use server"` module must be an async function the build
can see is one:

```ts
export async function a() {}            // ok
export const b = async () => {};        // ok
export default async function () {}     // ok
export { c, d as e };                   // ok when c and d are async functions declared here
export type Input = { title: string };  // types are fine
```

Each of these is a build error naming the file, line and export:
non-async functions, values (`export const LIMIT = 10`), `let`/`var`
exports, classes, async generators, the result of a call
(`export const save = withAuth(async () => {})` — wrap inside the function
instead), re-exports (`export { x } from "./y"`, `export *`, re-exported
imports).

**Inline `"use server"` is not supported.** A `"use server"` directive inside
a function body (an action declared in a component or a closure) is a build
error, and so is a `"use server"` string that is not the module's first
statement. Move the function into an action module and import it.

## The request

A call is `POST /_neutron/action` with the id in the `X-Neutron-Action`
header. On the server, in order:

1. **Method**: anything but `POST` is `405`.
2. **Same-origin check** (always on): a browser's `Sec-Fetch-Site` must be
   `same-origin`; without it, an `Origin` must name the request's own host.
   A request carrying neither is not a browser's and is not a forgery vector.
   Rejected requests answer `403` before any app code runs. The custom header
   also makes a cross-origin call non-simple, so a browser preflights it.
   `server.serverActions.allowedOrigins` admits extra origins (exact match) —
   for a proxy that rewrites `Host`.
3. **Body cap**: `server.serverActions.bodySizeLimit`, default 1 MiB, `413`
   beyond it (declared length or streamed bytes).
4. **Global middleware** (`src/middleware.ts`) runs around the action exactly
   as around a page: it can deny (`401`, a redirect, a thrown `Response`) and
   it fills the `context` the action reads. An unknown id answers `404` from
   inside the chain, so an auth gate answers first.
5. **CSRF tokens**: if your global middleware includes `csrfMiddleware`, the
   client sends the token from `<meta name="csrf-token" content="…">` in the
   `x-csrf-token` header. Render that tag from your root layout's `head()`
   with `context.csrfToken`.

The endpoint path is the same for every action, so a middleware that gates by
path does not see the page the call came from; read
`getActionContext().action` inside the action, or the `X-Neutron-Action`
header in middleware, to gate per action.

## Arguments and results

Both use the loader-data serializer (devalue): plain objects and arrays,
primitives, `undefined`, `Date`, `Map`, `Set`, `BigInt`, `RegExp`, `URL`,
repeated and circular references. `FormData` and `Blob`/`File` arguments are
sent as multipart, anywhere in the argument list. A value that cannot be
serialized (a function, a class instance) rejects on the client before
anything is sent; a return value that cannot be serialized is a server error
(below), reported with the action's name.

## Errors

- A thrown `ProblemError` answers with its status and RFC 7807 body; the
  client rejects with a `ServerActionError` carrying `status` and `problem`.
- A thrown or returned `Response` answers as itself. A non-redirect `2xx`
  resolves the call with that `Response` (a file download, a stream); any
  other status rejects with a `ServerActionError`.
- Anything else is a generic `500` problem that never echoes the message. It
  is reported through `hooks.onError` with `source: "action"` (and printed,
  with the dev overlay, in `neutron-ts dev`).

## Redirects and invalidation

A redirect an action returns or throws (`throw redirect("/x")`) is sent with
the data protocol's signalling (`204` + `X-Neutron-Redirect`): the client
navigates — through the router when the page runs it, as a document
navigation otherwise — and the call resolves `undefined`.

`getActionContext().responseHeaders` is merged into the action's response.
Setting `x-neutron-invalidate` (`/todos`, a comma list, `self` for the page
that called, `*` for everything) evicts those paths from the server's app and
loader caches, and if the page on screen is among them the client router
revalidates it in place. Append `Set-Cookie` there to set cookies.

## `<form action={fn}>`

Under `runtime: "react-compat"`, `<form action={serverAction}>` works on the
client as in React 19: submitting calls the action with the form's
`FormData` (the submitter's name/value included) instead of navigating, and
resets the form when it settles successfully. Not supported: submitting
before hydration (the server-rendered form carries no action, so without
JavaScript it submits to the current URL), `formAction` on buttons,
`useFormStatus` and `useActionState`. Under `runtime: "preact"`, call the
action from an `onSubmit` handler as in the example above.

## Deployment

| Target | Server actions |
| --- | --- |
| `neutron-ts dev` | Served (modules Vite has transformed for the client) |
| `neutron-ts start` | Served, from the server bundle or the Vite runtime fallback |
| `--preset docker` | Served by the image's runtime |
| `--preset vercel` / `netlify` / `cloudflare` | Served by the generated runtime (apps with at least one app route) |
| `--preset static` | Build error: there is no server |

An adapter whose output cannot serve the registered actions fails the build
instead of shipping stubs that would answer `404`. On Cloudflare the action
context uses `AsyncLocalStorage`; the adapter enables `nodejs_compat` when the
bundle needs it.

## Configuration

```ts
// neutron.config.ts
export default defineConfig({
  server: {
    serverActions: {
      bodySizeLimit: 10 * 1024 * 1024, // uploads up to 10 MiB
      allowedOrigins: ["https://app.example.com"],
    },
  },
});
```
