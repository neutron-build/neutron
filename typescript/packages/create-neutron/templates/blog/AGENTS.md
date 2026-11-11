# Working in a Neutron app (AGENTS.md)

Rules for AI coding assistants (and humans) editing this project. Neutron is a
new framework and is **not** in model training data — do not assume React/Vite
SPA conventions. Follow these exactly.

## The one rule that breaks everything if ignored

**This is NOT a `render(<App/>)` SPA.** There is no `App.tsx`, no `index.css`
import in the entry, no `ReactDOM.createRoot`. Pages are files in `src/routes/`.

- **NEVER** create or import `./App`, `App.tsx`, or `./index.css` from `main.tsx`.
- **NEVER** rewrite `src/main.tsx`. It is fixed framework glue:
  ```tsx
  import { init, registerRoutes } from "@neutron-build/core/client";
  import { routes } from "virtual:neutron/routes";
  registerRoutes(routes);
  void init();
  ```
  `virtual:neutron/routes` is a build-time virtual module — it is real, do not
  "fix" it, do not replace it with manual imports.
- **One JSX import source per scaffold.** `tsconfig.json` and the Vite
  transform both use `jsxImportSource: "__JSX_IMPORT_SOURCE__"` (derived from
  the `runtime` in neutron.config.ts, the same rule the CLI applies). This
  scaffold imports hooks from `"__HOOKS_IMPORT__"`. Do **not** mix import
  sources within one module.

## Project structure

- `src/routes/` — file-based routing. A file's path IS its URL.
  - `index.tsx` → `/`
  - `about.tsx` → `/about`
  - `users/[id].tsx` → `/users/:id` (dynamic segment in brackets)
  - `_layout.tsx` → shared layout wrapping its subtree (nested `_layout.tsx` nest)
  - `api/foo.tsx` → API route (return `Response`, default export returns `null`)
- `src/main.tsx` — client entry (do not edit; see above).
- `neutron.config.ts` — framework config.
- Add a page = add a file under `src/routes/`. That's the whole mental model.

## Anatomy of a route file

```tsx
import type { LoaderArgs } from "@neutron-build/core";

// Per-route mode. "static" = prerendered at build (content that doesn't change
// per request). "app" = server-rendered per request (dynamic/data-driven).
export const config = { mode: "app", cache: { maxAge: 30 } };

// Runs on the server; its return value arrives as `props.data`.
export async function loader({ params, request }: LoaderArgs) {
  return { user: { id: params.id, name: `User ${params.id}` } };
}

// Default export = the page component (Preact). Reads loader data from props.
export default function UserRoute(props: { data?: { user: { id: string; name: string } } }) {
  return <section><h2>{props.data?.user.name}</h2></section>;
}
```

## Data writes (forms / actions)

```tsx
import { Form } from "@neutron-build/core/client";
import type { ActionArgs } from "@neutron-build/core";

export const config = { mode: "app" };
export async function loader() { return { currentName: "Acme" }; }

// Handles POST. Return a plain object: it arrives as `props.actionData` and
// the page re-renders. A returned Response (Response.json, redirect(), ...) is
// sent as the HTTP response itself, and the page is not rendered.
export async function action({ request }: ActionArgs) {
  const fd = await request.formData();
  return { ok: true, name: String(fd.get("name") || "") };
}

export default function Settings(props: { data?: { currentName: string }; actionData?: { ok: boolean; name: string } }) {
  return (
    <Form method="post">
      <input name="name" defaultValue={props.data?.currentName} />
      <button type="submit">Save</button>
      {props.actionData?.ok ? <p>Saved {props.actionData.name}</p> : null}
    </Form>
  );
}
```

Rule: **plain object → `props.actionData`; a `Response` → sent as the response.**
Return a Response only to redirect or to answer a non-page request (see API
routes below).

## API routes

```tsx
import type { ActionArgs } from "@neutron-build/core";
export const config = { mode: "app" };
export async function action({ request }: ActionArgs) {
  return Response.json({ ok: true });
}
export default function ApiRoute() { return null; }
```

## Layout & document head

`_layout.tsx` default-exports a component rendering `props.children`. Export a
`head()` to set `<link>`/`<meta>`/title:
```tsx
export function head() {
  return { link: { rel: "icon", type: "image/svg+xml", href: "/favicon.svg" } };
}
```

## Navigation, client state, imports

- Navigate with plain `<a href="...">`, or `Link` / `useNavigate` from
  `@neutron-build/core/client`. Read params with `useParams`, loader data with
  `useLoaderData`.
- Reactive state: `signal` / `computed` / `effect` from `@neutron-build/core`.
- Interactive client components: `Island` from `@neutron-build/core`.
- Do **not** add `react`, `react-dom`, `next`, or a third-party router —
  routing and rendering are built in.

## Styling

Inline styles or plain CSS, as in the existing routes. If the user wants
Tailwind, add it via Vite — don't assume it's present.

## The CMS

This site uses `@neutron-build/cms`. Its full guide:
https://neutron.build/docs/content/cms

- `src/cms.ts` declares what can be edited (`defineCms`). Posts are on; add
  `lists` or `texts` there.
- The admin at `/n-admin` is `src/routes/n-admin/index.ts` and
  `src/routes/n-admin/[...path].ts`, each exporting `{ GET, POST }` from the
  CMS. Keep both files as they are and add no other routes under `n-admin/`.
- Read content in loaders: `cms.posts()`, `cms.post(slug)`, `cms.list(key)`,
  `cms.text(key)`. They are synchronous. Post bodies are Markdown; render them
  with `renderMarkdown` from `@neutron-build/cms`.
- Pages that show CMS content use `{ mode: "app", hydrate: false }`. Never
  `mode: "static"`: the content changes after the build.
- The site runs as a server (`npm run build`, then `npm start`). Do not switch
  it to the `static` or `docker` preset.
- Content lives in `data/` in development and in `CMS_DATA_DIR` in
  production. Never commit it, or `.env`.

## Before you finish

- Every relative import must resolve to a file you actually created. If a route
  imports `./Foo`, create `Foo.tsx`. (The #1 generation failure is importing a
  file that was never written.)
- Mirror the patterns in the existing `src/routes/` files exactly.

Docs: https://neutron.build/docs
