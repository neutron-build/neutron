# Content Collections

> **Terminology note:** This page documents **Neutron TypeScript**. In broader ecosystem docs, **Neutron** refers to the umbrella framework/platform across implementations.


Neutron TypeScript content collections provide typed content from `src/content/*`.

## Config

```ts
// src/content/config.ts
import { defineCollection, z } from "@neutron-build/core/content";

export const collections = {
  blog: defineCollection({
    schema: z.object({
      title: z.string(),
      date: z.coerce.date(),
      draft: z.boolean().default(false),
    }),
  }),
  authors: defineCollection({
    type: "data",
    schema: z.object({
      name: z.string(),
      bio: z.string(),
    }),
  }),
};
```

## Query

```ts
import { getCollection, getEntry } from "@neutron-build/core/content";

const posts = await getCollection("blog", (entry) => !entry.data.draft);
const post = await getEntry("blog", "hello-world");
```

Each entry includes:

- `id`, `slug`, `collection`
- `data` (validated schema output)
- `body` (raw content body)
- `html` — empty string for markdown/MDX (rendering is lazy, see below); for HTML-passthrough entries this is the authored content
- `render()` helper returning a `Content` Preact component
- `renderEntry(entry)` helper (`@neutron-build/core/content`) returning `{ html: string }` — the same lazy markup as `render()`, without the Preact round-trip

Markdown and MDX bodies render **lazily**, not at collection load: rendering an
entire collection's HTML up front (and pinning it in the content-store cache for
the whole build) is what pushed large sites past the V8 heap limit. So `html`
on a markdown/MDX entry is `""` until you ask for it. Two ways to ask:

- `entry.render()` — returns `{ Content }`, a Preact component. Use this when a
  Preact component is what you need (e.g. rendering inside a component tree).
- `renderEntry(entry)` — returns `{ html: string }`. Use this in a static
  route's loader, where the data must be serializable and a component cannot
  pass through.

Both reuse the same bounded, content-addressed render cache and neither
memoizes the result onto the entry.

MDX files are compiled with `@mdx-js/mdx` (Preact runtime).  
Markdown files are rendered to HTML.

Error diagnostics include collection + file context for schema parse and MDX compile/render failures.

## Build Output

`neutron build` generates:

- `dist/.neutron-content.json` (runtime manifest fallback)
- `src/content/.neutron-content.d.ts` (collection map typing)
