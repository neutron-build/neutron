# Content Collections

> **Terminology note:** This page documents **Neutron TypeScript**. In broader ecosystem docs, **Neutron** refers to the umbrella framework/platform across implementations.


Neutron TypeScript content collections provide typed content from `src/content/*`.

## Config

```ts
// src/content/config.ts
import { defineCollection, z } from "neutron/content";

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
import { getCollection, getEntry } from "neutron/content";

const posts = await getCollection("blog", (entry) => !entry.data.draft);
const post = await getEntry("blog", "hello-world");
```

Each entry includes:

- `id`, `slug`, `collection`
- `data` (validated schema output)
- `body` (raw content body)
- `html` (rendered HTML for markdown/MDX files)
- `render()` helper returning a `Content` component

MDX files are compiled with `@mdx-js/mdx` (Preact runtime).  
Markdown files are rendered to HTML.

Error diagnostics include collection + file context for schema parse and MDX compile/render failures.

## Build Output

`neutron build` generates:

- `dist/.neutron-content.json` (runtime manifest fallback)
- `src/content/.neutron-content.d.ts` (collection map typing)
