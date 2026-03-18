# Neutron

The unified TypeScript web framework.
This repository/workspace is the **Neutron TypeScript** implementation within the broader Neutron multi-language system.

Static sites with zero JavaScript. App routes with Preact SSR. One router. Deploy anywhere.

---

## Why Neutron

Every web framework forces a trade-off. Astro is great for content but struggles with interactive apps. Remix nails data flow but ships JavaScript to every page. Next.js does everything but drowns you in rendering modes, caching layers, and complexity.

Neutron combines the best of all three into one framework:

- **From Astro** -- Zero-JS static rendering, islands architecture, content collections
- **From Remix** -- Loaders, actions, `<Form>`, nested layouts, auto-revalidation
- **From Next.js** -- File-based routing conventions, image optimization, middleware

Then it gets out of your way.

## Key Features

**Two modes, one router.** Static routes ship pure HTML with zero JavaScript. App routes get full Preact SSR with loaders, actions, and client-side navigation. Both coexist in one project.

**3KB runtime.** App routes use Preact (not React). Static routes ship nothing.

**Web standards.** Request, Response, Headers, FormData. No proprietary APIs.

**Non-opinionated.** Neutron doesn't choose your database, auth, CSS, or deployment target. You do.

**Deploy anywhere.** Built-in adapters for Node.js, Cloudflare Workers, Vercel, Docker, and static hosting.

## Quick Start

```bash
npm create neutron@latest
```

```bash
cd my-app
npm run dev
```

## How It Works

### Static Route (zero JS, like Astro)

```tsx
// src/routes/about.tsx
export const config = { mode: "static" };

export default function About() {
  return (
    <div>
      <h1>About Us</h1>
      <p>This ships as pure HTML. No JavaScript.</p>
    </div>
  );
}
```

### App Route (SSR + interactivity, like Remix)

```tsx
// src/routes/app/projects/[id].tsx
export const config = { mode: "app" };

export async function loader({ params }) {
  const project = await getProject(params.id);
  if (!project) throw new Response("Not found", { status: 404 });
  return { project };
}

export async function action({ request }) {
  const form = await request.formData();
  await updateProject(form.get("id"), { name: form.get("name") });
  return { success: true };
}

export default function ProjectPage() {
  const { project } = useLoaderData();
  return (
    <Form method="post">
      <input name="name" defaultValue={project.name} />
      <button type="submit">Save</button>
    </Form>
  );
}
```

### Islands (opt-in interactivity on static pages)

```tsx
// src/routes/index.tsx
export const config = { mode: "static" };

import { Counter } from "../components/Counter";

export default function Home() {
  return (
    <div>
      <h1>Welcome</h1>
      <p>This is static HTML.</p>
      <Counter client:load />  {/* Only this hydrates */}
    </div>
  );
}
```

## Performance

Benchmarked against Next.js, Remix 2, Remix 3 (React Router 7), and Astro on Node.js with 80 concurrent connections using [autocannon](https://github.com/mcollina/autocannon). All frameworks running production builds.

| Scenario | Neutron | Next.js | Remix 2 | Remix 3 | Astro |
|---|---|---|---|---|---|
| Static `GET /` | **8,262 req/s** | 2,756 | 690 | 392 | 1,206 |
| Dynamic `GET /users/1` | **7,384 req/s** | 355 | 612 | 355 | 1,123 |
| Compute `GET /compute` | **707 req/s** | 256 | 342 | 256 | 425 |
| Big payload `GET /big` | **1,854 req/s** | 240 | 331 | 76 | 274 |
| Login page `GET /login` | **7,784 req/s** | 2,499 | 613 | 349 | 442 |
| Auth protected `GET /protected` | **1,102 req/s** | 187 | 284 | 172 | 165 |
| Mutation `POST /api/mutate` | **776 req/s** | 293 | 314 | 270 | 838 |

Full benchmark suite with conformance matrix, publish-grade CI, and regression gates in [`benchmarks/`](./benchmarks/).

## Streaming SSR

App routes stream HTML as data resolves. The head — including critical CSS and above-the-fold content — is flushed immediately. Slow data fetches are wrapped in `<Suspense>` boundaries and streamed later. Time to First Byte is decoupled from data latency.

```tsx
// src/routes/app/products/[id].tsx
export async function loader({ params }) {
  const product = await getProduct(params.id); // fast, critical data
  const reviews = getReviews(params.id);       // slow, deferred — Promise not awaited
  return { product, reviews };
}

export default function ProductPage() {
  const { product, reviews } = useLoaderData();
  return (
    <>
      <ProductHeader product={product} />  {/* rendered immediately */}
      <Suspense fallback={<ReviewSkeleton />}>
        <Await resolve={reviews}>
          {(data) => <ReviewList reviews={data} />}  {/* streamed in */}
        </Await>
      </Suspense>
    </>
  );
}
```

## Islands Decision Guide

**Use static mode + islands** when most of your page is content (blogs, marketing, docs, e-commerce product pages). Static HTML ships immediately, JavaScript only for interactive components.

**Use app mode** (full SSR + client navigation) when the page is primarily interactive — dashboards, editors, collaborative tools.

| Site type | Mode | Why |
|-----------|------|-----|
| Blog / docs / marketing | `static` + `<Counter client:load />` | 0 JS baseline, islands for interactions |
| SaaS dashboard | `app` | Frequent updates, complex state |
| E-commerce PDP | `static` + cart island | Fast LCP, only cart hydrates |
| Real-time feed | `app` | Continuous data, client navigation |

Astro's island architecture outperforms full-hydration approaches for content-heavy sites. Qwik resumability only wins for SPA-like experiences where <50ms TTI is a hard requirement.

## Caching

Three-tier model, applied automatically based on route type:

| Tier | Scope | Lifetime |
|------|-------|---------|
| **Request** | Single render | Deduplicates identical fetches within one page render |
| **Function** | Server process | Memoizes loader results; bust manually on mutation |
| **CDN** | Edge network | Static routes cached indefinitely; app routes use `Cache-Control: s-maxage` |

```tsx
// Opt-in caching in loaders — composable with the existing cache() API
export async function loader({ request }) {
  return cache(() => fetch("/api/data").then(r => r.json()), {
    key: "my-data",
    ttl: 60,
    tags: ["products"],  // invalidate with revalidateTag("products")
  });
}
```

## Framework Stack

| Component | Choice | Why |
|---|---|---|
| Language | TypeScript | Type safety, industry standard |
| Bundler | Vite 6 | Fast, framework-agnostic SSR, HMR <50ms — Rolldown (Rust) bridges dev-to-prod gap |
| Routing | File-based nested | Layouts persist, parallel data loading, per-route errors |
| Data loading | Loaders | One pattern, server-only, full type inference |
| Mutations | Actions + `<Form>` | Progressive enhancement, auto-revalidation |
| Static rendering | Build-time HTML | Zero JS, islands for opt-in interactivity |
| App rendering | Streaming SSR + Preact | 3KB runtime, head-early flush, Suspense-based streaming |
| Deployment | Adapter pattern | Same code, any target |

## Packages

| Package | Description |
|---|---|
| `neutron` | Core framework |
| `neutron-cli` | Dev server, build, preview |
| `create-neutron` | Project scaffolding |
| `neutron-data` | Database, cache, sessions, queues, storage |
| `@neutron/auth` | Auth middleware and adapters |
| `@neutron/security` | CSP, CSRF, rate limiting |
| `@neutron/cache-redis` | Distributed Redis/Dragonfly cache |
| `@neutron/ops` | Health checks, request tracing, logging |
| `@neutron/otel` | OpenTelemetry integration |

## Documentation

See the [docs](./docs/) for full documentation, or visit [neutron.build](https://neutron.build).

## License

MIT
