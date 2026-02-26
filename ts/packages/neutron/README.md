# neutron

The unified TypeScript web framework. Static sites with zero JavaScript, app routes with Preact SSR, one router, deploy anywhere.

## Install

```bash
npm create neutron@latest
```

Or add to an existing project:

```bash
npm install neutron neutron-cli
```

## Usage

### Static Route (zero JS)

```tsx
// src/routes/about.tsx
export const config = { mode: "static" };

export default function About() {
  return <h1>Pure HTML. No JavaScript shipped.</h1>;
}
```

### App Route (SSR + interactivity)

```tsx
// src/routes/app/dashboard.tsx
import { useLoaderData, Form } from "neutron";

export const config = { mode: "app" };

export async function loader({ request }) {
  return { user: await getUser(request) };
}

export default function Dashboard() {
  const { user } = useLoaderData();
  return <h1>Welcome, {user.name}</h1>;
}
```

### Islands (interactive components on static pages)

```tsx
import { Counter } from "../components/Counter";

export default function Home() {
  return (
    <div>
      <h1>Static HTML</h1>
      <Counter client:load />
    </div>
  );
}
```

## Exports

```ts
import { ... } from "neutron";           // Core types & utilities
import { ... } from "neutron/server";    // Server runtime, createServer
import { ... } from "neutron/client";    // Client hooks, Form, Link
import { ... } from "neutron/vite";      // Vite plugin
import { ... } from "neutron/content";   // Content collections
```

## CLI

```bash
neutron dev       # Start dev server
neutron build     # Production build
neutron start     # Start production server
neutron preview   # Preview production build
```

## Documentation

Full docs at [neutron.build](https://neutron.build) or in the [docs/](https://github.com/neutron-build/neutron/tree/main/docs) directory.

## License

Business Source License 1.1. Converts to MIT after 4 years.
