export const VALID_TOKEN = "valid-token";

export const USERS = new Map([
  [1, { id: 1, name: "Alice Johnson", email: "alice@example.com", role: "admin" }],
  [2, { id: 2, name: "Bob Smith", email: "bob@example.com", role: "user" }],
  [3, { id: 3, name: "Charlie Brown", email: "charlie@example.com", role: "user" }],
  [4, { id: 4, name: "Diana Prince", email: "diana@example.com", role: "moderator" }],
  [5, { id: 5, name: "Eve Williams", email: "eve@example.com", role: "user" }],
]);

export const BLOG_POSTS = [
  {
    slug: "getting-started-with-solidstart",
    title: "Getting Started with SolidStart",
    date: "2025-01-15",
    tags: ["solidjs", "solidstart", "tutorial"],
    body: `SolidStart is the official meta-framework for SolidJS, providing a full-stack development experience that combines the reactivity of Solid with server-side rendering, file-based routing, and API endpoints. In this comprehensive guide, we will explore the fundamentals of building applications with SolidStart and understand why it has become a popular choice among developers seeking performance and developer experience.

The framework leverages fine-grained reactivity, which means that when state changes occur, only the specific DOM nodes that depend on that state are updated. This is fundamentally different from virtual DOM-based frameworks that perform diffing operations to determine what changed. The result is exceptional runtime performance with minimal overhead, making SolidStart applications feel incredibly fast and responsive to user interactions.

Setting up a SolidStart project is straightforward. You can use the official template to scaffold a new application, which comes pre-configured with TypeScript support, file-based routing, and server-side rendering. The development server supports hot module replacement, allowing you to see changes instantly as you develop. The build process produces optimized bundles that are ready for production deployment.

One of the key features of SolidStart is its approach to data loading. The framework provides query functions that can be used to fetch data on the server before the page renders. This ensures that the initial HTML sent to the client contains all the necessary data, improving both performance and SEO. The createAsync primitive allows components to consume this data reactively, automatically updating when the underlying data changes.

File-based routing in SolidStart follows intuitive conventions. Pages are defined as components in the routes directory, with dynamic segments indicated by square brackets. Layout components can be created to share common UI elements across multiple routes. API routes are defined as TypeScript files that export handler functions for different HTTP methods, making it easy to build full-stack applications without a separate backend server.

The framework also supports middleware, error boundaries, and streaming rendering. Middleware can be used to implement cross-cutting concerns like authentication, logging, and request validation. Error boundaries provide graceful error handling at the component level. Streaming rendering allows the server to send HTML in chunks, improving time-to-first-byte for pages with slow data dependencies.`,
  },
  {
    slug: "reactive-patterns-in-solid",
    title: "Reactive Patterns in Solid",
    date: "2025-02-10",
    tags: ["solidjs", "reactivity", "patterns"],
    body: `Understanding reactive patterns is essential for building efficient SolidJS applications. Solid's reactivity system is built on three core primitives: signals, effects, and memos. These primitives work together to create a fine-grained dependency tracking system that automatically updates only the parts of your application that need to change when state updates occur.

Signals are the foundation of Solid's reactivity. A signal is a reactive value that can be read and written to. When you create a signal with createSignal, you get a getter and setter pair. The getter is a function that returns the current value and registers a dependency when called inside a tracking scope. The setter updates the value and notifies all dependents that the value has changed. This simple mechanism enables powerful reactive compositions.

Effects are side effects that automatically re-run when their dependencies change. Created with createEffect, they track which signals are read during execution and re-execute whenever those signals update. Effects are useful for DOM manipulations, logging, network requests, and other operations that should happen in response to state changes. They run asynchronously after the current synchronous execution completes.

Memos are derived computations that cache their results. Created with createMemo, they are similar to effects but return a value. The computation only re-runs when its dependencies change, and downstream dependents are only notified if the computed value actually changed. This makes memos excellent for expensive computations that multiple parts of your application depend on.

Stores provide a way to manage complex nested state. Unlike signals which work best with primitive values, stores use proxies to make nested objects reactive at every level. You can read deeply nested properties and only the specific property access will be tracked as a dependency. Updates can be made at any level of the object hierarchy using the setter function with path syntax.

The batch function allows you to group multiple signal updates into a single transaction. Without batching, each signal update would trigger dependent effects and computations immediately. With batching, all updates are applied simultaneously, and dependents only run once with the final values. This is important for maintaining consistency and avoiding unnecessary intermediate computations.

Resource management in Solid handles asynchronous data fetching with built-in loading and error states. The createResource primitive accepts a source signal and a fetcher function, automatically refetching when the source changes. It provides loading and error accessors that can be used to show appropriate UI states while data is being fetched.`,
  },
  {
    slug: "server-side-rendering-deep-dive",
    title: "Server-Side Rendering Deep Dive",
    date: "2025-03-05",
    tags: ["ssr", "performance", "solidstart"],
    body: `Server-side rendering is a critical technique for modern web applications that need fast initial page loads and strong search engine optimization. SolidStart provides first-class support for SSR with a streaming architecture that enables optimal performance characteristics. In this article, we will explore how SSR works in SolidStart and the various strategies available for different use cases.

The SSR pipeline in SolidStart begins when a request hits the server. The framework matches the URL to a route, executes any associated data loading functions, and renders the component tree to HTML. This HTML is sent to the client along with the serialized data, allowing the application to hydrate without making additional network requests. The result is a fast first contentful paint with fully interactive content.

Streaming SSR takes this a step further by allowing the server to send HTML in chunks. When a component depends on asynchronous data, the server can send the shell of the page immediately and stream in the dynamic content as it becomes available. This is implemented using Suspense boundaries, which act as natural streaming boundaries in the component tree. The client receives a loading placeholder initially and swaps in the real content when it arrives.

Hydration is the process of attaching event listeners and reactivity to the server-rendered HTML. Solid's hydration is highly efficient because it does not need to re-render the entire component tree on the client. Instead, it walks the existing DOM nodes and attaches reactive subscriptions directly. This means the hydration process is essentially free from a rendering perspective, only adding the necessary interactivity without any visual changes.

Data serialization between server and client is handled automatically by SolidStart. When data is loaded on the server, it is serialized and embedded in the HTML response. On the client, this data is deserialized and used to initialize the reactive state without additional network requests. This ensures consistency between the server-rendered HTML and the client-side application state.

Progressive enhancement is another important aspect of SSR in SolidStart. Forms and actions can work without JavaScript enabled, falling back to traditional form submissions. When JavaScript is available, these interactions are enhanced with client-side handling, providing a smoother user experience. This approach ensures that your application remains functional even in environments where JavaScript is disabled or has not yet loaded.

Caching strategies play a vital role in SSR performance. SolidStart supports various caching mechanisms including CDN caching with appropriate cache headers, in-memory caching for frequently accessed data, and stale-while-revalidate patterns. By combining these strategies, you can achieve near-static performance for dynamic content while maintaining data freshness.`,
  },
  {
    slug: "building-apis-with-solidstart",
    title: "Building APIs with SolidStart",
    date: "2025-04-12",
    tags: ["api", "solidstart", "backend"],
    body: `SolidStart provides a powerful and flexible system for building API endpoints alongside your frontend application. API routes are defined as TypeScript files in the routes directory, following the same file-based routing conventions as page routes. This co-location of frontend and backend code simplifies development and deployment, making SolidStart an excellent choice for full-stack applications.

API routes in SolidStart export functions named after HTTP methods. A file can export GET, POST, PUT, DELETE, and PATCH handlers, each receiving an APIEvent object that provides access to the request, URL parameters, and other context. The handlers return standard Web API Response objects, giving you full control over status codes, headers, and response bodies. This standards-based approach means your knowledge transfers directly to other platforms.

Request parsing is straightforward with the Web API primitives available in the event object. For JSON APIs, you can use event.request.json() to parse the request body. For form data, event.request.formData() provides access to submitted form fields. URL search parameters are available through the URL constructor, and path parameters are extracted from the route pattern automatically.

Error handling in API routes should follow HTTP conventions. Return appropriate status codes for different error conditions: 400 for bad requests, 401 for authentication failures, 403 for authorization failures, 404 for missing resources, and 500 for server errors. Including descriptive error messages in the response body helps API consumers understand and fix issues. Consider using a consistent error response format across all your endpoints.

Authentication and authorization are common concerns for API endpoints. SolidStart supports middleware that can intercept requests before they reach your handlers. You can implement token-based authentication by checking the Authorization header, session-based authentication using cookies, or any other authentication mechanism. Middleware can attach user information to the request context, making it available to downstream handlers.

Streaming responses are supported through the Web Streams API. You can create a ReadableStream and return it in a Response object to send data progressively to the client. This is useful for server-sent events, long-running computations that produce incremental results, or large datasets that should be processed in chunks rather than loaded entirely into memory.

Rate limiting, CORS configuration, and request validation are additional concerns that can be addressed through middleware or within individual handlers. SolidStart gives you the flexibility to implement these at whatever granularity makes sense for your application. For simple applications, inline validation may be sufficient. For larger applications, shared middleware provides better code organization and consistency.`,
  },
  {
    slug: "performance-optimization-techniques",
    title: "Performance Optimization Techniques",
    date: "2025-05-20",
    tags: ["performance", "optimization", "web"],
    body: `Performance optimization is a continuous process that requires understanding the various layers of a web application stack. From network-level optimizations to rendering strategies, every decision impacts the user experience. In this article, we explore practical techniques for optimizing SolidStart applications across the entire stack, from server configuration to client-side rendering patterns.

Network optimization begins with minimizing the number and size of requests. Code splitting automatically divides your application into chunks that are loaded on demand, reducing the initial bundle size. SolidStart handles route-based code splitting automatically, ensuring that users only download the code needed for the current page. Tree shaking eliminates unused code from your bundles, further reducing their size.

Image optimization is one of the most impactful performance improvements you can make. Using modern formats like WebP or AVIF significantly reduces file sizes compared to JPEG or PNG. Responsive images with srcset attributes ensure that users on mobile devices do not download unnecessarily large images. Lazy loading defers the loading of off-screen images until they are about to enter the viewport.

Caching is essential for performance at scale. Browser caching with appropriate Cache-Control headers prevents unnecessary network requests for static assets. CDN caching distributes your content geographically, reducing latency for users worldwide. Application-level caching stores computed results in memory, avoiding repeated expensive operations. Each layer of caching compounds the performance benefits.

Rendering performance in SolidJS is already excellent due to its fine-grained reactivity, but there are patterns that can further improve it. Avoiding unnecessary signal reads prevents components from tracking dependencies they do not need. Using untrack when reading signals inside effects prevents circular dependencies. Lazy evaluation with createMemo defers expensive computations until their results are actually needed.

Database query optimization is critical for server-side performance. Use indexes on frequently queried columns, avoid N+1 query patterns by batching related queries, and consider denormalization for read-heavy workloads. Connection pooling reduces the overhead of establishing database connections. Query result caching at the application level can eliminate database round trips entirely for frequently accessed data.

Monitoring and measurement are essential for identifying performance bottlenecks. Use browser developer tools to analyze network waterfalls, rendering performance, and memory usage. Server-side monitoring tracks response times, error rates, and resource utilization. Real user monitoring provides insights into actual user experiences across different devices and network conditions. Without measurement, optimization efforts are guided by assumptions rather than data.`,
  },
];

export const GALLERY_IMAGES = Array.from({ length: 10 }, (_, i) => ({
  id: i + 1,
  src: `https://picsum.photos/seed/bench${i + 1}/800/600`,
  thumb: `https://picsum.photos/seed/bench${i + 1}/400/300`,
  alt: `Gallery image ${i + 1}`,
  width: 800,
  height: 600,
}));

export interface Settings {
  siteName: string;
  postsPerPage: number;
  enableComments: boolean;
  theme: string;
}

export const DEFAULT_SETTINGS: Settings = {
  siteName: "Bench SolidStart",
  postsPerPage: 10,
  enableComments: true,
  theme: "light",
};

let currentSettings: Settings = { ...DEFAULT_SETTINGS };

export function getSettings(): Settings {
  return { ...currentSettings };
}

export function updateSettings(partial: Partial<Settings>): Settings {
  currentSettings = { ...currentSettings, ...partial };
  return { ...currentSettings };
}

export function validateSettings(
  input: Record<string, unknown>
): { valid: true; data: Partial<Settings> } | { valid: false; errors: string[] } {
  const errors: string[] = [];
  const data: Partial<Settings> = {};

  if ("siteName" in input) {
    if (typeof input.siteName !== "string" || input.siteName.length < 1 || input.siteName.length > 100) {
      errors.push("siteName must be a string between 1 and 100 characters");
    } else {
      data.siteName = input.siteName;
    }
  }

  if ("postsPerPage" in input) {
    const n = Number(input.postsPerPage);
    if (!Number.isInteger(n) || n < 1 || n > 100) {
      errors.push("postsPerPage must be an integer between 1 and 100");
    } else {
      data.postsPerPage = n;
    }
  }

  if ("enableComments" in input) {
    if (typeof input.enableComments !== "boolean") {
      errors.push("enableComments must be a boolean");
    } else {
      data.enableComments = input.enableComments;
    }
  }

  if ("theme" in input) {
    if (typeof input.theme !== "string" || !["light", "dark", "auto"].includes(input.theme)) {
      errors.push("theme must be one of: light, dark, auto");
    } else {
      data.theme = input.theme;
    }
  }

  if (errors.length > 0) return { valid: false, errors };
  return { valid: true, data };
}

export function lcgWork(seed: number, iterations: number): number {
  let v = seed;
  for (let i = 0; i < iterations; i++) {
    v = (v * 1664525 + 1013904223) & 0xffffffff;
  }
  return v >>> 0;
}

export function generateBigTableRows(count: number) {
  const rows = [];
  let v = 42;
  for (let i = 0; i < count; i++) {
    v = (v * 1664525 + 1013904223) & 0xffffffff;
    rows.push({
      id: i + 1,
      name: `row-${i + 1}`,
      value: v >>> 0,
      active: i % 3 !== 0,
      category: ["alpha", "beta", "gamma", "delta"][i % 4],
    });
  }
  return rows;
}
