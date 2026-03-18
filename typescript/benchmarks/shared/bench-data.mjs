/**
 * Shared benchmark data — blog posts, gallery items, user map, dashboard settings.
 * Each framework app imports or copies the relevant data structures.
 */

export const USERS = {
  1: "Alice",
  2: "Bob",
  3: "Charlie",
};

export const BLOG_POSTS = [
  {
    slug: "getting-started",
    title: "Getting Started",
    date: "2026-01-15",
    tags: ["intro"],
    body: `Welcome to the framework benchmark suite. This post covers the basics of setting up a modern web application from scratch. We'll walk through project initialization, dependency management, and the core concepts you need to understand before diving deeper.

Every framework in this benchmark implements the same set of routes, but each uses its own idiomatic patterns. The goal is to measure real-world performance across realistic workloads rather than synthetic micro-benchmarks that don't reflect actual application behavior.

When choosing a framework, performance is just one factor among many. Developer experience, ecosystem maturity, deployment options, and community support all play critical roles. This benchmark aims to provide objective throughput data so you can make informed decisions.

The test scenarios range from simple static page rendering to complex form mutations with validation, streaming responses, and nested layout composition. Each represents a common pattern found in production web applications.

We encourage you to run these benchmarks on your own hardware and network conditions. Results will vary based on CPU architecture, memory bandwidth, Node.js version, and operating system configuration. The relative rankings tend to be more stable than absolute numbers.

To get started with any framework in this suite, check the individual README files in each benchmark app directory. They include setup instructions, architecture notes, and links to official documentation.

Performance optimization is an iterative process. Start with correct behavior, measure against realistic workloads, identify bottlenecks through profiling, and then apply targeted optimizations. Premature optimization without measurement data is a common anti-pattern that leads to complex code without meaningful gains.`,
  },
  {
    slug: "performance-tips",
    title: "Performance Tips",
    date: "2026-01-20",
    tags: ["perf"],
    body: `Optimizing web application performance requires understanding where time is spent. The critical rendering path, server response time, and network transfer size are the three main areas where frameworks differ significantly.

Server-side rendering performance depends on the virtual DOM implementation, component tree depth, and data fetching strategy. Frameworks that minimize allocations during render cycles tend to perform better under high concurrency. Streaming SSR can improve Time to First Byte by flushing HTML as components resolve.

Client-side hydration is another performance-critical phase. Partial hydration, progressive hydration, and island architecture are strategies that reduce the JavaScript payload required for interactivity. Each framework in this benchmark takes a different approach to this problem.

Caching is the single most impactful optimization for read-heavy workloads. HTTP cache headers, in-memory application caches, and edge caching can eliminate redundant computation entirely. The cache invalidation routes in this benchmark measure how quickly frameworks can bust stale data.

Database query optimization, while outside the scope of this benchmark, is often the dominant factor in production application performance. The synthetic data loaders used here approximate the overhead of a fast in-memory lookup to isolate framework overhead from I/O latency.

Bundle size directly impacts load performance on slower networks. Tree-shaking effectiveness, code-splitting granularity, and runtime size vary significantly across frameworks. Smaller bundles mean faster parse and execute times, especially on mobile devices.

Connection pooling, keep-alive settings, and HTTP/2 multiplexing at the infrastructure level can dramatically affect throughput. This benchmark uses autocannon with configurable connection counts and pipelining to stress-test these characteristics under controlled conditions.`,
  },
  {
    slug: "deployment-guide",
    title: "Deployment Guide",
    date: "2026-01-25",
    tags: ["ops"],
    body: `Deploying a modern web application involves choosing between several hosting models: serverless functions, long-running Node.js servers, edge runtimes, and static site hosting. Each framework supports different deployment targets with varying levels of optimization.

For this benchmark suite, all frameworks are deployed as long-running Node.js processes to ensure a fair comparison. Production builds are generated with each framework's recommended build tooling, and servers are started with production environment variables.

Container-based deployments using Docker provide consistent runtime environments across development and production. A minimal Node.js Alpine image with multi-stage builds keeps container sizes small. Health check endpoints should be configured for orchestrators like Kubernetes.

Environment variable management is critical for security. Never commit secrets to version control. Use platform-specific secret management services or encrypted environment files. The benchmark apps use simple in-memory configuration for demonstration purposes.

Monitoring and observability in production require structured logging, distributed tracing, and metric collection. OpenTelemetry provides a vendor-neutral standard for instrumenting applications. Each framework has community integrations for popular observability platforms.

Rolling deployments with health checks minimize downtime. Blue-green deployments provide instant rollback capability. Canary releases route a small percentage of traffic to new versions for validation. The deployment strategy should match your risk tolerance and rollback requirements.

CDN configuration for static assets, proper cache headers for dynamic content, and geographic distribution of compute resources round out a production-ready deployment. The benchmark results represent single-server throughput; real-world architectures typically scale horizontally behind load balancers.`,
  },
  {
    slug: "authentication",
    title: "Authentication Patterns",
    date: "2026-02-01",
    tags: ["auth"],
    body: `Authentication in web applications typically follows one of several patterns: session-based with cookies, token-based with JWTs, or delegated via OAuth/OIDC providers. Each framework in this benchmark implements a simplified token-based auth check for the protected routes.

The benchmark auth flow validates a Bearer token in the Authorization header. Protected routes return 401 when the token is missing or invalid. This mirrors the middleware pattern used in production applications where auth validation happens before route handlers execute.

Session management adds complexity with cookie handling, CSRF protection, and session storage backends. The session refresh endpoint in this benchmark simulates the overhead of reading and writing session data during a request cycle. Frameworks that handle cookies at the middleware level tend to have lower per-request overhead.

Multi-factor authentication, passwordless login, and social sign-in are increasingly common in production applications. These patterns involve external API calls that add variable latency to authentication flows. The benchmark uses fast in-memory validation to isolate framework overhead.

Authorization is distinct from authentication. Role-based access control, attribute-based policies, and resource-level permissions layer additional checks after identity verification. The dashboard routes in this benchmark demonstrate a simple auth guard pattern that most frameworks support natively.

Security headers like Content-Security-Policy, Strict-Transport-Security, and X-Frame-Options should be configured at the reverse proxy or middleware level. Framework-specific security middleware packages often provide sensible defaults that can be customized per route.

Rate limiting, brute force protection, and account lockout policies are essential for production authentication systems. These are typically implemented at the infrastructure level using tools like fail2ban, WAF rules, or dedicated rate-limiting middleware.`,
  },
  {
    slug: "data-fetching",
    title: "Data Fetching Strategies",
    date: "2026-02-10",
    tags: ["data"],
    body: `Data fetching patterns differ significantly across modern web frameworks. Server-side data loading, client-side fetching, and hybrid approaches each have performance implications that this benchmark measures.

Server Components in React-based frameworks allow data fetching directly in component code without API routes. This colocation of data and UI logic simplifies the mental model but requires careful attention to caching and waterfall prevention. Next.js and React Router both support this pattern.

Loader functions in Remix and React Router run on the server before rendering. They receive the request context including URL parameters, headers, and cookies. Data flows unidirectionally from loaders to components via hooks. This clear separation makes caching and error handling straightforward.

Astro's frontmatter data fetching runs at build time for static pages or request time for server-rendered pages. The island architecture means interactive components receive serialized props rather than fetching their own data. This minimizes client-side JavaScript while maintaining interactivity where needed.

SvelteKit's load functions run on both server and client during navigation. Server-only load functions in +page.server.ts handle sensitive operations like database queries and authentication. Universal load functions in +page.ts run on both sides for data that doesn't require server access.

SolidStart uses the query primitive with createAsync for data fetching. Server functions marked with "use server" run exclusively on the server. Solid's fine-grained reactivity means only the specific DOM nodes affected by data changes update, avoiding full component re-renders.

Parallel data fetching is critical for performance. Frameworks that support Promise.all-style concurrent loading of independent data sources outperform those with sequential waterfall fetching. The dashboard route in this benchmark loads multiple data sources to test this capability.

Stale-while-revalidate patterns serve cached data immediately while refreshing in the background. This provides the best user experience for data that changes infrequently. The cache routes in this benchmark measure the overhead of this pattern across frameworks.`,
  },
];

export const GALLERY_IMAGES = [
  { id: 1, src: "https://picsum.photos/seed/bench1/800/600", alt: "Bench image 1", width: 800, height: 600 },
  { id: 2, src: "https://picsum.photos/seed/bench2/800/600", alt: "Bench image 2", width: 800, height: 600 },
  { id: 3, src: "https://picsum.photos/seed/bench3/800/600", alt: "Bench image 3", width: 800, height: 600 },
  { id: 4, src: "https://picsum.photos/seed/bench4/800/600", alt: "Bench image 4", width: 800, height: 600 },
  { id: 5, src: "https://picsum.photos/seed/bench5/800/600", alt: "Bench image 5", width: 800, height: 600 },
  { id: 6, src: "https://picsum.photos/seed/bench6/800/600", alt: "Bench image 6", width: 800, height: 600 },
  { id: 7, src: "https://picsum.photos/seed/bench7/800/600", alt: "Bench image 7", width: 800, height: 600 },
  { id: 8, src: "https://picsum.photos/seed/bench8/800/600", alt: "Bench image 8", width: 800, height: 600 },
  { id: 9, src: "https://picsum.photos/seed/bench9/800/600", alt: "Bench image 9", width: 800, height: 600 },
  { id: 10, src: "https://picsum.photos/seed/bench10/800/600", alt: "Bench image 10", width: 800, height: 600 },
];

export const DEFAULT_SETTINGS = {
  theme: "light",
  language: "en",
  notifications: true,
  timezone: "UTC",
};

/**
 * Bench cache — 30s TTL in-memory cache with LCG work function.
 * Each framework copies this into its own bench-cache module.
 */
export function createBenchCache(ttlMs = 30000) {
  let cached = null;
  let cachedAt = 0;
  let version = 1;

  return {
    get() {
      const now = Date.now();
      if (cached && now - cachedAt < ttlMs) {
        return { data: cached, hit: true };
      }
      const data = { version, generatedAt: new Date().toISOString() };
      cached = data;
      cachedAt = now;
      return { data, hit: false };
    },
    revalidate() {
      version += 1;
      cached = null;
      cachedAt = 0;
      return { version };
    },
  };
}

/**
 * LCG (Linear Congruential Generator) work function.
 * Burns CPU cycles for compute benchmark scenarios.
 */
export function lcgWork(seed = 13, iterations = 140000) {
  let value = seed;
  for (let i = 0; i < iterations; i++) {
    value = (value * 1664525 + 1013904223) & 0xffffffff;
  }
  return value >>> 0;
}

/**
 * Generate table rows for the /big route.
 */
export function generateBigTableRows(count = 400) {
  const rows = [];
  for (let i = 1; i <= count; i++) {
    rows.push({
      id: i,
      name: `User ${i}`,
      email: `user${i}@example.com`,
      score: ((i * 7 + 13) % 100),
    });
  }
  return rows;
}

/**
 * Validate settings form data.
 * Returns { valid: true, data } or { valid: false, errors }.
 */
export function validateSettings(formData) {
  const errors = {};
  const theme = formData.theme || formData.get?.("theme");
  const language = formData.language || formData.get?.("language");
  const timezone = formData.timezone || formData.get?.("timezone");

  if (!theme || !["light", "dark", "system"].includes(theme)) {
    errors.theme = "Invalid theme. Must be light, dark, or system.";
  }
  if (!language || typeof language !== "string" || language.length < 2) {
    errors.language = "Language code required (minimum 2 characters).";
  }
  if (!timezone || typeof timezone !== "string") {
    errors.timezone = "Timezone is required.";
  }

  if (Object.keys(errors).length > 0) {
    return { valid: false, errors };
  }

  return {
    valid: true,
    data: {
      theme,
      language,
      notifications: formData.notifications === "on" || formData.notifications === true || formData.get?.("notifications") === "on",
      timezone,
    },
  };
}

/**
 * Auth token validation.
 */
export const VALID_TOKEN = "valid-token";

export function validateAuth(request) {
  const authHeader = typeof request === "string"
    ? request
    : request?.headers?.get?.("authorization") || request?.headers?.authorization || "";
  return authHeader === `Bearer ${VALID_TOKEN}`;
}

/**
 * In-memory settings store (per-process).
 */
let currentSettings = { ...DEFAULT_SETTINGS };

export function getSettings() {
  return { ...currentSettings };
}

export function updateSettings(newSettings) {
  currentSettings = { ...currentSettings, ...newSettings };
  return currentSettings;
}
