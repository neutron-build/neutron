export const USERS: Record<number, string> = {
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
    body: "Welcome to our platform! This comprehensive guide will walk you through everything you need to know to get up and running quickly. We have designed the onboarding experience to be as smooth as possible, but there are a few key concepts worth understanding before you dive in.\n\nFirst, let's talk about the core architecture. Our system is built on a modern stack that prioritizes performance and developer experience. The frontend uses component-based rendering with server-side rendering for optimal initial load times. The backend is powered by a high-performance runtime that handles concurrent requests efficiently.\n\nSetting up your development environment is straightforward. You will need Node.js 18 or later, a package manager like npm or pnpm, and a code editor of your choice. We recommend VS Code with our official extension for the best experience, including syntax highlighting, auto-completion, and inline documentation.\n\nOnce your environment is ready, creating a new project takes just one command. The CLI will scaffold a complete application with routing, server-side rendering, and a development server with hot module replacement. You can start building features immediately without worrying about configuration.\n\nThe project structure follows convention over configuration principles. Pages are defined by the file system, with each route corresponding to a file in the routes directory. Data loading happens in dedicated server files that run only on the backend, keeping your API keys and database queries secure.\n\nWe also provide a rich ecosystem of plugins and adapters. Whether you are deploying to a traditional server, a serverless platform, or the edge, there is an adapter that handles the build output for your target environment. This flexibility means you can start developing now and decide on your deployment strategy later.",
  },
  {
    slug: "performance-tips",
    title: "Performance Tips",
    date: "2026-01-20",
    tags: ["perf"],
    body: "Performance is not just about speed — it is about delivering the best possible user experience. In this article, we will explore proven techniques to make your application faster and more responsive.\n\nStart with measuring. Before optimizing anything, establish baseline metrics. Use Lighthouse, Web Vitals, and server-side timing to understand where time is being spent. Premature optimization without data often leads to complex code that does not address the actual bottlenecks.\n\nServer-side rendering is your first line of defense against slow initial loads. By rendering HTML on the server, users see meaningful content before any JavaScript has loaded or executed. This dramatically improves First Contentful Paint and Largest Contentful Paint scores.\n\nCode splitting happens automatically with route-based chunking, but you can go further. Dynamic imports for heavy components, lazy loading for below-the-fold content, and tree shaking for unused exports all reduce the amount of JavaScript shipped to the client.\n\nCaching is a multiplier for performance. Implement caching at every layer: HTTP cache headers for static assets, in-memory caches for computed data, and CDN caching for global distribution. A well-designed cache strategy can reduce server load by orders of magnitude.\n\nDatabase queries are often the biggest bottleneck. Use connection pooling, indexed queries, and pagination. Consider denormalization for read-heavy workloads. For complex aggregations, pre-compute results and serve them from cache.\n\nFinally, monitor production performance continuously. Synthetic benchmarks are useful but do not capture the full picture. Real user monitoring reveals issues specific to certain devices, networks, and geographic regions that lab tests might miss.",
  },
  {
    slug: "deployment-guide",
    title: "Deployment Guide",
    date: "2026-01-25",
    tags: ["ops"],
    body: "Deploying your application to production requires careful planning and the right tools. This guide covers everything from build configuration to monitoring in production.\n\nThe build process transforms your source code into optimized production assets. Static assets are hashed for long-term caching, JavaScript is minified and tree-shaken, and server code is bundled for your target runtime. The adapter system handles platform-specific output formats.\n\nFor traditional server deployments, the Node adapter produces a standalone server that can run anywhere Node.js is available. Configure your process manager (PM2, systemd, or Docker) to handle restarts and log rotation. Set up a reverse proxy like Nginx for SSL termination and static file serving.\n\nServerless deployments offer automatic scaling and pay-per-use pricing. Each route becomes an independent function that scales independently. Cold starts can be mitigated with provisioned concurrency or by keeping function bundles small.\n\nEdge deployments bring your application closer to users worldwide. With edge functions, your server-side code runs in data centers near each user, reducing latency dramatically. This is ideal for personalized content that cannot be cached at the CDN level.\n\nEnvironment variables manage configuration across deployment stages. Never commit secrets to version control. Use your platform's secret management system and validate that required variables are present at startup.\n\nHealth checks and monitoring are essential for production reliability. Implement a health endpoint that verifies database connectivity and external service availability. Set up alerting for error rates, response times, and resource utilization.",
  },
  {
    slug: "authentication",
    title: "Authentication Patterns",
    date: "2026-02-01",
    tags: ["auth"],
    body: "Authentication is a critical part of any web application. Getting it right means balancing security with user experience. This article explores modern authentication patterns and their tradeoffs.\n\nSession-based authentication remains the most straightforward approach. After verifying credentials, the server creates a session and sends a cookie to the client. Subsequent requests include the cookie automatically, making it seamless for the user. Server-side sessions allow easy revocation and do not expose any data to the client.\n\nToken-based authentication using JWTs offers stateless verification. The server issues a signed token containing user claims, and the client includes it in request headers. This eliminates server-side session storage but makes revocation more complex. Short-lived access tokens paired with refresh tokens provide a reasonable compromise.\n\nOAuth and social login delegate authentication to trusted providers. Users sign in with their existing Google, GitHub, or other accounts, reducing friction and password fatigue. Implement the authorization code flow with PKCE for maximum security.\n\nMulti-factor authentication adds a second layer of verification. Time-based one-time passwords (TOTP), hardware security keys (WebAuthn), or email magic links all provide additional security. Make MFA easy to set up and provide backup codes for account recovery.\n\nProtecting routes requires both client-side and server-side checks. Client-side guards prevent unauthorized navigation and show appropriate UI, while server-side middleware ensures that protected data never reaches unauthorized clients. Always validate authorization on the server — client-side checks alone are insufficient.\n\nRate limiting and account lockout policies protect against brute force attacks. Implement exponential backoff after failed attempts, notify users of suspicious activity, and consider IP-based rate limiting for login endpoints.",
  },
  {
    slug: "data-fetching",
    title: "Data Fetching Strategies",
    date: "2026-02-10",
    tags: ["data"],
    body: "Efficient data fetching is the backbone of a responsive application. The strategy you choose affects performance, user experience, and code complexity. Let us examine the options available.\n\nServer-side data loading runs before the page renders, ensuring that content is available immediately. Load functions execute on the server during SSR and on the client during navigation. They receive context about the request, including URL parameters, cookies, and headers, making them ideal for authenticated data fetching.\n\nParallel data loading prevents waterfall requests. When a page needs data from multiple sources, fetching them concurrently reduces total load time to the duration of the slowest request rather than the sum of all requests. Use Promise.all or parallel load functions to maximize throughput.\n\nStreaming allows the server to send data progressively. Instead of waiting for all data before responding, the server can flush the HTML shell immediately and stream in data-dependent sections as they resolve. This improves perceived performance, especially for pages with slow data sources.\n\nClient-side fetching is appropriate for data that changes frequently or is user-specific in ways that prevent caching. Polling, WebSockets, and Server-Sent Events each serve different real-time update patterns. Choose based on your update frequency and infrastructure constraints.\n\nCaching strategies vary by data characteristics. Static data can be cached aggressively with long TTLs. User-specific data benefits from stale-while-revalidate patterns. Frequently changing data might skip caching entirely or use very short TTLs with background refresh.\n\nError handling in data fetching requires thoughtful design. Network failures, timeouts, and server errors all need graceful handling. Show meaningful error messages, provide retry mechanisms, and ensure that partial failures do not break the entire page. Implement circuit breakers for external service dependencies.",
  },
];

export const GALLERY_IMAGES = Array.from({ length: 10 }, (_, i) => ({
  id: i + 1,
  src: `https://picsum.photos/seed/bench${i + 1}/800/600`,
  alt: `Bench image ${i + 1}`,
  width: 800,
  height: 600,
}));

export const DEFAULT_SETTINGS = {
  theme: "light" as string,
  language: "en" as string,
  notifications: true as boolean,
  timezone: "UTC" as string,
};

export let currentSettings = { ...DEFAULT_SETTINGS };

export const VALID_TOKEN = "valid-token";

export function lcgWork(seed: number, iterations: number): number {
  let v = seed;
  for (let i = 0; i < iterations; i++) {
    v = (v * 1664525 + 1013904223) & 0xffffffff;
  }
  return v >>> 0;
}

export function generateBigTableRows(count: number) {
  const rows = [];
  for (let i = 0; i < count; i++) {
    rows.push({
      id: i + 1,
      name: `Row ${i + 1}`,
      value: ((i * 1664525 + 1013904223) & 0xffffffff) >>> 0,
      active: i % 3 !== 0,
    });
  }
  return rows;
}

export function validateSettings(data: Record<string, unknown>) {
  const errors: Record<string, string> = {};

  const theme = data.theme as string | undefined;
  if (!theme || !["light", "dark", "system"].includes(theme)) {
    errors.theme = "Theme must be light, dark, or system";
  }

  const language = data.language as string | undefined;
  if (!language || language.length < 2) {
    errors.language = "Language must be at least 2 characters";
  }

  const timezone = data.timezone as string | undefined;
  if (!timezone) {
    errors.timezone = "Timezone is required";
  }

  return {
    valid: Object.keys(errors).length === 0,
    errors,
    parsed: {
      theme: theme ?? currentSettings.theme,
      language: language ?? currentSettings.language,
      notifications: data.notifications === "on" || data.notifications === true,
      timezone: timezone ?? currentSettings.timezone,
    },
  };
}
