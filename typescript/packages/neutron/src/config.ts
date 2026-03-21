import type { NeutronAdapter } from "./adapters/adapter.js";
import type { NeutronServerOptions } from "./server/index.js";

export type NeutronRuntime = "preact" | "react-compat";

export interface NeutronRedirectRule {
  source: string;
  destination: string;
  permanent?: boolean;
  statusCode?: 301 | 302 | 307 | 308;
}

export interface NeutronRewriteRule {
  source: string;
  destination: string;
}

export interface NeutronHeaderRule {
  source: string;
  headers: Record<string, string>;
}

export interface NeutronRoutesConfig {
  redirects?: NeutronRedirectRule[];
  rewrites?: NeutronRewriteRule[];
  headers?: NeutronHeaderRule[];
}

export interface NeutronWorkerConfig {
  entry?: string;
}

/**
 * Input validation limits to prevent DoS attacks
 *
 * SECURITY: These limits protect against malicious requests with oversized payloads,
 * excessive headers, or extremely long URLs that could cause memory exhaustion or
 * processing delays.
 */
export interface NeutronInputLimits {
  /**
   * Maximum request body size in bytes
   * @default 10485760 (10MB)
   */
  maxRequestBodySize?: number;

  /**
   * Maximum size of individual header values in bytes
   * @default 16384 (16KB)
   */
  maxHeaderSize?: number;

  /**
   * Maximum number of headers allowed in a request
   * @default 100
   */
  maxHeaderCount?: number;

  /**
   * Maximum URL length in characters
   * @default 2048
   */
  maxUrlLength?: number;
}

export interface NeutronImageConfig {
  /** Default quality for optimized images (1-100) @default 75 */
  quality?: number;
  /** Preferred output formats in priority order @default ["avif", "webp"] */
  formats?: string[];
  /** Responsive image widths to generate @default [320, 640, 960, 1200, 1600] */
  widths?: number[];
  /** Allowed remote image hostnames */
  remotePatterns?: Array<{ hostname: string; protocol?: string }>;
}

export interface NeutronMarkdownConfig {
  remarkPlugins?: unknown[];
  rehypePlugins?: unknown[];
  syntaxHighlight?: { theme?: string } | false;
}

export interface NeutronConfig {
  adapter?: NeutronAdapter;
  server?: Omit<NeutronServerOptions, "rootDir">;
  routes?: NeutronRoutesConfig;
  runtime?: NeutronRuntime;
  worker?: NeutronWorkerConfig;
  /** Image optimization configuration */
  image?: NeutronImageConfig;
  /**
   * Input validation limits
   * @default { maxRequestBodySize: 10485760, maxHeaderSize: 16384, maxHeaderCount: 100, maxUrlLength: 2048 }
   */
  inputLimits?: NeutronInputLimits;
  markdown?: NeutronMarkdownConfig;
}

export function defineConfig(config: NeutronConfig): NeutronConfig {
  return config;
}

export function resolveRuntime(config?: NeutronConfig): NeutronRuntime {
  return config?.runtime ?? "preact";
}

export function resolveRuntimeAliases(
  runtime: NeutronRuntime
): Record<string, string> | undefined {
  if (runtime !== "react-compat") {
    return undefined;
  }

  return {
    react: "preact/compat",
    "react-dom": "preact/compat",
    "react-dom/client": "preact/compat",
    "react-dom/server": "preact-render-to-string",
    "react-dom/server.browser": "preact-render-to-string",
    "react-dom/test-utils": "preact/test-utils",
    "react/jsx-runtime": "preact/jsx-runtime",
    "react/jsx-dev-runtime": "preact/jsx-dev-runtime",
  };
}

export function resolveRuntimeNoExternal(runtime: NeutronRuntime): string[] {
  if (runtime !== "react-compat") {
    return [];
  }

  return [
    "react",
    "react-dom",
    "react-dom/client",
    "react-dom/server",
    "react-dom/server.browser",
  ];
}
