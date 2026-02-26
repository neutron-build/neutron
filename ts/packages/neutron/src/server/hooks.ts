/**
 * Global server hooks for request/error handling
 * Inspired by SvelteKit's hooks system
 */

import type { AppContext } from '../core/types.js';

/**
 * Event object passed to hooks
 */
export interface HookEvent {
  request: Request;
  context: AppContext;
  url: URL;
  params: Record<string, string>;
}

/**
 * Resolve function for handle hook
 */
export interface ResolveOptions {
  /**
   * Transform the response before returning
   */
  transformPageChunk?: (chunk: { html: string; done: boolean }) => string;
}

/**
 * Global request handler hook
 * Runs for every request before route matching
 *
 * @example
 * ```typescript
 * export async function handle({ event, resolve }) {
 *   // Add custom headers, auth, logging, etc.
 *   event.context.user = await getUser(event.request);
 *
 *   const response = await resolve(event);
 *
 *   response.headers.set('X-Custom-Header', 'value');
 *   return response;
 * }
 * ```
 */
export type HandleHook = (args: {
  event: HookEvent;
  resolve: (event: HookEvent, options?: ResolveOptions) => Promise<Response>;
}) => Promise<Response>;

/**
 * Global error handler hook
 * Called when any error occurs during request handling
 *
 * @example
 * ```typescript
 * export async function handleError({ error, event }) {
 *   console.error('Server error:', error);
 *
 *   await errorService.log({
 *     error,
 *     user: event.context.user,
 *     url: event.url.pathname,
 *   });
 *
 *   return {
 *     message: 'An error occurred',
 *     code: error.code || 'UNKNOWN',
 *   };
 * }
 * ```
 */
export type HandleErrorHook = (args: {
  error: Error | unknown;
  event: HookEvent;
}) => Promise<{ message: string; code?: string } | void>;

/**
 * Fetch interceptor hook
 * Intercepts all fetch calls during SSR
 *
 * @example
 * ```typescript
 * export async function handleFetch({ request, fetch }) {
 *   // Add auth headers to external API calls
 *   if (request.url.startsWith('https://api.example.com')) {
 *     request.headers.set('Authorization', `Bearer ${API_TOKEN}`);
 *   }
 *
 *   return fetch(request);
 * }
 * ```
 */
export type HandleFetchHook = (args: {
  request: Request;
  fetch: typeof global.fetch;
}) => Promise<Response>;

/**
 * Global server hooks configuration
 */
export interface GlobalServerHooks {
  handle?: HandleHook;
  handleError?: HandleErrorHook;
  handleFetch?: HandleFetchHook;
}

/**
 * Default handle hook (pass-through)
 */
export const defaultHandle: HandleHook = async ({ event, resolve }) => {
  return resolve(event);
};

/**
 * Default error handler (console.error)
 *
 * SECURITY: In production, generic error messages are returned to prevent
 * information disclosure. Detailed error messages are only shown in development.
 */
export const defaultHandleError: HandleErrorHook = async ({ error, event }) => {
  // Always log the full error server-side for debugging
  console.error('[Neutron] Server error:', error);
  console.error('[Neutron] URL:', event.url.pathname);

  // Determine if we're in production mode
  const isProduction = process.env.NODE_ENV === 'production';

  // In production, return generic error message to prevent information disclosure
  // In development, return detailed error message for debugging
  return {
    message: isProduction
      ? 'An internal error occurred'
      : error instanceof Error
      ? error.message
      : 'An error occurred',
    code: 'INTERNAL_ERROR',
  };
};

/**
 * Default fetch handler (pass-through)
 */
export const defaultHandleFetch: HandleFetchHook = async ({ request, fetch }) => {
  return fetch(request);
};
