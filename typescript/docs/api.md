# Neutron API

> **Terminology note:** This page documents **Neutron TypeScript**. In broader ecosystem docs, **Neutron** refers to the umbrella framework/platform across implementations.


Primary package: `neutron`

## Core

- `defineConfig(config)`
- `createServer(options)`
- `startServer(options)`
- `discoverRoutes({ routesDir })`
- `createRouter()`
- `prepareRouteTypes(options?)`
- `server.hooks` observability callbacks (`onRequestStart`, `onRequestEnd`, `onLoaderStart`, `onLoaderEnd`, `onActionStart`, `onActionEnd`, `onError`)

Global route rules (`defineConfig({ routes })` / `createServer({ routes })`):

- `routes.redirects`: source -> destination redirects (`307` default, `308` when `permanent: true`).
- `routes.rewrites`: source -> destination internal rewrites.
- `routes.headers`: path-based response headers applied when absent.

Route config cache fields:

- `cache.maxAge`: app response cache TTL (seconds)
- `cache.loaderMaxAge`: loader-data cache TTL (seconds)
- `hydrate`: set `false` to serve an app route without inlined loader payload or client runtime boot script (zero-JS SSR route).

Route module exports:

- `head(args)`: optional route/layout head metadata resolver (supports layered SEO objects or raw head fragments).

Loader cache policy notes:

- Read hits only on `GET`/`HEAD`.
- Fresh loader results can be stored after mutations, then reused on next reads.
- Requests with `Authorization`/`Cookie` or `Cache-Control: no-cache|no-store` bypass loader cache.
- Client data requests can use `X-Neutron-Data: true`.
- Optional `X-Neutron-Routes: route:id,...` can request partial loader execution for matched routes.

Server cache store options (`createServer({ cache })`):

- `cache.app`: pluggable app-response cache store.
- `cache.loader`: pluggable loader-data cache store.
- In-memory defaults remain built-in when no custom stores are provided.

Observability hook notes:

- Hooks are framework-native extension points and do not require vendor SDKs.
- Hooks are suitable for adapters to PostHog, OpenTelemetry/SigNoz, Umami, Uptime Kuma, etc.
- Hook failures are isolated and do not crash request handling.

## Response + Serialization

- `redirect(url, init?)`
- `json(data, init?)`
- `isResponse(value)`
- `serializeTransportData(value)`
- `deserializeTransportData(serialized)`
- `encodeSerializedPayload(payload)`
- `decodeSerializedPayload(encoded)`
- `encodeSerializedPayloadAsJson(payload)`
- `serializeForInlineScript(value)`
- `escapeJsonForInlineScript(value)`

## Content Collections

Import from `neutron/content` or `neutron`.

- `defineCollection({ type?, schema })`
- `getCollection(name, filter?)`
- `getEntry(name, slug)`
- `prepareContentCollections(options?)`
- `z` (re-export from Zod)

Types:

- `CollectionDefinition`
- `CollectionEntry`
- `ContentCollectionMap`
- `PrepareContentCollectionsOptions`

## Runtime + Adapters

- `resolveRuntime(config?)`
- `resolveRuntimeAliases(runtime)`
- `resolveRuntimeNoExternal(runtime)`
- `adapterNode(options?)`
- `adapterStatic(options?)`
- `adapterCloudflare(options?)`
- `adapterVercel(options?)`
- `adapterDocker(options?)`

Adapter types:

- `NeutronAdapter`
- `AdapterBuildContext`
- `AdapterRoutesSummary`
- `AdapterRuntimeBundle`

## Cookies + Sessions

- `parseCookieHeader(cookieHeader)`
- `getCookie(cookieHeader, name)`
- `serializeCookie(name, value, options?)`
- `createMemorySessionStorage(options?)`
- `sessionMiddleware(options?)`
- `getSessionFromContext(context)`
- `createMemoryAppCacheStore(options?)`
- `createMemoryLoaderCacheStore(options?)`

Cache store types:

- `NeutronCacheStores`
- `NeutronAppCacheStore`
- `NeutronLoaderCacheStore`
- `NeutronAppResponseCacheEntry`
- `NeutronLoaderDataCacheEntry`

## Client APIs

Components:

- `Form`
- `Link`
- `NavLink`
- `Island`
- `Image`
- `ViewTransitions`

Hooks:

- `useLoaderData()`
- `useRouteLoaderData(routeId)`
- `useActionData()`
- `useNavigation()`
- `useNavigate()`
- `useSubmit()`
- `useParams()`
- `useLocation()`
- `useSearchParams()`
- `useRevalidator()`

Navigation:

- `navigate(to, options?)`
- `go(delta)`
- `route(value)` typed route helper.

Typed routing types:

- `RoutePath`
- `RouteHref`
- `NeutronGeneratedRouteMap` (module-augmented by generated `src/routes/.neutron-routes.d.ts`)

Image helpers:

- `defaultImageLoader(args)`
- `ImageLoader`, `ImageLoaderArgs`, `ImageProps`

## SEO Utilities

- `buildMetaTags(metaInput)`
- `renderMetaTags(tags)`
- `mergeSeoMetaInput(base, override)`
- `renderDocumentHead(pathname, seo?, headFragments?)`
- `buildSitemapXml(entries, options?)`
- `buildRobotsTxt(options)`

Types:

- `SeoMetaInput`
- `SeoTag`
- `SitemapEntry`
- `SitemapOptions`
- `RobotsRule`
- `RobotsOptions`

## i18n Routing Utilities

- `resolveLocalePath(pathname, options)`
- `withLocalePath(pathname, locale, options)`
- `stripLocalePrefix(pathname, options)`
- `createI18nMiddleware(options)`

Types:

- `I18nOptions`
- `ResolvedLocalePath`

## Types

- `Route`
- `RouteMatch`
- `RouteConfig`
- `RouteCacheConfig`
- `RouteModule`
- `LoaderArgs`
- `ActionArgs`
- `HeadersArgs`
- `HeadArgs`
- `AppContext`
- `MiddlewareFn`
- `ErrorBoundaryProps`
- `GetStaticPathsResult`

## Optional Enterprise Packages

- `@neutron/cache-redis`: Redis/Dragonfly-backed distributed app/loader cache stores for multi-instance deployments.
- `@neutron/otel`: bridge Neutron server hooks to OpenTelemetry spans and errors.
- `@neutron/auth`: auth context + protected-route middleware with Better Auth/Auth.js style adapters.
- `@neutron/security`: CSP nonce middleware, CSRF middleware, trusted proxy IP resolution, rate-limit middleware, secure cookie defaults.
- `@neutron/ops`: request-id/trace context middleware, health/readiness middleware, JSON structured logging hooks.
