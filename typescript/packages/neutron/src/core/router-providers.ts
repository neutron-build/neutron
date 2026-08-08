// Mounts the router contexts around a server-composed element.
//
// The client hydrate path has always wrapped the tree in these four providers;
// the server renderers mounted none of them, so every hook that reads them fell
// through to its `createContext` default during SSR. For `useLocation` that
// default is `pathname: "/"` — plausible, wrong on every route but the home
// route, and silent: a layout branching on pathname renders the home branch on
// the server and the correct one after hydration. A-011.
//
// `h` is a PARAMETER rather than an import because the two server renderers do
// not share one. render-app-route.ts uses the statically imported Preact;
// render-static.ts resolves its graph at runtime through importPreactSsr, app
// copy first, precisely so app components and the renderer share one `options`
// object (see core/preact-ssr.ts). Creating these vnodes with the static `h`
// from inside the SSG would reintroduce the split graph that produces the
// `reading '__H'` crash — and a context whose Provider and consumer come from
// different Preact instances silently returns the default, which is the very
// bug being fixed here.
import type * as preact from "preact";

import {
  LoaderContext,
  ActionDataContext,
  NavigationContext,
  RouterContext,
  type LoaderData,
} from "../client/contexts.js";

/** The `h` / `createElement` of whichever Preact graph is doing the rendering. */
export type CreateElement = (
  type: unknown,
  props: Record<string, unknown> | null,
  ...children: unknown[]
) => preact.VNode;

export interface RouterProviderValues {
  routeId: string;
  pathname: string;
  search: string;
  params: Record<string, string>;
  loaderData: LoaderData;
  actionData: unknown;
}

export function withRouterProviders(
  h: CreateElement,
  element: preact.ComponentChildren,
  values: RouterProviderValues
): preact.VNode {
  // Provider order matches client/hydrate.ts deliberately. Nothing currently
  // depends on it, but two orders that drift are a latent difference between
  // the server tree and the tree hydration expects to find.
  return h(
    RouterContext.Provider,
    {
      value: {
        routeId: values.routeId,
        pathname: values.pathname,
        search: values.search,
        params: values.params,
      },
    },
    h(
      LoaderContext.Provider,
      { value: values.loaderData },
      h(
        ActionDataContext.Provider,
        { value: values.actionData },
        // "idle" is the only honest server value: navigation state is a client
        // concept and there is no navigation in flight during SSR.
        h(NavigationContext.Provider, { value: { state: "idle" } }, element)
      )
    )
  );
}
