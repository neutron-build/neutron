// The router contexts, defined apart from the hooks that read them.
//
// Both the client hydrate path and the server renderer must mount these, and a
// context only matches a consumer by object identity — two `createContext`
// calls produce two unrelated contexts. Keeping them here means the server can
// import the contexts alone: `hooks.ts` pulls in `navigate.js` and the prefetch
// cache, which the render path has no use for.
//
// Their defaults are deliberately plausible-looking, which is why the server
// MUST mount a provider: a component that reads `pathname` outside one gets
// "/" and renders the wrong branch rather than failing. See A-011 in
// docs/ADOPTION_FINDINGS.md.
import { createContext } from "preact";

export interface LoaderData {
  [routeId: string]: unknown;
}

export interface NavigationState {
  state: "idle" | "loading" | "submitting";
  formData?: FormData;
  formAction?: string;
  formMethod?: string;
  location?: string;
}

export interface RouterState {
  routeId: string;
  pathname: string;
  search: string;
  params: Record<string, string>;
}

export interface UIMatch {
  id: string;
  pathname: string;
  params: Record<string, string>;
  data: unknown;
  handle?: unknown;
}

export const LoaderContext = createContext<LoaderData>({});
export const ActionDataContext = createContext<unknown>(undefined);
export const NavigationContext = createContext<NavigationState>({ state: "idle" });
export const RouterContext = createContext<RouterState>({
  routeId: "",
  pathname: "/",
  search: "",
  params: {},
});
export const MatchesContext = createContext<UIMatch[]>([]);
