import { createContext } from "preact";
import { useContext, useCallback, useMemo, useState, useEffect, useRef } from "preact/hooks";
import { decodeLoaderDataPayload } from "./serialization.js";
import { go, navigate } from "./navigate.js";
import type { RouteHref } from "../core/typed-routes.js";
import type { SerializeFrom } from "../core/types.js";

function toLocalUrl(url: string): string | null {
  const resolved = new URL(url, window.location.href);
  if (resolved.origin !== window.location.origin) {
    return null;
  }
  return resolved.pathname + resolved.search;
}

function applyClientData(data: unknown): void {
  window.__NEUTRON_DATA__ = data as LoaderData;
  window.dispatchEvent(new CustomEvent("neutron:data-updated", { detail: data }));
}

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

const LoaderContext = createContext<LoaderData>({});
const ActionDataContext = createContext<unknown>(undefined);
const NavigationContext = createContext<NavigationState>({ state: "idle" });
const RouterContext = createContext<RouterState>({
  routeId: "",
  pathname: "/",
  search: "",
  params: {},
});
const MatchesContext = createContext<UIMatch[]>([]);

export function useLoaderData<T = unknown>(): SerializeFrom<T> {
  const data = useContext(LoaderContext);
  const router = useContext(RouterContext);
  return (data[router.routeId] || data.page) as SerializeFrom<T>;
}

export function useRouteLoaderData<T = unknown>(routeId: string): SerializeFrom<T> | undefined {
  const data = useContext(LoaderContext);
  return data[routeId] as SerializeFrom<T> | undefined;
}

export function useActionData<T = unknown>(): SerializeFrom<T> | undefined {
  return useContext(ActionDataContext) as SerializeFrom<T> | undefined;
}

export function useNavigation(): NavigationState {
  const contextState = useContext(NavigationContext);
  const [state, setState] = useState<NavigationState>(() => readNavigationState());

  useEffect(() => {
    const handleNavigation = (event: Event) => {
      const detail = (event as CustomEvent<NavigationState>).detail;
      if (detail) {
        setState(detail);
      } else {
        setState(readNavigationState());
      }
    };

    window.addEventListener("neutron:navigation", handleNavigation);
    return () => {
      window.removeEventListener("neutron:navigation", handleNavigation);
    };
  }, []);

  if (state.state !== "idle") {
    return state;
  }
  return contextState;
}

export function useParams(): Record<string, string> {
  const router = useContext(RouterContext);
  return router.params;
}

export function useLocation(): { pathname: string; search: string } {
  const router = useContext(RouterContext);
  return { pathname: router.pathname, search: router.search };
}

export function useSearchParams(): [URLSearchParams, (params: URLSearchParams) => void] {
  const router = useContext(RouterContext);
  
  const searchParams = useMemo(() => new URLSearchParams(router.search), [router.search]);
  
  const setSearchParams = useCallback((params: URLSearchParams) => {
    const search = params.toString();
    const path = window.location.pathname;
    const url = search ? `${path}?${search}` : path;
    navigate(url);
  }, []);
  
  return [searchParams, setSearchParams];
}

export function useNavigate() {
  return useCallback((to: RouteHref | number) => {
    if (typeof to === "number") {
      go(to);
    } else {
      navigate(to);
    }
  }, []);
}

export function useRevalidator() {
  const [state, setState] = useState<"idle" | "loading">("idle");
  const requestIdRef = useRef(0);
  
  const revalidate = useCallback(async () => {
    const requestId = ++requestIdRef.current;
    setState("loading");
    setNavigationState({
      state: "loading",
      location: window.location.pathname + window.location.search,
    });

    try {
      const response = await fetch(window.location.href, {
        headers: {
          Accept: "application/json",
          "X-Neutron-Data": "true",
          "X-Neutron-Routes": getActiveRouteIdsHeader(),
        },
      });
      if (response.ok) {
        const payload = await response.json();
        const data = decodeLoaderDataPayload(payload);
        if (data && requestId === requestIdRef.current) {
          window.__NEUTRON_DATA__ = data;
          window.dispatchEvent(new CustomEvent("neutron:data-updated", { detail: data }));
        }
      }
    } finally {
      if (requestId === requestIdRef.current) {
        setState("idle");
        setNavigationState({ state: "idle" });
      }
    }
  }, []);
  
  return { revalidate, state };
}

export function useSubmit() {
  return useCallback(
    async (
      form: HTMLFormElement | FormData,
      options: { action?: string; method?: string } = {}
    ) => {
      const formData = form instanceof FormData ? form : new FormData(form);
      const action = options.action || window.location.pathname;
      const method = (options.method || "post").toUpperCase();

      if (method === "GET") {
        const query = new URLSearchParams();
        formData.forEach((value, key) => {
          if (typeof value === 'string') query.append(key, value);
        });
        const queryString = query.toString();
        const destination = queryString ? `${action}?${queryString}` : action;
        navigate(destination);
        return;
      }

      setNavigationState({
        state: "submitting",
        formData,
        formAction: action,
        formMethod: method,
        location: window.location.pathname + window.location.search,
      });

      try {
        const response = await fetch(action, {
          method,
          body: formData,
          headers: {
            Accept: "application/json",
            "X-Neutron-Data": "true",
            "X-Neutron-Routes": getActiveRouteIdsHeader(),
          },
          redirect: "follow",
        });

        if (response.redirected) {
          const localUrl = toLocalUrl(response.url);
          if (localUrl) {
            navigate(localUrl);
          } else {
            window.location.href = response.url;
          }
          return;
        }

        if (!response.ok) {
          return;
        }

        setNavigationState({
          state: "loading",
          location: window.location.pathname + window.location.search,
        });

        const contentType = response.headers.get("content-type");
        if (contentType?.includes("application/json")) {
          const payload = await response.json();
          const data = decodeLoaderDataPayload(payload);
          if (isRedirectResult(data)) {
            const localUrl = toLocalUrl(data.redirect);
            if (localUrl) {
              navigate(localUrl);
            } else {
              window.location.href = data.redirect;
            }
            return;
          }

          const currentUrl = window.location.pathname + window.location.search;
          window.__NEUTRON_PREFETCH_CACHE__ = window.__NEUTRON_PREFETCH_CACHE__ || {};
          window.__NEUTRON_PREFETCH_CACHE__[currentUrl] = data;
          applyClientData(data);
        } else {
          window.dispatchEvent(new PopStateEvent("popstate"));
        }
      } finally {
        setNavigationState({ state: "idle" });
      }
    },
    []
  );
}

export interface SubmitOptions {
  action?: RouteHref;
  method?: string;
}

export { 
  LoaderContext, 
  ActionDataContext, 
  NavigationContext, 
  RouterContext 
};

export function setNavigationState(next: NavigationState): void {
  window.__NEUTRON_NAVIGATION_STATE__ = next;
  window.dispatchEvent(new CustomEvent("neutron:navigation", { detail: next }));
}

function readNavigationState(): NavigationState {
  return window.__NEUTRON_NAVIGATION_STATE__ || { state: "idle" };
}

function getActiveRouteIdsHeader(): string {
  const routeIds = window.__NEUTRON_ACTIVE_ROUTE_IDS__;
  if (!Array.isArray(routeIds) || routeIds.length === 0) {
    return "";
  }
  return routeIds.join(",");
}

function isRedirectResult(value: unknown): value is { redirect: string } {
  if (!value || typeof value !== "object") {
    return false;
  }
  const candidate = value as Record<string, unknown>;
  return typeof candidate.redirect === "string";
}

export function useMatches(): UIMatch[] {
  return useContext(MatchesContext);
}

export function useBeforeUnload(
  callback: (event: BeforeUnloadEvent) => void,
  options?: { capture?: boolean }
): void {
  useEffect(() => {
    const opts = { capture: options?.capture ?? false };
    window.addEventListener("beforeunload", callback, opts);
    return () => window.removeEventListener("beforeunload", callback, opts);
  }, [callback, options?.capture]);
}

export interface BlockerState {
  state: "blocked" | "proceeding" | "idle";
  location?: { pathname: string; search: string };
  proceed: () => void;
  reset: () => void;
}

export function useBlocker(
  shouldBlock: boolean | ((args: {
    currentLocation: { pathname: string; search: string; hash: string; href: string };
    nextLocation: { pathname: string; search: string; hash: string; href: string };
  }) => boolean)
): BlockerState {
  const [state, setState] = useState<"blocked" | "proceeding" | "idle">("idle");
  const [blockedLocation, setBlockedLocation] = useState<{ pathname: string; search: string } | undefined>();
  const blockerIdRef = useRef<number | null>(null);

  useEffect(() => {
    const { registerBlocker, unregisterBlocker } = require('./navigate.js');

    if (shouldBlock) {
      const id = registerBlocker(shouldBlock);
      blockerIdRef.current = id;

      const handleBlocked = (event: Event) => {
        const detail = (event as CustomEvent).detail;
        setState("blocked");
        setBlockedLocation({ pathname: detail.to.split('?')[0], search: detail.to.includes('?') ? '?' + detail.to.split('?')[1] : '' });
      };

      window.addEventListener('neutron:navigation-blocked', handleBlocked);

      return () => {
        window.removeEventListener('neutron:navigation-blocked', handleBlocked);
        unregisterBlocker(id);
        blockerIdRef.current = null;
      };
    } else {
      if (blockerIdRef.current !== null) {
        unregisterBlocker(blockerIdRef.current);
        blockerIdRef.current = null;
      }
    }
  }, [shouldBlock]);

  const proceed = useCallback(() => {
    if (blockedLocation && blockerIdRef.current !== null) {
      const { unregisterBlocker } = require('./navigate.js');
      const { navigate } = require('./navigate.js');
      unregisterBlocker(blockerIdRef.current);
      blockerIdRef.current = null;
      setState("proceeding");
      navigate(blockedLocation.pathname + blockedLocation.search);
      setState("idle");
      setBlockedLocation(undefined);
    }
  }, [blockedLocation]);

  const reset = useCallback(() => {
    setState("idle");
    setBlockedLocation(undefined);
  }, []);

  return { state, location: blockedLocation, proceed, reset };
}

declare global {
  interface Window {
    __NEUTRON_DATA__?: LoaderData;
    __NEUTRON_DATA_SERIALIZED__?: string;
    __NEUTRON_PREFETCH_CACHE__?: Record<string, LoaderData>;
    __NEUTRON_ROUTE__?: string;
    __NEUTRON_VIEW_TRANSITIONS__?: boolean;
    __NEUTRON_ROUTER_ACTIVE__?: boolean;
    __NEUTRON_NAVIGATION_STATE__?: NavigationState;
    __NEUTRON_ACTIVE_ROUTE_IDS__?: string[];
    __NEUTRON_MATCHES__?: UIMatch[];
  }
}
