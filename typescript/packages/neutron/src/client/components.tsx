import { h, FunctionalComponent } from "preact";
import { useState, useCallback, useRef, useEffect } from "preact/hooks";
import { decodeLoaderDataPayload } from "./serialization.js";
import { navigate } from "./navigate.js";
import { setNavigationState } from "./hooks.js";
import type { SubmitError } from "./hooks.js";
import { hasFreshPrefetch, storePrefetch } from "./prefetch-cache.js";
import type { RouteHref } from "../core/typed-routes.js";

function toLocalUrl(url: string): string | null {
  const resolved = new URL(url, window.location.href);
  if (resolved.origin !== window.location.origin) {
    return null;
  }
  return resolved.pathname + resolved.search;
}

function resolveRelativeAction(action: string | undefined, currentPath: string): string {
  if (!action) return currentPath;

  // Absolute path - use as-is
  if (action.startsWith('/')) return action;

  // Relative action (".", "..", "../..", etc.)
  if (action === '.') return currentPath;

  const segments = currentPath.split('/').filter(Boolean);

  if (action === '..') {
    segments.pop();
    return '/' + segments.join('/');
  }

  if (action.startsWith('../')) {
    const upCount = action.match(/\.\.\//g)?.length || 0;
    for (let i = 0; i < upCount; i++) {
      segments.pop();
    }
    const remainder = action.replace(/\.\.\//g, '');
    if (remainder) segments.push(remainder);
    return '/' + segments.join('/');
  }

  // Relative path without ".." (append to current)
  return currentPath.endsWith('/') ? currentPath + action : currentPath + '/' + action;
}

function applyClientData(data: unknown): void {
  window.__NEUTRON_DATA__ = data as Record<string, unknown>;
  window.dispatchEvent(new CustomEvent("neutron:data-updated", { detail: data }));
}

export async function prefetch(to: string): Promise<void> {
  const localUrl = toLocalUrl(to);
  if (!localUrl) return;

  if (hasFreshPrefetch(localUrl)) {
    return;
  }

  const response = await fetch(localUrl, {
    headers: {
      Accept: "application/json",
      "X-Neutron-Data": "true",
    },
  });

  if (!response.ok) return;

  const contentType = response.headers.get("content-type") || "";
  if (!contentType.includes("application/json")) return;

  const payload = await response.json();
  const data = decodeLoaderDataPayload(payload);
  // Warm the one cache navigation reads (takePrefetch). Writing only the raw
  // window global made every warmed payload unconsumed — a double fetch.
  storePrefetch(localUrl, data);
}

// Keep internal name for backwards compatibility
const prefetchRouteData = prefetch;

/** What a function-children `Form` receives: the live submit state. */
export interface FormRenderState {
  submitting: boolean;
  error: SubmitError | null;
}

export interface FormProps {
  method?: "get" | "post" | "put" | "patch" | "delete";
  action?: string;
  replace?: boolean;
  /** Plain children render as-is; a function receives the live submit state
   *  (`submitting`, `error`) so failures are observable in UI. */
  children?: preact.ComponentChildren | ((state: FormRenderState) => preact.ComponentChildren);
  /** Called when a submission fails (non-ok response or network error) —
   *  the observable error surface for plain-children forms. */
  onError?: (error: SubmitError) => void;
  class?: string;
  className?: string;
  id?: string;
  style?: string | Record<string, string>;
  encType?: string;
  onSubmit?: (event: Event) => void;
}

export const Form: FunctionalComponent<FormProps> = ({
  method = "post",
  action,
  replace,
  children,
  onError,
  encType,
  ...props
}) => {
  const formRef = useRef<HTMLFormElement>(null);
  const submittingRef = useRef(false);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<SubmitError | null>(null);

  const handleSubmit = useCallback(async (event: Event) => {
    if (!(window as any).__NEUTRON_ROUTER_ACTIVE__) {
      return; // Let browser submit the form natively
    }
    event.preventDefault();

    if (submittingRef.current) return;

    const form = event.currentTarget as HTMLFormElement;
    const formData = new FormData(form);
    const rawAction = action || form.getAttribute('action') || undefined;
    const formAction = resolveRelativeAction(rawAction, window.location.pathname);
    const formMethod = method.toUpperCase();

    if (formMethod === "GET") {
      const query = new URLSearchParams();
      formData.forEach((value, key) => {
        if (typeof value === 'string') query.append(key, value);
      });
      const queryString = query.toString();
      const destination = queryString ? `${formAction}?${queryString}` : formAction;
      navigate(destination);
      return;
    }

    submittingRef.current = true;
    setIsSubmitting(true);
    setError(null);
    setNavigationState({
      state: "submitting",
      formData,
      formAction,
      formMethod,
      location: window.location.pathname + window.location.search,
    });

    try {
      const response = await fetch(formAction, {
        method: formMethod,
        body: formData,
        headers: {
          Accept: "application/json",
          "X-Neutron-Data": "true",
          "X-Neutron-Routes":
            (window.__NEUTRON_ACTIVE_ROUTE_IDS__ || []).join(","),
        },
        redirect: "follow",
      });

      if (response.redirected) {
        const localUrl = toLocalUrl(response.url);
          if (localUrl) {
            if (replace) {
              window.history.replaceState(null, "", localUrl);
            } else {
              window.history.pushState(null, "", localUrl);
            }
            window.dispatchEvent(new PopStateEvent("popstate"));
        } else {
          window.location.href = response.url;
        }
      } else if (response.ok) {
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
              if (replace) {
                window.history.replaceState(null, "", localUrl);
                window.dispatchEvent(new PopStateEvent("popstate"));
              } else {
                navigate(localUrl);
              }
            } else {
              window.location.href = data.redirect;
            }
          } else {
            const currentUrl = window.location.pathname + window.location.search;
            // Same contract as useSubmit: post-mutation payloads go through
            // the expiring prefetch cache, never the raw global (no TTL).
            storePrefetch(currentUrl, data);
            applyClientData(data);
          }
        } else {
          window.dispatchEvent(new PopStateEvent("popstate"));
        }
      } else {
        const submitError: SubmitError = {
          status: response.status,
          message:
            response.statusText || `request failed with status ${response.status}`,
        };
        setError(submitError);
        onError?.(submitError);
        console.error("Form submission failed:", response.status);
      }
    } catch (err) {
      const submitError: SubmitError = {
        status: 0,
        message: err instanceof Error ? err.message : String(err),
      };
      setError(submitError);
      onError?.(submitError);
      console.error("Form submission error:", err);
    } finally {
      submittingRef.current = false;
      setIsSubmitting(false);
      setNavigationState({ state: "idle" });
    }
  }, [action, method, replace, onError]);

  return h(
    "form",
    {
      ...props,
      ref: formRef,
      method,
      action,
      encType,
      onSubmit: handleSubmit,
      "data-submitting": isSubmitting || undefined,
      "data-submit-error": error ? String(error.status) : undefined,
    },
    typeof children === "function"
      ? (children as (state: FormRenderState) => preact.ComponentChildren)({
          submitting: isSubmitting,
          error,
        })
      : children
  );
};

export interface LinkProps {
  to: RouteHref;
  replace?: boolean;
  prefetch?: "none" | "intent" | "render";
  children?: preact.ComponentChildren;
  class?: string;
  className?: string;
  style?: string | Record<string, string>;
  target?: string;
}

export const Link: FunctionalComponent<LinkProps> = ({ 
  to, 
  replace, 
  prefetch = "none",
  target,
  children, 
  ...props 
}) => {
  const linkRef = useRef<HTMLAnchorElement>(null);
  const [prefetched, setPrefetched] = useState(false);

  const prefetchPage = useCallback(() => {
    if (prefetched || prefetch === "none") return;
    
    const link = document.createElement("link");
    link.rel = "prefetch";
    link.href = to;
    link.as = "document";
    document.head.appendChild(link);

    void prefetchRouteData(to).catch(() => {
      // Ignore prefetch failures; navigation will fetch fresh data.
    });

    setPrefetched(true);
  }, [to, prefetch, prefetched]);

  const handleClick = useCallback((event: MouseEvent) => {
    const localUrl = toLocalUrl(to);

    const shouldNavigate = 
      !event.defaultPrevented &&
      event.button === 0 &&
      !!localUrl &&
      (!target || target === "_self") &&
      !(event.metaKey || event.altKey || event.ctrlKey || event.shiftKey);

    if (shouldNavigate) {
      event.preventDefault();
      
      if (replace) {
        window.history.replaceState(null, "", localUrl!);
        window.dispatchEvent(new PopStateEvent("popstate"));
      } else {
        navigate(localUrl!);
      }
    }
  }, [to, replace, target]);

  const handleMouseEnter = useCallback(() => {
    if (prefetch === "intent") {
      prefetchPage();
    }
  }, [prefetch, prefetchPage]);

  useEffect(() => {
    if (prefetch === "render" && linkRef.current) {
      const observer = new IntersectionObserver(([entry]) => {
        if (entry.isIntersecting) {
          prefetchPage();
          observer.disconnect();
        }
      });
      observer.observe(linkRef.current);
      return () => observer.disconnect();
    }
  }, [prefetch, prefetchPage]);

  return h(
    "a",
    {
      ...props,
      ref: linkRef,
      href: to,
      target,
      onClick: handleClick,
      onMouseEnter: handleMouseEnter,
    },
    children
  );
};

export interface NavLinkProps extends LinkProps {
  activeClass?: string;
  activeStyle?: Record<string, string>;
  end?: boolean;
}

export const NavLink: FunctionalComponent<NavLinkProps> = ({ 
  to, 
  activeClass,
  activeStyle,
  end,
  children,
  ...props 
}) => {
  const [isActive, setIsActive] = useState(false);

  useEffect(() => {
    const checkActive = () => {
      const pathname = window.location.pathname;
      if (end) {
        setIsActive(pathname === to);
      } else {
        // "/" is the prefix of every path: `to + "/"` is "//", which no
        // pathname starts with — home would never be active on subpaths.
        setIsActive(pathname === to || to === "/" || pathname.startsWith(to + "/"));
      }
    };

    checkActive();
    window.addEventListener("popstate", checkActive);
    return () => window.removeEventListener("popstate", checkActive);
  }, [to, end]);

  const className = [
    props.class || props.className,
    isActive ? activeClass : null,
  ].filter(Boolean).join(" ") || undefined;

  const style = {
    ...(typeof props.style === "object" ? props.style : {}),
    ...(isActive ? activeStyle : {}),
  };

  return h(
    Link,
    {
      ...props,
      to,
      class: className,
      style: Object.keys(style).length > 0 ? style : props.style,
    },
    children
  );
};

function isRedirectResult(value: unknown): value is { redirect: string } {
  if (!value || typeof value !== "object") {
    return false;
  }
  const candidate = value as Record<string, unknown>;
  return typeof candidate.redirect === "string";
}
