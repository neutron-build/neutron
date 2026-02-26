import { h, FunctionalComponent } from "preact";
import { useState, useCallback, useRef, useEffect } from "preact/hooks";
import { decodeLoaderDataPayload } from "./serialization.js";
import { navigate } from "./navigate.js";
import { setNavigationState } from "./hooks.js";
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

  window.__NEUTRON_PREFETCH_CACHE__ = window.__NEUTRON_PREFETCH_CACHE__ || {};
  if (window.__NEUTRON_PREFETCH_CACHE__[localUrl]) {
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
  window.__NEUTRON_PREFETCH_CACHE__[localUrl] = data;
}

// Keep internal name for backwards compatibility
const prefetchRouteData = prefetch;

export interface FormProps {
  method?: "get" | "post" | "put" | "patch" | "delete";
  action?: string;
  replace?: boolean;
  children?: preact.ComponentChildren;
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
  encType,
  ...props
}) => {
  const formRef = useRef<HTMLFormElement>(null);
  const submittingRef = useRef(false);
  const [isSubmitting, setIsSubmitting] = useState(false);

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
            window.__NEUTRON_PREFETCH_CACHE__ = window.__NEUTRON_PREFETCH_CACHE__ || {};
            window.__NEUTRON_PREFETCH_CACHE__[currentUrl] = data;
            applyClientData(data);
          }
        } else {
          window.dispatchEvent(new PopStateEvent("popstate"));
        }
      } else {
        console.error("Form submission failed:", response.status);
      }
    } catch (error) {
      console.error("Form submission error:", error);
    } finally {
      submittingRef.current = false;
      setIsSubmitting(false);
      setNavigationState({ state: "idle" });
    }
  }, [action, method, replace]);

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
    },
    children
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
        setIsActive(pathname === to || pathname.startsWith(to + "/"));
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
