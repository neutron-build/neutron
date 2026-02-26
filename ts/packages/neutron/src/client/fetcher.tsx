import { h, FunctionalComponent } from "preact";
import { useState, useCallback, useMemo, useRef, useEffect } from "preact/hooks";
import { decodeLoaderDataPayload } from "./serialization.js";
import { navigate } from "./navigate.js";

export interface FetcherState<T = unknown> {
  state: "idle" | "loading" | "submitting";
  data: T | undefined;
  formData: FormData | undefined;
  formAction: string | undefined;
  formMethod: string | undefined;
}

export interface FetcherSubmitOptions {
  action?: string;
  method?: string;
  encType?: string;
}

interface FetcherFormProps {
  action?: string;
  method?: "get" | "post" | "put" | "patch" | "delete";
  children?: preact.ComponentChildren;
  class?: string;
  className?: string;
  id?: string;
  style?: string | Record<string, string>;
  encType?: string;
  onSubmit?: (event: Event) => void;
}

interface FetcherEntry {
  state: FetcherState;
}

const fetcherRegistry = new Map<string, FetcherEntry>();
let fetcherIdCounter = 0;

function notifyFetchersChanged(): void {
  window.dispatchEvent(new CustomEvent("neutron:fetchers-changed"));
}

function toLocalUrl(url: string): string | null {
  const resolved = new URL(url, window.location.href);
  if (resolved.origin !== window.location.origin) {
    return null;
  }
  return resolved.pathname + resolved.search;
}

function isRedirectResult(value: unknown): value is { redirect: string } {
  if (!value || typeof value !== "object") {
    return false;
  }
  return typeof (value as Record<string, unknown>).redirect === "string";
}

function revalidateRouteData(locationHref: string): void {
  const routeIds = window.__NEUTRON_ACTIVE_ROUTE_IDS__;
  const routeIdsHeader =
    Array.isArray(routeIds) && routeIds.length > 0 ? routeIds.join(",") : "";

  void fetch(locationHref, {
    headers: {
      Accept: "application/json",
      "X-Neutron-Data": "true",
      ...(routeIdsHeader ? { "X-Neutron-Routes": routeIdsHeader } : {}),
    },
  })
    .then(async (response) => {
      // Guard: don't apply stale data if user has navigated away
      if (window.location.href !== locationHref) return;
      if (!response.ok) return;
      const contentType = response.headers.get("content-type") || "";
      if (!contentType.includes("application/json")) return;
      const payload = await response.json();
      if (window.location.href !== locationHref) return;
      const data = decodeLoaderDataPayload(payload);
      if (data) {
        window.__NEUTRON_DATA__ = data as Record<string, unknown>;
        window.dispatchEvent(
          new CustomEvent("neutron:data-updated", { detail: data })
        );
      }
    })
    .catch(() => {
      // Revalidation is best-effort; swallow errors.
    });
}

function formDataToSearchParams(formData: FormData): URLSearchParams {
  const params = new URLSearchParams();
  formData.forEach((value, key) => {
    if (typeof value === "string") {
      params.append(key, value);
    }
  });
  return params;
}

export interface Fetcher<T = unknown> extends FetcherState<T> {
  key: string;
  load: (href: string) => void;
  submit: (
    target:
      | HTMLFormElement
      | FormData
      | URLSearchParams
      | Record<string, string>,
    options?: FetcherSubmitOptions
  ) => void;
  Form: FunctionalComponent<FetcherFormProps>;
}

export function useFetcher<T = unknown>(key?: string): Fetcher<T> {
  const [stableId] = useState(() => key || `__fetcher_${++fetcherIdCounter}`);
  const idRef = useRef(stableId);
  const abortRef = useRef<AbortController | null>(null);
  const dataRef = useRef<T | undefined>(undefined);
  const [state, setState] = useState<FetcherState<T>>({
    state: "idle",
    data: undefined,
    formData: undefined,
    formAction: undefined,
    formMethod: undefined,
  });

  // Keep dataRef in sync with state
  dataRef.current = state.data;

  const update = useCallback((next: FetcherState<T>) => {
    setState(next);
    const entry = fetcherRegistry.get(idRef.current);
    if (entry) {
      entry.state = next as FetcherState;
    } else {
      fetcherRegistry.set(idRef.current, {
        state: next as FetcherState,
      });
    }
    notifyFetchersChanged();
  }, []);

  // Register on mount, unregister + abort on unmount
  useEffect(() => {
    const id = idRef.current;
    if (!fetcherRegistry.has(id)) {
      fetcherRegistry.set(id, {
        state: { state: "idle", data: undefined, formData: undefined, formAction: undefined, formMethod: undefined },
      });
    }
    return () => {
      fetcherRegistry.delete(id);
      abortRef.current?.abort();
      notifyFetchersChanged();
    };
  }, []);

  const load = useCallback(
    (href: string) => {
      abortRef.current?.abort();
      const controller = new AbortController();
      abortRef.current = controller;

      update({
        state: "loading",
        data: dataRef.current,
        formData: undefined,
        formAction: undefined,
        formMethod: undefined,
      });

      void (async () => {
        try {
          const response = await fetch(href, {
            headers: {
              Accept: "application/json",
              "X-Neutron-Data": "true",
            },
            signal: controller.signal,
          });

          if (controller.signal.aborted) return;

          if (response.ok) {
            const contentType = response.headers.get("content-type") || "";
            if (contentType.includes("application/json")) {
              const payload = await response.json();
              if (controller.signal.aborted) return;
              const decoded = decodeLoaderDataPayload(payload);
              const routeData = extractLoaderData<T>(decoded);
              update({
                state: "idle",
                data: routeData,
                formData: undefined,
                formAction: undefined,
                formMethod: undefined,
              });
              return;
            }
          }

          update({
            state: "idle",
            data: undefined as T | undefined,
            formData: undefined,
            formAction: undefined,
            formMethod: undefined,
          });
        } catch (error) {
          if ((error as Error).name === "AbortError") return;
          update({
            state: "idle",
            data: undefined as T | undefined,
            formData: undefined,
            formAction: undefined,
            formMethod: undefined,
          });
        }
      })();
    },
    [update]
  );

  const submit = useCallback(
    (
      target:
        | HTMLFormElement
        | FormData
        | URLSearchParams
        | Record<string, string>,
      options: FetcherSubmitOptions = {}
    ) => {
      abortRef.current?.abort();
      const controller = new AbortController();
      abortRef.current = controller;

      let formData: FormData;
      if (target instanceof FormData) {
        formData = target;
      } else if (target instanceof URLSearchParams) {
        formData = new FormData();
        target.forEach((value, key) => formData.set(key, value));
      } else if (target instanceof HTMLFormElement) {
        formData = new FormData(target);
      } else {
        formData = new FormData();
        for (const [key, value] of Object.entries(target)) {
          formData.set(key, value);
        }
      }

      const action = options.action || window.location.pathname;
      const method = (options.method || "post").toUpperCase();

      if (method === "GET") {
        const query = formDataToSearchParams(formData);
        const queryString = query.toString();
        const href = queryString ? `${action}?${queryString}` : action;

        update({
          state: "loading",
          data: dataRef.current,
          formData,
          formAction: action,
          formMethod: method,
        });

        void (async () => {
          try {
            const response = await fetch(href, {
              headers: {
                Accept: "application/json",
                "X-Neutron-Data": "true",
              },
              signal: controller.signal,
            });

            if (controller.signal.aborted) return;

            if (response.ok) {
              const contentType = response.headers.get("content-type") || "";
              if (contentType.includes("application/json")) {
                const payload = await response.json();
                if (controller.signal.aborted) return;
                const decoded = decodeLoaderDataPayload(payload);
                const routeData = extractLoaderData<T>(decoded);
                update({
                  state: "idle",
                  data: routeData,
                  formData: undefined,
                  formAction: undefined,
                  formMethod: undefined,
                });
                return;
              }
            }

            update({
              state: "idle",
              data: undefined as T | undefined,
              formData: undefined,
              formAction: undefined,
              formMethod: undefined,
            });
          } catch (error) {
            if ((error as Error).name === "AbortError") return;
            update({
              state: "idle",
              data: undefined as T | undefined,
              formData: undefined,
              formAction: undefined,
              formMethod: undefined,
            });
          }
        })();

        return;
      }

      // Mutation (POST/PUT/PATCH/DELETE)
      update({
        state: "submitting",
        data: dataRef.current,
        formData,
        formAction: action,
        formMethod: method,
      });

      const locationAtSubmit = window.location.href;

      void (async () => {
        try {
          const response = await fetch(action, {
            method,
            body: formData,
            headers: {
              Accept: "application/json",
              "X-Neutron-Data": "true",
            },
            signal: controller.signal,
            redirect: "follow",
          });

          if (controller.signal.aborted) return;

          if (response.redirected) {
            const localUrl = toLocalUrl(response.url);
            if (localUrl) {
              navigate(localUrl);
            } else {
              window.location.href = response.url;
            }
            update({
              state: "idle",
              data: undefined as T | undefined,
              formData: undefined,
              formAction: undefined,
              formMethod: undefined,
            });
            return;
          }

          if (response.ok) {
            const contentType = response.headers.get("content-type") || "";
            if (contentType.includes("application/json")) {
              const payload = await response.json();
              if (controller.signal.aborted) return;
              const decoded = decodeLoaderDataPayload(payload);

              if (isRedirectResult(decoded)) {
                const localUrl = toLocalUrl(decoded.redirect);
                if (localUrl) {
                  navigate(localUrl);
                } else {
                  window.location.href = decoded.redirect;
                }
                update({
                  state: "idle",
                  data: undefined as T | undefined,
                  formData: undefined,
                  formAction: undefined,
                  formMethod: undefined,
                });
                return;
              }

              // Extract action data if present
              const actionData = extractActionData<T>(decoded);

              update({
                state: "idle",
                data: actionData,
                formData: undefined,
                formAction: undefined,
                formMethod: undefined,
              });

              // Revalidate page data after mutation (guarded against navigation)
              revalidateRouteData(locationAtSubmit);
              return;
            }
          }

          update({
            state: "idle",
            data: undefined as T | undefined,
            formData: undefined,
            formAction: undefined,
            formMethod: undefined,
          });
        } catch (error) {
          if ((error as Error).name === "AbortError") return;
          update({
            state: "idle",
            data: undefined as T | undefined,
            formData: undefined,
            formAction: undefined,
            formMethod: undefined,
          });
        }
      })();
    },
    [update]
  );

  // Stable ref so FetcherForm never needs to re-create
  const submitRef = useRef(submit);
  submitRef.current = submit;

  // Stable component identity — never remounts the <form> DOM
  const FetcherForm = useMemo<FunctionalComponent<FetcherFormProps>>(
    () =>
      ({ method = "post", action, children, encType, onSubmit, ...props }) => {
        const handleSubmit = (event: Event) => {
          event.preventDefault();
          if (onSubmit) onSubmit(event);
          const form = event.currentTarget as HTMLFormElement;
          submitRef.current(form, { action, method, encType });
        };

        return h(
          "form",
          {
            ...props,
            method,
            action,
            encType,
            onSubmit: handleSubmit,
          },
          children
        );
      },
    []
  );

  return {
    ...state,
    key: stableId,
    load,
    submit,
    Form: FetcherForm,
  };
}

export function useFetchers(): FetcherState[] {
  const [, setVersion] = useState(0);

  useEffect(() => {
    const handler = () => setVersion((v) => v + 1);
    window.addEventListener("neutron:fetchers-changed", handler);
    return () =>
      window.removeEventListener("neutron:fetchers-changed", handler);
  }, []);

  const result: FetcherState[] = [];
  for (const entry of fetcherRegistry.values()) {
    result.push(entry.state);
  }
  return result;
}

/**
 * Extract loader data from a decoded JSON payload (for fetcher.load).
 * If the payload has a single route key, unwrap it. Otherwise return as-is.
 */
function extractLoaderData<T>(decoded: unknown): T | undefined {
  if (decoded == null || typeof decoded !== "object") {
    return decoded as T | undefined;
  }

  const record = decoded as Record<string, unknown>;
  const keys = Object.keys(record);

  // Single route's data — unwrap for convenience
  if (keys.length === 1) {
    return record[keys[0]] as T;
  }

  return decoded as T;
}

/**
 * Extract action data from a decoded JSON payload (for fetcher.submit).
 * Prefers __action__ if present, falls back to full payload.
 */
function extractActionData<T>(decoded: unknown): T | undefined {
  if (decoded == null || typeof decoded !== "object") {
    return decoded as T | undefined;
  }

  const record = decoded as Record<string, unknown>;

  if (record.__action__ !== undefined) {
    return record.__action__ as T;
  }

  return decoded as T;
}
