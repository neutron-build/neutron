/**
 * Control flow components for optimized conditional rendering
 * Inspired by SolidJS control flow primitives
 *
 * These components provide better performance and cleaner JSX than
 * standard JavaScript operators for common rendering patterns.
 */

import { h, ComponentChildren, Fragment } from 'preact';
import { useMemo } from 'preact/hooks';

/**
 * Conditional rendering with type narrowing
 * Better than: {condition && <Component />}
 *
 * @example
 * ```tsx
 * <Show when={user} fallback={<Login />}>
 *   {(u) => <Dashboard user={u} />}
 * </Show>
 * ```
 */
export interface ShowProps<T> {
  when: T | null | undefined | false;
  fallback?: ComponentChildren;
  children: (item: NonNullable<T>) => ComponentChildren;
}

/**
 * The return type is deliberately `any` (like Form/Link/Island — see
 * components.tsx): the element is a preact VNode at runtime, which
 * @types/react's ReactNode rejects (TS2786), so a preact-typed return would
 * make `<Show>` unusable from React-typed JSX (react-compat apps compiling
 * with `jsxImportSource: "react"`). `any` typechecks under both JSX runtimes;
 * the props — including the keyed render callback and `fallback` — stay
 * fully typed. See src/client/type-fixtures/react-control-flow/.
 */
export function Show<T>({ when, fallback, children }: ShowProps<T>): any {
  if (when) {
    return <>{children(when as NonNullable<T>)}</>;
  }
  return <>{fallback}</>;
}

/**
 * Optimized list rendering
 * Better than: {items.map(item => ...)}
 * Only re-renders changed items, not entire list
 *
 * @example
 * ```tsx
 * <For each={items}>
 *   {(item, index) => <Item data={item} index={index()} />}
 * </For>
 * ```
 */
export interface ForProps<T> {
  each: readonly T[] | null | undefined;
  fallback?: ComponentChildren;
  children: (item: T, index: () => number) => ComponentChildren;
}

/** JSX-agnostic return type, same as Show above. See react-control-flow/. */
export function For<T>({ each, fallback, children }: ForProps<T>): any {
  if (!each || each.length === 0) {
    return <>{fallback}</>;
  }

  // Optimize by memoizing items with keys. `children` is deliberately NOT a
  // dependency: inline-arrow children get a new identity every render, which
  // would make the memo never hit.
  const items = useMemo(() => {
    return each.map((item, i) => {
      // Create stable index accessor
      const getIndex = () => i;
      return children(item, getIndex);
    });
  }, [each]);

  return <>{items}</>;
}

/**
 * Multi-way conditional rendering
 * Better than: {type === 'a' ? <A /> : type === 'b' ? <B /> : <C />}
 *
 * @example
 * ```tsx
 * <Switch fallback={<NotFound />}>
 *   <Match when={type === 'admin'}><AdminPanel /></Match>
 *   <Match when={type === 'user'}><UserPanel /></Match>
 * </Switch>
 * ```
 */
export interface SwitchProps {
  fallback?: ComponentChildren;
  children: ComponentChildren;
}

/** JSX-agnostic return type, same as Show above. See react-control-flow/. */
export function Switch({ fallback, children }: SwitchProps): any {
  // Find first matching child
  const childArray = Array.isArray(children) ? children : [children];

  for (const child of childArray) {
    if (child && typeof child === 'object' && 'props' in child) {
      const props = child.props as any;
      if (props.when) {
        return <>{child}</>;
      }
    }
  }

  return <>{fallback}</>;
}

/**
 * Match component for use with Switch
 *
 * @example
 * ```tsx
 * <Match when={condition}>
 *   <Component />
 * </Match>
 * ```
 */
export interface MatchProps {
  when: boolean;
  children: ComponentChildren;
}

/** JSX-agnostic return type, same as Show above. See react-control-flow/. */
export function Match({ when, children }: MatchProps): any {
  return when ? <>{children}</> : null;
}

/**
 * Index-optimized list rendering for arrays
 * More efficient than For when order matters and items don't have stable identity
 *
 * @example
 * ```tsx
 * <Index each={items}>
 *   {(item, index) => <div>{index}: {item()}</div>}
 * </Index>
 * ```
 */
export interface IndexProps<T> {
  each: readonly T[] | null | undefined;
  fallback?: ComponentChildren;
  children: (item: () => T, index: number) => ComponentChildren;
}

/** JSX-agnostic return type, same as Show above. See react-control-flow/. */
export function Index<T>({ each, fallback, children }: IndexProps<T>): any {
  if (!each || each.length === 0) {
    return <>{fallback}</>;
  }

  return (
    <>
      {each.map((item, i) => {
        const getItem = () => item;
        return children(getItem, i);
      })}
    </>
  );
}
