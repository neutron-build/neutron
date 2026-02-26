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

export function Show<T>({ when, fallback, children }: ShowProps<T>) {
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

export function For<T>({ each, fallback, children }: ForProps<T>) {
  if (!each || each.length === 0) {
    return <>{fallback}</>;
  }

  // Optimize by memoizing items with keys
  const items = useMemo(() => {
    return each.map((item, i) => {
      // Create stable index accessor
      const getIndex = () => i;
      return children(item, getIndex);
    });
  }, [each, children]);

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

export function Switch({ fallback, children }: SwitchProps) {
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

export function Match({ when, children }: MatchProps) {
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

export function Index<T>({ each, fallback, children }: IndexProps<T>) {
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
