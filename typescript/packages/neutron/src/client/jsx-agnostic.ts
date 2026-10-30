/**
 * JSX-agnostic children types for components that must typecheck under BOTH
 * JSX runtimes (preact and react-compat apps typechecking against
 * @types/react with `jsxImportSource: "react"`).
 *
 * Why these exist: preact's `ComponentChildren` includes bare `object`, so it
 * accepts elements from either runtime as *input* — but React's `ReactNode`
 * rejects `object`, so a component typed with `preact.ComponentChildren`
 * fails to *render* its children from React-typed JSX (TS2322). And a
 * React-typed module cannot import `ReactNode` at all in a preact-only
 * install. The types below name the element shape both runtimes produce
 * structurally (`{ type, props, key }` — React's `ReactElement`, preact's
 * `VNode`), so a `children` prop typed `JsxChildren` can be passed elements
 * AND rendered, under either `jsxImportSource`.
 *
 * The framework's own components (`Form`, `Link`, `Island`, …) do not need
 * this for their `children` props — preact's `object` member already admits
 * React-authored elements at the call site, and their internal rendering
 * happens on the preact side. These types are for components that *forward*
 * children in JSX themselves, like the scaffolded root layout. Both worlds'
 * compile contracts are pinned by src/client/type-fixtures/react-nav-form/
 * (react side) and components.test.tsx (preact side).
 */

/**
 * The structural shape of a JSX element from either runtime. React 19's
 * `ReactElement` defaults to `{ type: string | JSXElementConstructor<any>;
 * props: unknown; key: string | null }` and preact's `VNode` is assignable to
 * that (its `type: ComponentType | string` and `key: Key` — both widen), while
 * both accept it back: React into `ReactNode`, preact into `ComponentChild`
 * (an element is an `object`). `type` uses permissive function/class members
 * because preact's component types return `ComponentChildren`, which React's
 * own constructor signatures reject — the structural unions are the only
 * spelling both runtimes round-trip.
 */
export type AnyJsxElement = {
  type: string | ((props: any) => any) | (new (props: any, context?: any) => any);
  props: unknown;
  key: string | null;
};

/** One renderable child: an element from either runtime, or a primitive. */
export type JsxChild =
  | AnyJsxElement
  | string
  | number
  | bigint
  | boolean
  | null
  | undefined;

/**
 * What a `children` prop accepts. Flat arrays mirror preact's own
 * `ComponentChildren = ComponentChild | ComponentChild[]`; React accepts the
 * same shape through `Iterable<ReactNode>`.
 *
 * Deliberately NOT included: bare `object` (preact-only, and what breaks the
 * React render position) and `Promise` children (React-only). Functions used
 * as render props belong on a dedicated prop type, not on `children`.
 */
export type JsxChildren = JsxChild | JsxChild[];
