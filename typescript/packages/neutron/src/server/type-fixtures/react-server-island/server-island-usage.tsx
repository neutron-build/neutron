/**
 * Fail-before fixture: <ServerIsland> used from REACT-typed JSX.
 *
 * A react-compat app typechecks against @types/react
 * (`jsxImportSource: "react"`) while the runtime stays preact/compat. This
 * module is what such an app's code looks like from that side — React-authored
 * JSX hosting the server-side deferred-rendering island with an explicit id,
 * a fallback, and the default-loading shapes a page shell uses.
 *
 * Note the import path: like the control-flow family, ServerIsland is
 * exported from the package ROOT entry, @neutron-build/core (it is a
 * server-side component; there is no client-entry export of it).
 *
 * Before the JSX-agnostic return fix, every usage below failed TS2786
 * ('ServerIsland' cannot be used as a JSX component — its preact VNode
 * return is not a ReactNode). After it, this module compiles clean AND the
 * @ts-expect-error pins below still error — the `any` element return did not
 * turn off prop checking under the react import source.
 *
 * Compiled only by server-islands.react-types.test.ts with the tsconfig in
 * this directory; deliberately excluded from the package build (which is
 * jsxImportSource: preact).
 */
import { ServerIsland } from "@neutron-build/core";

function CartContents(props: { items: Array<{ sku: string; label: string }> }) {
  return (
    <ul>
      {props.items.map((item) => (
        <li key={item.sku}>{item.label}</li>
      ))}
    </ul>
  );
}

/** The headline shape: a slow server widget deferred behind a fallback. */
export function CartPage(props: { items: Array<{ sku: string; label: string }> }) {
  return (
    <main>
      <h1>Your cart</h1>
      <ServerIsland id="cart" fallback={<p role="status">Loading cart…</p>}>
        <CartContents items={props.items} />
      </ServerIsland>
    </main>
  );
}

/** Defaults: no id (one is generated), no fallback (generic loader). */
export function DeferredTrending() {
  return (
    <ServerIsland>
      <section data-trending>Slow feed widget</section>
    </ServerIsland>
  );
}

/**
 * Props stay fully typed under this import source too: each pin below must
 * keep erroring (an unused @ts-expect-error fails this fixture's compile), so
 * the `any` element return cannot silently weaken prop checking.
 */
export function ServerIslandNegatives() {
  return (
    <div>
      {/* @ts-expect-error `id` must be a string, not a number */}
      <ServerIsland id={42}>x</ServerIsland>
      {/* @ts-expect-error `children` is required */}
      <ServerIsland fallback={<p>loading</p>} />
    </div>
  );
}
