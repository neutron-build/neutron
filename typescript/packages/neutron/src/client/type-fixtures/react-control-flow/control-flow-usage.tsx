/**
 * Fail-before fixture: control-flow components used from REACT-typed JSX.
 *
 * A react-compat app typechecks against @types/react
 * (`jsxImportSource: "react"`) while the runtime stays preact/compat. This
 * module is what such an app's code looks like from that side — React-authored
 * JSX (hooks + elements) hosting <Show>, <For>, <Switch>, <Match> and
 * <Index> with keyed render callbacks, fallbacks, `key` attributes and
 * Switch/Match composition, the shapes role-panel and list pages use.
 *
 * Note the import path: unlike Form/Link/Island (exported from
 * @neutron-build/core/client), the control-flow family is exported from the
 * package ROOT entry, @neutron-build/core.
 *
 * Before the JSX-agnostic return fix, every usage below failed TS2786
 * ('Show' cannot be used as a JSX component — its preact VNode return is not
 * a ReactNode). After it, this module compiles clean AND the @ts-expect-error
 * pins below still error — the `any` element return did not turn off prop
 * checking under the react import source.
 *
 * Compiled only by controlflow.react-types.test.ts with the tsconfig in this
 * directory; deliberately excluded from the package build (which is
 * jsxImportSource: preact).
 */
import { useState } from "react";
import { Show, For, Switch, Match, Index } from "@neutron-build/core";

interface User {
  id: number;
  name: string;
}

function Dashboard(props: { user: User }) {
  return <p>{props.user.name}</p>;
}

/** Show with the keyed render callback: `when` narrows T for the child. */
export function ShowKeyed(props: { user: User | null }) {
  return (
    <Show when={props.user} fallback={<p>Anonymous</p>}>
      {(user) => <Dashboard user={user} />}
    </Show>
  );
}

/** Show toggled by React state, the migration-port shape of `cond && <X/>`. */
export function ShowToggled() {
  const [open, setOpen] = useState(true);
  return (
    <div>
      <button onClick={() => setOpen(!open)}>toggle</button>
      <Show when={open} fallback={<p>closed</p>}>
        {() => <p>open</p>}
      </Show>
    </div>
  );
}

/** For with the item/index render callback and keyed rows. */
export function ForRows(props: { items: Array<{ id: number; label: string }> }) {
  return (
    <ul>
      <For each={props.items} fallback={<li>Nothing yet</li>}>
        {(item, index) => (
          <li key={item.id}>
            {index()}: {item.label}
          </li>
        )}
      </For>
    </ul>
  );
}

/** Switch/Match: first matching Match wins, fallback otherwise. */
export function RolePanel(props: { role: "admin" | "user" | "guest" }) {
  return (
    <Switch fallback={<p>Guest view</p>}>
      <Match when={props.role === "admin"}>
        <p>Admin panel</p>
      </Match>
      <Match when={props.role === "user"}>
        <p>User panel</p>
      </Match>
    </Switch>
  );
}

/** Index: accessor-style item plus numeric index, keyed rows. */
export function IndexRows(props: { words: string[] }) {
  return (
    <ol>
      <Index each={props.words}>
        {(word, i) => (
          <li key={i}>
            {i}: {word()}
          </li>
        )}
      </Index>
    </ol>
  );
}

/**
 * Props stay fully typed under this import source too: each pin below must
 * keep erroring (an unused @ts-expect-error fails this fixture's compile), so
 * the `any` element return cannot silently weaken prop checking.
 */
export function ControlFlowNegatives() {
  return (
    <div>
      {/* @ts-expect-error `each` must be an array, not a number */}
      <For each={42}>{() => null}</For>
      {/* @ts-expect-error Show children must be the keyed render callback */}
      <Show when={1}>plain</Show>
      {/* @ts-expect-error `when` on Match must be boolean */}
      <Match when="yes">x</Match>
    </div>
  );
}
