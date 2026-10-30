/**
 * Fail-before fixture: `<Island>` used from REACT-typed JSX.
 *
 * A react-compat app typechecks against @types/react
 * (`jsxImportSource: "react"`) while the runtime stays preact/compat. The
 * framework's client components are authored against preact's types; this
 * module is what an app's island host looks like from that side — a
 * React-authored component (hooks + JSX), passed to `<Island>` with children.
 *
 * Compiled only by island.react-types.test.ts with the tsconfig in this
 * directory; deliberately excluded from the package build (which is
 * jsxImportSource: preact).
 */
import { useState } from "react";
import { Island } from "@neutron-build/core/client";

function ReactCounter(props: { start?: number; label?: string }) {
  const [count, setCount] = useState(props.start ?? 0);
  return (
    <button type="button" onClick={() => setCount(count + 1)}>
      {props.label ?? "Count"}: {count}
    </button>
  );
}

export function IslandHost() {
  return (
    <Island component={ReactCounter} client="load" start={3} label="Hits">
      <p>Fallback content until the island hydrates.</p>
    </Island>
  );
}
