import { Island } from "@neutron-build/core/client";
import { Counter } from "../components/Counter";

export const config = { mode: "static" };

export default function Home() {
  return (
    <section>
      <h2>aliased-smoke</h2>
      <p>Neutron runs from an aliased local checkout.</p>
      <Island component={Counter} client="visible" id="aliased-counter" start={1} />
    </section>
  );
}
