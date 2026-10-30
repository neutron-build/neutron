import { Island } from "@neutron-build/core/client";
import Counter from "../components/counter.js";

export const config = { mode: "static" };

// <Island> must type-check under React's JSX types (react-compat runtime):
// `pnpm typecheck` runs tsc over this app.
export default function Islands() {
  return (
    <main>
      <h1>Islands</h1>
      <Island component={Counter} client="load" start={3} />
    </main>
  );
}
