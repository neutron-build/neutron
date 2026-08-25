// @vitest-environment happy-dom
import { describe, it, expect } from "vitest";
import { h, render } from "preact";
import { For } from "./control-flow.js";

describe("For", () => {
  it("does not re-invoke children for the same `each` across re-renders", () => {
    let calls = 0;
    const items = ["a", "b"];
    const App = () => (
      <For each={items}>
        {(item) => {
          calls += 1;
          return <span>{item}</span>;
        }}
      </For>
    );

    const container = document.createElement("div");
    document.body.appendChild(container);
    render(h(App, null), container);
    expect(calls).toBe(2);

    // A re-render creates a fresh inline children function; with `each`
    // unchanged the memoized items must be reused, not rebuilt.
    render(h(App, null), container);
    expect(calls).toBe(2);

    render(null, container);
    container.remove();
  });
});
