// @vitest-environment happy-dom
import { describe, it, expect } from "vitest";
import { h, render } from "preact";
import { Show, For, Switch, Match, Index } from "./control-flow.js";

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

// Preact-side counterpart of type-fixtures/react-control-flow/: the control-
// flow components (Show/For/Switch/Match/Index) now return `any` elements so
// React-typed JSX can use them too. This file is compiled by the package build
// (jsxImportSource: preact), so the JSX below pins that preact DX, runtime
// behavior and prop checking did not regress — including the @ts-expect-error,
// which fails the build if a mistyped prop stops being rejected.
describe("control-flow components (preact JSX)", () => {
  it("still rejects mistyped props (props stay fully typed)", () => {
    // @ts-expect-error `each` must be an array, not a number
    return <For each={42}>{() => null}</For>;
  });

  it("renders Show's keyed child, and its fallback when falsy", () => {
    const ShowUser = ({ when }: { when: { id: number; name: string } | null }) => (
      <Show when={when} fallback={<p>Anonymous</p>}>
        {(u) => (
          <p>
            Hi {u.name} #{u.id}
          </p>
        )}
      </Show>
    );

    const container = document.createElement("div");
    document.body.appendChild(container);
    render(h(ShowUser, { when: { id: 1, name: "Ada" } }), container);
    expect(container.textContent).toBe("Hi Ada #1");
    render(h(ShowUser, { when: null }), container);
    expect(container.textContent).toBe("Anonymous");
    render(null, container);
    container.remove();
  });

  it("renders For rows through the item/index callback", () => {
    const items = [
      { id: 1, label: "a" },
      { id: 2, label: "b" },
    ];
    const List = () => (
      <ul>
        <For each={items}>
          {(item, index) => (
            <li key={item.id}>
              {index()}: {item.label}
            </li>
          )}
        </For>
      </ul>
    );

    const container = document.createElement("div");
    document.body.appendChild(container);
    render(h(List, null), container);
    expect(Array.from(container.querySelectorAll("li")).map((li) => li.textContent)).toEqual(["0: a", "1: b"]);
    render(null, container);
    container.remove();
  });

  it("Switch renders the first matching Match, and the fallback when none match", () => {
    const Panel = ({ role }: { role: string }) => (
      <Switch fallback={<p>Guest view</p>}>
        <Match when={role === "admin"}>
          <p>Admin panel</p>
        </Match>
        <Match when={role === "user"}>
          <p>User panel</p>
        </Match>
      </Switch>
    );

    const container = document.createElement("div");
    document.body.appendChild(container);
    render(h(Panel, { role: "admin" }), container);
    expect(container.textContent).toBe("Admin panel");
    render(h(Panel, { role: "user" }), container);
    expect(container.textContent).toBe("User panel");
    render(h(Panel, { role: "guest" }), container);
    expect(container.textContent).toBe("Guest view");
    render(null, container);
    container.remove();
  });

  it("renders Index rows through the accessor/index callback", () => {
    const Words = () => (
      <ol>
        <Index each={["alpha", "beta"]}>
          {(word, i) => (
            <li key={i}>
              {i}: {word()}
            </li>
          )}
        </Index>
      </ol>
    );

    const container = document.createElement("div");
    document.body.appendChild(container);
    render(h(Words, null), container);
    expect(Array.from(container.querySelectorAll("li")).map((li) => li.textContent)).toEqual(["0: alpha", "1: beta"]);
    render(null, container);
    container.remove();
  });
});
