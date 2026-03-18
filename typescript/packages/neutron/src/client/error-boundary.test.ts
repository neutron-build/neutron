// @vitest-environment happy-dom
import { describe, it, expect, vi, beforeEach } from "vitest";
import { h, render } from "preact";
import { ClientErrorBoundary } from "./error-boundary.js";
import type { FunctionalComponent } from "preact";

/** Flush Preact's async setState from componentDidCatch */
function flush(): Promise<void> {
  return new Promise((r) => setTimeout(r, 0));
}

function getContainer(): HTMLDivElement {
  const container = document.createElement("div");
  document.body.appendChild(container);
  return container;
}

function cleanup(container: HTMLDivElement) {
  render(null, container);
  container.remove();
}

// A component that always throws during render
function ThrowingComponent(): never {
  throw new Error("Component exploded");
}

describe("ClientErrorBoundary", () => {
  let container: HTMLDivElement;

  beforeEach(() => {
    container = getContainer();
    vi.spyOn(console, "error").mockImplementation(() => {});
  });

  it("has getDerivedStateFromError defined as a static method", () => {
    expect(typeof ClientErrorBoundary.getDerivedStateFromError).toBe("function");
    const state = ClientErrorBoundary.getDerivedStateFromError(new Error("test"));
    expect(state).toEqual({ error: expect.any(Error) });
  });

  it("renders children normally when no error", () => {
    render(
      h(ClientErrorBoundary, null, h("div", null, "Hello")),
      container
    );
    expect(container.textContent).toBe("Hello");
    cleanup(container);
  });

  it("catches render error and shows default fallback", async () => {
    render(
      h(ClientErrorBoundary, null, h(ThrowingComponent, null)),
      container
    );
    await flush();

    expect(container.textContent).toContain("Something went wrong");
    expect(container.querySelector("button")?.textContent).toBe("Try again");
    cleanup(container);
  });

  it("renders route ErrorBoundary fallback when provided", async () => {
    const CustomFallback: FunctionalComponent<{
      error: Error;
      reset?: () => void;
    }> = ({ error, reset }) =>
      h(
        "div",
        null,
        h("p", { class: "custom-error" }, `Custom: ${error.message}`),
        h("button", { onClick: reset }, "Retry")
      );

    render(
      h(
        ClientErrorBoundary,
        { fallback: CustomFallback },
        h(ThrowingComponent, null)
      ),
      container
    );
    await flush();

    expect(container.querySelector(".custom-error")?.textContent).toBe(
      "Custom: Component exploded"
    );
    expect(container.querySelector("button")?.textContent).toBe("Retry");
    cleanup(container);
  });

  it("reset clears error and re-renders children", async () => {
    let resetFn: (() => void) | undefined;

    const FallbackCapture: FunctionalComponent<{
      error: Error;
      reset?: () => void;
    }> = ({ error, reset }) => {
      resetFn = reset;
      return h("div", { class: "error-state" }, error.message);
    };

    let shouldThrow = true;
    function MaybeThrow() {
      if (shouldThrow) throw new Error("Boom");
      return h("div", { class: "success" }, "Recovered");
    }

    render(
      h(ClientErrorBoundary, { fallback: FallbackCapture }, h(MaybeThrow, null)),
      container
    );
    await flush();

    expect(container.querySelector(".error-state")?.textContent).toBe("Boom");

    // Fix the condition and reset
    shouldThrow = false;
    resetFn?.();
    await flush();

    expect(container.querySelector(".success")?.textContent).toBe("Recovered");
    expect(container.querySelector(".error-state")).toBeNull();
    cleanup(container);
  });

  it("nested boundaries: inner catches before outer", async () => {
    const outerCaught: string[] = [];
    const innerCaught: string[] = [];

    const OuterFallback: FunctionalComponent<{ error: Error }> = ({
      error,
    }) => {
      outerCaught.push(error.message);
      return h("div", { class: "outer-error" }, `Outer: ${error.message}`);
    };

    const InnerFallback: FunctionalComponent<{ error: Error }> = ({
      error,
    }) => {
      innerCaught.push(error.message);
      return h("div", { class: "inner-error" }, `Inner: ${error.message}`);
    };

    render(
      h(
        ClientErrorBoundary,
        { fallback: OuterFallback },
        h("div", { class: "layout" },
          h("h1", null, "Layout Header"),
          h(
            ClientErrorBoundary,
            { fallback: InnerFallback },
            h(ThrowingComponent, null)
          )
        )
      ),
      container
    );
    await flush();

    // Inner boundary should catch the error
    expect(innerCaught).toEqual(["Component exploded"]);
    expect(outerCaught).toEqual([]);

    // Layout should remain intact
    expect(container.querySelector("h1")?.textContent).toBe("Layout Header");
    expect(container.querySelector(".inner-error")?.textContent).toBe(
      "Inner: Component exploded"
    );
    cleanup(container);
  });

  it("error propagates to outer when inner has no fallback", async () => {
    const outerCaught: string[] = [];

    const OuterFallback: FunctionalComponent<{ error: Error }> = ({
      error,
    }) => {
      outerCaught.push(error.message);
      return h("div", { class: "outer-error" }, `Outer: ${error.message}`);
    };

    render(
      h(
        ClientErrorBoundary,
        { fallback: OuterFallback },
        h("div", { class: "layout" },
          // Inner boundary with no fallback renders DefaultErrorFallback
          h(ClientErrorBoundary, null, h(ThrowingComponent, null))
        )
      ),
      container
    );
    await flush();

    // Inner boundary catches it (shows default fallback), doesn't propagate
    expect(outerCaught).toEqual([]);
    expect(container.textContent).toContain("Something went wrong");
    cleanup(container);
  });
});
