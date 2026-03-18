import { describe, it, expect } from "vitest";
import { h, Fragment } from "preact";

describe("Streaming SSR", () => {
  it("should render to string via streaming API", async () => {
    const Component = ({ name }: { name: string }) => h("div", null, `Hello, ${name}!`);

    // Import streaming module
    const { renderToReadableStream } = await import("preact-render-to-string/stream");
    
    const element = h(Component, { name: "World" });
    const stream = renderToReadableStream(element);
    
    // Wait for stream to complete
    await stream.allReady;
    
    // Read into string
    const reader = stream.getReader();
    const decoder = new TextDecoder();
    let html = "";
    
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      html += decoder.decode(value, { stream: true });
    }

    expect(html).toContain("Hello, World!");
    expect(html).toContain("<div>");
    expect(html).toContain("</div>");
  });

  it("should render nested components", async () => {
    const Child = ({ count }: { count: number }) => h("span", null, `Count: ${count}`);
    const Parent = ({ items }: { items: number[] }) => 
      h("ul", null, items.map(i => h("li", { key: i }, h(Child, { count: i }))));

    const { renderToReadableStream } = await import("preact-render-to-string/stream");
    
    const element = h(Parent, { items: [1, 2, 3] });
    const stream = renderToReadableStream(element);
    await stream.allReady;
    
    const reader = stream.getReader();
    const decoder = new TextDecoder();
    let html = "";
    
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      html += decoder.decode(value, { stream: true });
    }

    expect(html).toContain("<ul>");
    expect(html).toContain("<li>");
    expect(html).toContain("Count: 1");
    expect(html).toContain("Count: 2");
    expect(html).toContain("Count: 3");
    expect(html).toContain("</ul>");
  });

  it("should match sync render output", async () => {
    const App = () => h("div", { class: "app" },
      h("h1", null, "Title"),
      h("p", null, "Content"),
      h("ul", null, [
        h("li", { key: 1 }, "One"),
        h("li", { key: 2 }, "Two"),
      ])
    );

    // Sync render
    const { renderToString } = await import("preact-render-to-string");
    const syncHtml = renderToString(h(App, {}));

    // Stream render
    const { renderToReadableStream } = await import("preact-render-to-string/stream");
    const stream = renderToReadableStream(h(App, {}));
    await stream.allReady;
    
    const reader = stream.getReader();
    const decoder = new TextDecoder();
    let streamHtml = "";
    
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      streamHtml += decoder.decode(value, { stream: true });
    }

    // Outputs should match
    expect(streamHtml).toBe(syncHtml);
  });
});
