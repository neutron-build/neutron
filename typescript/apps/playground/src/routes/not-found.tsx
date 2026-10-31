import { useState } from "preact/hooks";

// The app's 404 page. App-tier so it is rendered per request (and kept out
// of the SSG snapshot); it answers unmatched paths and loader notFound()s.
export const config = { mode: "app" };

export function head() {
  return { title: "Page not found" };
}

export default function NotFound() {
  const [clicks, setClicks] = useState(0);
  return (
    <div>
      <h1>Page not found</h1>
      <button type="button" onClick={() => setClicks(clicks + 1)}>
        Checked {clicks} times
      </button>
      <p>
        <a href="/todos">Go to todos</a>
      </p>
    </div>
  );
}
