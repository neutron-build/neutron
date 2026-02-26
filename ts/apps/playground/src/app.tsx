import { useState } from "preact/hooks";

export function App() {
  const [count, setCount] = useState(0);

  return (
    <div style={{ fontFamily: "system-ui, sans-serif", padding: "2rem" }}>
      <h1>Neutron Playground</h1>
      <p>Testing the framework core.</p>
      <button onClick={() => setCount((c) => c + 1)}>Count: {count}</button>
    </div>
  );
}
