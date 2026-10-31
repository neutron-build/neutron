import { useState } from "__HOOKS_IMPORT__";

export function Counter(props: { start?: number }) {
  const [count, setCount] = useState(props.start || 0);

  return (
    <div style={{ marginTop: "1rem", display: "inline-flex", gap: "0.5rem", alignItems: "center" }}>
      <strong>Island Counter:</strong>
      <button onClick={() => setCount((value) => value - 1)}>-</button>
      <span>{count}</span>
      <button onClick={() => setCount((value) => value + 1)}>+</button>
    </div>
  );
}
