import { useState } from "preact/hooks";

export function Counter(props: { start?: number }) {
  const [count, setCount] = useState(props.start || 0);

  return (
    <div style="margin-top: 1rem; display: inline-flex; gap: 0.5rem; align-items: center;">
      <strong>Island Counter:</strong>
      <button onClick={() => setCount((value) => value - 1)}>-</button>
      <span>{count}</span>
      <button onClick={() => setCount((value) => value + 1)}>+</button>
    </div>
  );
}
