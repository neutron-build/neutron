import { useState } from "preact/hooks";

export function Counter(props: { start?: number }) {
  const [count, setCount] = useState(props.start || 0);

  return (
    <div style="display: inline-flex; align-items: center; gap: 0.5rem;">
      <button onClick={() => setCount((value) => value - 1)}>-</button>
      <span>{count}</span>
      <button onClick={() => setCount((value) => value + 1)}>+</button>
    </div>
  );
}
