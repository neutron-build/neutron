import { useState } from "preact/hooks";

interface CounterProps {
  start?: number;
  label?: string;
}

function Counter({ start = 0, label = "Count" }: CounterProps) {
  const [count, setCount] = useState(start);

  return (
    <div style="padding: 1rem; border: 1px solid #333; border-radius: 8px;">
      <p style="font-size: 1.5rem; margin: 0 0 0.5rem 0;">
        {label}: <strong>{count}</strong>
      </p>
      <div style="display: flex; gap: 0.5rem;">
        <button onClick={() => setCount((c) => c - 1)}>-</button>
        <button onClick={() => setCount((c) => c + 1)}>+</button>
        <button onClick={() => setCount(start)}>Reset</button>
      </div>
    </div>
  );
}

Counter.displayName = "Counter";

export default Counter;
