import { useState, useEffect } from "preact/hooks";

interface StopwatchProps {
  title?: string;
}

function Stopwatch({ title = "Stopwatch" }: StopwatchProps) {
  const [time, setTime] = useState(0);
  const [running, setRunning] = useState(false);

  useEffect(() => {
    if (!running) return;

    const interval = setInterval(() => {
      setTime((t) => t + 10);
    }, 10);

    return () => clearInterval(interval);
  }, [running]);

  const format = (ms: number) => {
    const mins = Math.floor(ms / 60000);
    const secs = Math.floor((ms % 60000) / 1000);
    const cents = Math.floor((ms % 1000) / 10);
    return `${mins.toString().padStart(2, "0")}:${secs.toString().padStart(2, "0")}.${cents.toString().padStart(2, "0")}`;
  };

  return (
    <div style="padding: 1rem; border: 1px solid #333; border-radius: 8px; text-align: center;">
      <h3 style="margin: 0 0 0.5rem 0;">{title}</h3>
      <div style="font-size: 2rem; font-family: monospace; margin: 1rem 0;">
        {format(time)}
      </div>
      <div style="display: flex; gap: 0.5rem; justify-content: center;">
        <button onClick={() => setRunning(true)} disabled={running}>Start</button>
        <button onClick={() => setRunning(false)} disabled={!running}>Stop</button>
        <button onClick={() => { setRunning(false); setTime(0); }}>Reset</button>
      </div>
    </div>
  );
}

Stopwatch.displayName = "Stopwatch";

export default Stopwatch;
