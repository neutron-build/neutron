import { useState } from "preact/hooks";
import type { ComponentChildren } from "preact";

export function Tabs({ labels, children }: { labels: string[]; children: ComponentChildren }) {
  const [active, setActive] = useState(0);
  const panels = Array.isArray(children) ? children : [children];

  return (
    <div class="tabs">
      <div class="tabs-bar" role="tablist">
        {labels.map((label, i) => (
          <button
            key={label}
            role="tab"
            aria-selected={i === active}
            class="tabs-trigger"
            onClick={() => setActive(i)}
          >
            {label}
          </button>
        ))}
      </div>
      {panels.map((panel, i) => (
        <div
          key={i}
          role="tabpanel"
          class="tabs-panel"
          hidden={i !== active}
        >
          {panel}
        </div>
      ))}
    </div>
  );
}
