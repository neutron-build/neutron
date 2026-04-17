interface Bar {
  label: string;
  value: string;
  width: number;
  color: string;
}

interface BenchmarkBarsProps {
  bars: Bar[];
  title?: string;
}

export default function BenchmarkBars({ bars, title }: BenchmarkBarsProps) {
  return (
    <div class="benchmark-bars" data-animate>
      {title && <h3 class="benchmark-bars__title">{title}</h3>}
      <div class="benchmark-bars__list">
        {bars.map((bar, i) => (
          <div
            class="bench-bar"
            key={i}
            style={`--bar-width: ${bar.width}%; --bar-color: ${bar.color};`}
          >
            <span class="bench-bar__label">{bar.label}</span>
            <div class="bench-bar__track">
              <div class="bench-bar__fill"></div>
            </div>
            <span class="bench-bar__value">{bar.value}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
