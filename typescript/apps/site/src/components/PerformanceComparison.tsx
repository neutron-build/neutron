interface Framework {
  name: string;
  avgRPS: number;
  avgLatency: string;
  vsNeutron: string;
  variant: "winner" | "good" | "ok" | "slow" | "very-slow";
}

// Data from latest benchmark run (2026-02-13, 80 connections, 5s, 8 scenarios)
const frameworks: Framework[] = [
  {
    name: "Neutron (Preact)",
    avgRPS: 3498,
    avgLatency: "~8ms",
    vsNeutron: "Baseline",
    variant: "winner",
  },
  {
    name: "Neutron (React Compat)",
    avgRPS: 2872,
    avgLatency: "~10ms",
    vsNeutron: "-18%",
    variant: "good",
  },
  {
    name: "Next.js 15",
    avgRPS: 830,
    avgLatency: "~14ms",
    vsNeutron: "-76%",
    variant: "slow",
  },
  {
    name: "Astro 5",
    avgRPS: 634,
    avgLatency: "~16ms",
    vsNeutron: "-82%",
    variant: "slow",
  },
  {
    name: "Remix 2",
    avgRPS: 471,
    avgLatency: "~20ms",
    vsNeutron: "-87%",
    variant: "very-slow",
  },
  {
    name: "Remix 3 (RR7)",
    avgRPS: 277,
    avgLatency: "~28ms",
    vsNeutron: "-92%",
    variant: "very-slow",
  },
];

export default function PerformanceComparison() {
  return (
    <div class="perf-comparison" data-animate>
      <div class="perf-comparison__scroll">
        <table class="perf-comparison__table">
          <thead>
            <tr>
              <th scope="col">Framework</th>
              <th scope="col" class="text-right">
                Avg RPS
              </th>
              <th scope="col" class="text-right">
                Avg Latency
              </th>
              <th scope="col" class="text-right">
                vs Neutron
              </th>
            </tr>
          </thead>
          <tbody>
            {frameworks.map((fw) => (
              <tr
                key={fw.name}
                class={`perf-comparison__row perf-comparison__row--${fw.variant}`}
              >
                <td>
                  <strong>{fw.name}</strong>
                </td>
                <td class="text-right">
                  <strong>{fw.avgRPS.toLocaleString()}</strong>
                </td>
                <td class="text-right">
                  <strong>{fw.avgLatency}</strong>
                </td>
                <td class="text-right">
                  <span class={`perf-badge perf-badge--${fw.variant}`}>
                    {fw.vsNeutron}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p class="perf-comparison__note">
        autocannon, 80 concurrent connections, 5s duration, production builds. <a href="/blog/neutron-vs-nextjs-benchmarks-2026">Full methodology</a>.
      </p>
    </div>
  );
}
