interface Framework {
  name: string;
  avgRPS: number;
  avgLatency: string;
  vsNeutron: string;
  variant: "winner" | "good" | "ok" | "slow" | "very-slow";
}

// Data from latest benchmark run (2026-07-15, node track, 80 connections, 5s, 8 scenarios).
// Regenerate with `pnpm --dir typescript/benchmarks run compare:node`.
const frameworks: Framework[] = [
  {
    name: "Neutron (Preact)",
    avgRPS: 18510,
    avgLatency: "~8ms",
    vsNeutron: "Baseline",
    variant: "winner",
  },
  {
    name: "Neutron (React Compat)",
    avgRPS: 19538,
    avgLatency: "~8ms",
    vsNeutron: "+6%",
    variant: "winner",
  },
  {
    name: "Astro 5",
    avgRPS: 11140,
    avgLatency: "~11ms",
    vsNeutron: "-40%",
    variant: "good",
  },
  {
    name: "Next.js 15",
    avgRPS: 6762,
    avgLatency: "~18ms",
    vsNeutron: "-63%",
    variant: "ok",
  },
  {
    name: "Remix 2",
    avgRPS: 6158,
    avgLatency: "~18ms",
    vsNeutron: "-67%",
    variant: "slow",
  },
  {
    name: "Remix 3 (RR7)",
    avgRPS: 5185,
    avgLatency: "~24ms",
    vsNeutron: "-72%",
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
        autocannon, 80 concurrent connections, 5s duration, production builds, node track. Representative single run &mdash; figures vary run-to-run. <a href="https://github.com/neutron-build/neutron/tree/main/typescript/benchmarks">Reproducible harness</a>.
      </p>
    </div>
  );
}
