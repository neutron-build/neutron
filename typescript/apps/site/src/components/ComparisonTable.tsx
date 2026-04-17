interface ComparisonTableProps {
  headers: string[];
  rows: string[][];
  highlightColumn?: number;
  accentRgb?: string;
}

export default function ComparisonTable({
  headers,
  rows,
  highlightColumn,
  accentRgb = "0, 229, 160",
}: ComparisonTableProps) {
  return (
    <div class="comparison-table-wrap" data-animate>
      <table class="comparison-table">
        <thead>
          <tr>
            {headers.map((h, i) => (
              <th
                key={i}
                class={i === highlightColumn ? "ct-highlight" : undefined}
              >
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, ri) => (
            <tr key={ri}>
              {row.map((cell, ci) => (
                <td
                  key={ci}
                  class={
                    ci === highlightColumn ? "ct-highlight" : undefined
                  }
                >
                  {cell}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
