export const config = { mode: "app", hydrate: false };

function buildRows(count = 400) {
  return Array.from({ length: count }, (_, i) => ({
    id: i + 1,
    value: `row-${i + 1}-${(i * 37) % 101}`,
  }));
}

export async function loader() {
  return { rows: buildRows() };
}

export default function BigPage({
  data,
}: {
  data: { rows: Array<{ id: number; value: string }> };
}) {
  return (
    <main>
      <h1>Big Payload</h1>
      <ul>
        {data.rows.map((row) => (
          <li key={row.id}>{row.value}</li>
        ))}
      </ul>
    </main>
  );
}
