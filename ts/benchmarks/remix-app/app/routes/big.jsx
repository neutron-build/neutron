import { json } from "@remix-run/node";
import { useLoaderData } from "@remix-run/react";

function buildRows(count = 400) {
  return Array.from({ length: count }, (_, i) => ({
    id: i + 1,
    value: `row-${i + 1}-${(i * 37) % 101}`,
  }));
}

export async function loader() {
  return json({ rows: buildRows() });
}

export default function BigPage() {
  const { rows } = useLoaderData();
  return (
    <main>
      <h1>Big Payload</h1>
      <ul>
        {rows.map((row) => (
          <li key={row.id}>{row.value}</li>
        ))}
      </ul>
    </main>
  );
}
