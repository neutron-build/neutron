function buildRows(count = 400) {
  return Array.from({ length: count }, (_, i) => ({
    id: i + 1,
    value: `row-${i + 1}-${(i * 37) % 101}`,
  }));
}

export async function getServerSideProps() {
  return { props: { rows: buildRows() } };
}

export default function BigPage({ rows }) {
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
