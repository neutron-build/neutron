export const config = { mode: "app", hydrate: false };

function benchCompute(iterations = 140000) {
  let acc = 0;
  for (let i = 0; i < iterations; i += 1) {
    acc = (acc + ((i * 17) % 97)) % 1000003;
  }
  return acc;
}

export async function loader() {
  return { value: benchCompute() };
}

export default function ComputePage({ data }: { data: { value: number } }) {
  return (
    <main>
      <h1>Compute</h1>
      <p>value={data.value}</p>
    </main>
  );
}
