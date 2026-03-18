function benchCompute(iterations = 140000) {
  let acc = 0;
  for (let i = 0; i < iterations; i += 1) {
    acc = (acc + ((i * 17) % 97)) % 1000003;
  }
  return acc;
}

export async function getServerSideProps() {
  const value = benchCompute();
  return { props: { value } };
}

export default function ComputePage({ value }) {
  return (
    <main>
      <h1>Compute</h1>
      <p>value={value}</p>
    </main>
  );
}
