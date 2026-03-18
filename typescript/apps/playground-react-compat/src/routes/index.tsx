import React, { useMemo } from "react";

export const config = { mode: "static" };

export async function loader() {
  return {
    message: "Neutron React Compatibility Benchmark",
    frameworks: ["react imports", "preact/compat alias", "neutron runtime"],
  };
}

interface LoaderData {
  message: string;
  frameworks: string[];
}

export default function Home({ data }: { data: LoaderData }) {
  const heading = useMemo(() => data.message, [data.message]);

  return (
    <main>
      <h1>{heading}</h1>
      <ul>
        {data.frameworks.map((item) => (
          <li key={item}>{item}</li>
        ))}
      </ul>
    </main>
  );
}
