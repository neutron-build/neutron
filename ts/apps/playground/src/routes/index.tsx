import type { LoaderArgs } from "neutron";

export const config = { mode: "static" };

export async function loader({ params }: LoaderArgs) {
  return {
    message: "Welcome to Neutron!",
    features: [
      "File-based routing",
      "Static & app routes",
      "Loaders & actions",
      "Form handling",
    ],
    buildTime: new Date().toISOString(),
  };
}

interface LoaderData {
  message: string;
  features: string[];
  buildTime: string;
}

export default function Home({ data }: { data: LoaderData }) {
  return (
    <div>
      <h1>{data?.message}</h1>
      <p>This page was rendered at: <strong>{data?.buildTime}</strong></p>
      
      <h2>Features</h2>
      <ul>
        {data?.features.map((feature) => (
          <li key={feature}>{feature}</li>
        ))}
      </ul>
      
      <p style="margin-top: 2rem; color: #888;">
        This is a static route. No JavaScript shipped.
      </p>
    </div>
  );
}
