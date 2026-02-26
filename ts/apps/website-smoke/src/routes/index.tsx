import { Island } from "neutron/client";
import { Counter } from "../components/Counter";

export const config = { mode: "static" };

export async function loader() {
  return {
    title: "website-smoke",
    generatedAt: new Date().toISOString(),
  };
}

export default function Home(props: { data?: { title: string; generatedAt: string } }) {
  return (
    <section>
      <h2>{props.data?.title}</h2>
      <p>
        Static-first marketing site with optional islands.
      </p>
      <p>
        Generated at <strong>{props.data?.generatedAt}</strong>.
      </p>
      <Island component={Counter} client="visible" id="marketing-counter" start={1} />
    </section>
  );
}
