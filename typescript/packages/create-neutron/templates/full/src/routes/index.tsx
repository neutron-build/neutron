import { Island } from "neutron/client";
import { Counter } from "../components/Counter";

export const config = { mode: "static" };

export async function loader() {
  return {
    title: "__PROJECT_NAME__",
    generatedAt: new Date().toISOString().slice(0, 19).replace("T", " "),
  };
}

export default function Home(props: { data?: { title: string; generatedAt: string } }) {
  return (
    <section>
      <h2>{props.data?.title}</h2>
      <p>
        Static landing page with optional islands + app route links.
      </p>
      <ul>
        <li><a href="/pricing">Pricing (route group)</a></li>
        <li><a href="/app/dashboard">Dashboard (SSR app route)</a></li>
        <li><a href="/app/settings">Settings (action + Form)</a></li>
      </ul>
      <Island component={Counter} client="visible" id="full-counter" start={2} />
      <p style={{ marginTop: "1rem" }}>
        Generated at <strong>{props.data?.generatedAt}</strong>.
      </p>
    </section>
  );
}
