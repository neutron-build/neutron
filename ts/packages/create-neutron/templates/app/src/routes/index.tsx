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
        SaaS-oriented starter with app routes, loaders, actions, and auth-ready patterns.
      </p>
      <ul>
        <li><a href="/app/dashboard">Dashboard</a></li>
        <li><a href="/app/settings">Settings (action + Form)</a></li>
        <li><a href="/login">Login</a></li>
        <li><a href="/protected?token=demo">Protected route demo</a></li>
      </ul>
      <p>Generated at <strong>{props.data?.generatedAt}</strong>.</p>
    </section>
  );
}
