export const config = { mode: "app" };

export default function Login() {
  return (
    <section>
      <h2>Login</h2>
      <p>This template keeps auth logic explicit in routes/middleware.</p>
      <p>Demo token for local checks: <code>demo</code></p>
      <p>
        Try <a href="/protected?token=demo">/protected?token=demo</a>.
      </p>
    </section>
  );
}
