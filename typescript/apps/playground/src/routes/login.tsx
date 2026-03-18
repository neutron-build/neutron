export const config = { mode: "static" };

export default function LoginRoute() {
  return (
    <main>
      <h1>bench-login</h1>
      <p>Send Authorization: Bearer valid-token to access /protected.</p>
    </main>
  );
}
