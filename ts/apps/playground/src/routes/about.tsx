export const config = { mode: "static" };

export function headers() {
  return {
    "Cache-Control": "public, max-age=300",
  };
}

export default function About() {
  return (
    <div>
      <h1>About</h1>
      <p>This is the about page.</p>
    </div>
  );
}
