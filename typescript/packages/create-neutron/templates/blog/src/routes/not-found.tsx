export const config = { mode: "static", hydrate: false };

export function head() {
  return { title: "Not found" };
}

export default function NotFound() {
  return (
    <>
      <h1>Not found</h1>
      <p>There is nothing at this address. <a href="/">Back to the posts</a>.</p>
    </>
  );
}
