// §8 drain probe: a request that is still in flight when SIGTERM arrives.
// The runner starts it, signals the server, and asserts it still completes
// with 200 while new connections are refused and the process exits 0.
export const config = { mode: "app" };

export async function loader() {
  await new Promise((resolve) => setTimeout(resolve, 1500));
  throw new Response(JSON.stringify({ ok: true }), {
    status: 200,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}
