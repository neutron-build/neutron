export const config = { mode: "app" };

export async function loader() {
  const encoder = new TextEncoder();
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(encoder.encode("stream-start\n"));
      controller.enqueue(encoder.encode("chunk-1\n"));
      setTimeout(() => {
        controller.enqueue(encoder.encode("chunk-2\n"));
        setTimeout(() => {
          controller.enqueue(encoder.encode("chunk-3\nstream-end\n"));
          controller.close();
        }, 5);
      }, 5);
    },
  });

  throw new Response(stream, {
    status: 200,
    headers: {
      "Content-Type": "text/plain; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}

export async function action() {
  return new Response(JSON.stringify({ ok: false, error: "Method Not Allowed" }), {
    status: 405,
    headers: {
      "Content-Type": "application/json",
      Allow: "GET",
    },
  });
}

export default function ApiStreamRoute() {
  return <main>GET /api/stream</main>;
}
