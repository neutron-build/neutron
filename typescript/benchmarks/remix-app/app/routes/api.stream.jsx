import { json } from "@remix-run/node";

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

  return new Response(stream, {
    status: 200,
    headers: {
      "Content-Type": "text/plain; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}

export async function action() {
  return json({ ok: false, error: "Method Not Allowed" }, { status: 405 });
}
