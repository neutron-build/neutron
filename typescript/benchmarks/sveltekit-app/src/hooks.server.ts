import type { Handle } from "@sveltejs/kit";

export const handle: Handle = async ({ event, resolve }) => {
  // For /protected, check auth
  if (event.url.pathname === "/protected") {
    const auth = event.request.headers.get("authorization");
    if (auth !== "Bearer valid-token") {
      event.locals.authorized = false;
    } else {
      event.locals.authorized = true;
    }
  }

  // For /dashboard/*, check auth
  if (event.url.pathname.startsWith("/dashboard")) {
    const auth = event.request.headers.get("authorization");
    event.locals.authorized = auth === "Bearer valid-token";
    if (!event.locals.authorized) {
      return new Response(JSON.stringify({ error: "Unauthorized" }), {
        status: 401,
        headers: { "Content-Type": "application/json" },
      });
    }
  }

  return resolve(event);
};
