import { getCookie, serializeCookie, type MiddlewareFn } from "neutron";
import { getDataRuntime } from "../data/runtime.js";

export const config = { mode: "app" };
const SESSION_COOKIE_NAME = "neutron_sid";
const SESSION_TTL_SEC = 60 * 60 * 24 * 7;

export const middleware: MiddlewareFn = async (request, context, next) => {
  const runtime = await getDataRuntime();
  const existingSessionId = getCookie(request, SESSION_COOKIE_NAME);
  const session = existingSessionId
    ? await runtime.sessions.get<{
        visits?: number;
        isAuthenticated?: boolean;
        user?: { id: string; name: string; role: string } | null;
      }>(existingSessionId)
    : null;

  const currentSession =
    session ||
    (await runtime.sessions.create({
      visits: 0,
      isAuthenticated: false,
      user: null,
    }));

  const sessionId = currentSession.id;
  const visitCount = Number(currentSession.data.visits || 0) + 1;

  const authHeader = request.headers.get("Authorization");
  let isAuthenticated = false;
  let user: { id: string; name: string; role: string } | null = null;

  // Simulated auth check backed by a real session store.
  if (!authHeader) {
    isAuthenticated = false;
    user = null;
  } else if (authHeader === "Bearer valid-token") {
    isAuthenticated = true;
    user = { id: "1", name: "Admin User", role: "admin" };
  } else {
    isAuthenticated = false;
    user = null;
  }

  await runtime.sessions.set(sessionId, {
    visits: visitCount,
    isAuthenticated,
    user,
  });

  context.isAuthenticated = isAuthenticated;
  context.user = user;
  context.visits = visitCount;
  context.dataProfile = runtime.profile;

  const response = await next();
  const mutable = new Response(response.body, response);
  mutable.headers.append(
    "Set-Cookie",
    serializeCookie(SESSION_COOKIE_NAME, sessionId, {
      path: "/",
      httpOnly: true,
      sameSite: "Lax",
      secure: new URL(request.url).protocol === "https:",
      maxAge: SESSION_TTL_SEC,
    })
  );
  return mutable;
};

export default function Protected({
  data,
}: {
  data: {
    isAuthenticated: boolean;
    user: { name: string } | null;
    visits: number;
    dataProfile: string;
  };
}) {
  return (
    <div>
      <h1>Protected Area</h1>
      {data?.isAuthenticated ? (
        <div>
          <p>Welcome back, {data?.user?.name}!</p>
          <p>You have access to protected content.</p>
        </div>
      ) : (
        <div>
          <p style={{ color: "#FF4444" }}>You are not authenticated.</p>
          <p>Try adding <code>Authorization: Bearer valid-token</code> header.</p>
        </div>
      )}
      <p style={{ color: "#888" }}>
        Session visits: {data?.visits} | Session backend profile: <code>{data?.dataProfile}</code>
      </p>
    </div>
  );
}

export async function loader({
  context,
}: {
  context: {
    isAuthenticated: boolean;
    user: { name: string } | null;
    visits: number;
    dataProfile: string;
  };
}) {
  return {
    isAuthenticated: context.isAuthenticated,
    user: context.user,
    visits: context.visits,
    dataProfile: context.dataProfile,
  };
}
