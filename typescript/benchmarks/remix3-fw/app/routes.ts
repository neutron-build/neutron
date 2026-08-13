import { type RouteConfig, index, route } from "@react-router/dev/routes";

export default [
  index("routes/home.tsx"),
  route("login", "routes/login.tsx"),
  route("protected", "routes/protected.tsx"),
  route("users/:id", "routes/users.$id.tsx"),
  route("compute", "routes/compute.tsx"),
  route("big", "routes/big.tsx"),
  route("api/mutate", "routes/api.mutate.ts"),
  route("api/session/refresh", "routes/api.session.refresh.ts"),
  route("api/cache", "routes/api.cache.ts"),
  route("api/revalidate", "routes/api.revalidate.ts"),
  route("api/stream", "routes/api.stream.ts"),
] satisfies RouteConfig;
