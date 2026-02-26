import type { MiddlewareFn, AppContext } from "./types.js";

export async function runMiddlewareChain(
  middlewares: MiddlewareFn[],
  request: Request,
  context: AppContext,
  finalHandler: () => Promise<Response>
): Promise<Response> {
  let index = -1;

  async function dispatch(i: number): Promise<Response> {
    if (i <= index) {
      throw new Error("next() called multiple times");
    }
    index = i;

    if (i >= middlewares.length) {
      return finalHandler();
    }

    const middleware = middlewares[i];
    return middleware(request, context, () => dispatch(i + 1));
  }

  return dispatch(0);
}

export function composeMiddleware(
  middlewares: MiddlewareFn[]
): (request: Request, context: AppContext) => Promise<Response> {
  return (request: Request, context: AppContext) => {
    return runMiddlewareChain(middlewares, request, context, async () => {
      return new Response(null, { status: 404 });
    });
  };
}
