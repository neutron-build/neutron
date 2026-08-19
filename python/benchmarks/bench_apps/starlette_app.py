"""Bare Starlette benchmark app — the floor of the comparison.

Routes only, no user middleware, JSON bodies parsed by hand. This is
Starlette as it ships; it is also the layer neutron-py and FastAPI are
built on, so neutron-vs-starlette and fastapi-vs-starlette isolate each
framework's added per-request cost.
"""

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, Response
from starlette.routing import Route

from bench_apps import common


async def homepage(request: Request) -> Response:
    return HTMLResponse(common.STATIC_HTML)


async def user(request: Request) -> Response:
    return HTMLResponse(common.user_html(request.path_params["user_id"]))


async def compute(request: Request) -> Response:
    return HTMLResponse(common.compute_html(common.bench_compute()))


async def big(request: Request) -> Response:
    return HTMLResponse(common.big_html())


async def mutate(request: Request) -> Response:
    body = await request.json()
    seed, repeat = common.clamp_mutation(body.get("seed"), body.get("repeat"))
    return JSONResponse(
        {
            "ok": True,
            "seed": seed,
            "repeat": repeat,
            "value": common.run_mutation(seed, repeat),
        }
    )


async def login(request: Request) -> Response:
    return HTMLResponse(common.LOGIN_HTML)


async def protected(request: Request) -> Response:
    if not common.authorized(request.headers.get("authorization")):
        return JSONResponse(
            {"ok": False, "error": "Unauthorized"}, status_code=401
        )
    return HTMLResponse(common.protected_html(True))


async def session_refresh(request: Request) -> Response:
    if not common.authorized(request.headers.get("authorization")):
        return JSONResponse(
            {"ok": False, "error": "Unauthorized"}, status_code=401
        )
    return JSONResponse(
        {"ok": True, "refreshed": True, "token": common.VALID_TOKEN}
    )


app = Starlette(
    routes=[
        Route("/", homepage, methods=["GET"]),
        Route("/users/{user_id}", user, methods=["GET"]),
        Route("/compute", compute, methods=["GET"]),
        Route("/big", big, methods=["GET"]),
        Route("/api/mutate", mutate, methods=["POST"]),
        Route("/login", login, methods=["GET"]),
        Route("/protected", protected, methods=["GET"]),
        Route("/api/session/refresh", session_refresh, methods=["POST"]),
    ]
)
