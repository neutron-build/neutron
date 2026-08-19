"""Litestar benchmark app — default configuration, pydantic body via DTO.

``Litestar()`` as it ships (openapi routes at /schema/* present but not
measured). POST handlers force status_code=200 for parity with the other
apps (Litestar, like neutron-py, defaults POST to 201).
"""

from litestar import Litestar, Request, Response, get, post
from pydantic import BaseModel

from bench_apps import common


class MutateBody(BaseModel):
    seed: int = common.MUTATE_DEFAULT_SEED
    repeat: int = common.MUTATE_DEFAULT_REPEAT


@get(path="/")
async def homepage() -> Response:
    return Response(
        content=common.STATIC_HTML, media_type="text/html", status_code=200
    )


@get(path="/users/{user_id:str}")
async def user(user_id: str) -> Response:
    return Response(
        content=common.user_html(user_id),
        media_type="text/html",
        status_code=200,
    )


@get(path="/compute")
async def compute() -> Response:
    return Response(
        content=common.compute_html(common.bench_compute()),
        media_type="text/html",
        status_code=200,
    )


@get(path="/big")
async def big() -> Response:
    return Response(
        content=common.big_html(), media_type="text/html", status_code=200
    )


@post(path="/api/mutate", status_code=200)
async def mutate(data: MutateBody) -> dict:
    seed, repeat = common.clamp_mutation(data.seed, data.repeat)
    return {
        "ok": True,
        "seed": seed,
        "repeat": repeat,
        "value": common.run_mutation(seed, repeat),
    }


@get(path="/login")
async def login() -> Response:
    return Response(
        content=common.LOGIN_HTML, media_type="text/html", status_code=200
    )


@get(path="/protected")
async def protected(request: Request) -> Response:
    if not common.authorized(request.headers.get("authorization")):
        return Response(
            content={"ok": False, "error": "Unauthorized"},
            media_type="application/json",
            status_code=401,
        )
    return Response(
        content=common.protected_html(True),
        media_type="text/html",
        status_code=200,
    )


@post(path="/api/session/refresh", status_code=200)
async def session_refresh(request: Request) -> dict:
    if not common.authorized(request.headers.get("authorization")):
        return Response(
            content={"ok": False, "error": "Unauthorized"},
            media_type="application/json",
            status_code=401,
        )
    return {"ok": True, "refreshed": True, "token": common.VALID_TOKEN}


app = Litestar(
    route_handlers=[
        homepage,
        user,
        compute,
        big,
        mutate,
        login,
        protected,
        session_refresh,
    ]
)
