"""neutron-py benchmark app.

Two measured configurations, selected by NEUTRON_BENCH_STACK at import:

- ``bare``    (default) — ``App()`` with no user middleware. Directly
                           comparable to the bare Starlette app; isolates
                           neutron's routing/serialization overhead on top
                           of the Starlette it wraps.
- ``default`` — ``App(middleware=default_stack())``, the framework's
                           documented production posture (RequestID -> Logging
                           always on). This costs a uuid4 and a structlog
                           event per request and is reported as its own row,
                           ``neutron-default``, not hidden.

POST routes pin status_code=200 because the Router defaults POST to 201 and
the TS scenario expects 200.
"""

import os

from pydantic import BaseModel
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, Response

from bench_apps import common
from neutron import App, Router

if os.environ.get("NEUTRON_BENCH_STACK") == "default":
    from neutron.middleware import default_stack

    middleware = default_stack()
else:
    middleware = None


class MutateBody(BaseModel):
    seed: int = common.MUTATE_DEFAULT_SEED
    repeat: int = common.MUTATE_DEFAULT_REPEAT


router = Router()


@router.get("/")
async def homepage() -> Response:
    return HTMLResponse(common.STATIC_HTML)


@router.get("/users/{user_id}")
async def user(user_id: str) -> Response:
    return HTMLResponse(common.user_html(user_id))


@router.get("/compute")
async def compute() -> Response:
    return HTMLResponse(common.compute_html(common.bench_compute()))


@router.get("/big")
async def big() -> Response:
    return HTMLResponse(common.big_html())


@router.post("/api/mutate", status_code=200)
async def mutate(body: MutateBody) -> dict:
    seed, repeat = common.clamp_mutation(body.seed, body.repeat)
    return {
        "ok": True,
        "seed": seed,
        "repeat": repeat,
        "value": common.run_mutation(seed, repeat),
    }


@router.get("/login")
async def login() -> Response:
    return HTMLResponse(common.LOGIN_HTML)


@router.get("/protected")
async def protected(request: Request) -> Response:
    if not common.authorized(request.headers.get("authorization")):
        return JSONResponse(
            {"ok": False, "error": "Unauthorized"}, status_code=401
        )
    return HTMLResponse(common.protected_html(True))


@router.post("/api/session/refresh", status_code=200)
async def session_refresh(request: Request) -> dict:
    if not common.authorized(request.headers.get("authorization")):
        return JSONResponse(
            {"ok": False, "error": "Unauthorized"}, status_code=401
        )
    return {"ok": True, "refreshed": True, "token": common.VALID_TOKEN}


app = App(title="bench", version="1.0.0", middleware=middleware)
app.include_router(router)
