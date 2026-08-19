"""FastAPI benchmark app — default configuration, pydantic-validated body.

``FastAPI()`` as it ships (docs/openapi routes present but not measured).
The mutate body is validated through a pydantic model, which is FastAPI's
idiomatic request path and neutron-py's as well.
"""

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from pydantic import BaseModel

from bench_apps import common


class MutateBody(BaseModel):
    seed: int = common.MUTATE_DEFAULT_SEED
    repeat: int = common.MUTATE_DEFAULT_REPEAT


app = FastAPI(title="bench", version="1.0.0")


@app.get("/", response_class=HTMLResponse)
async def homepage() -> Response:
    return HTMLResponse(common.STATIC_HTML)


@app.get("/users/{user_id}", response_class=HTMLResponse)
async def user(user_id: str) -> Response:
    return HTMLResponse(common.user_html(user_id))


@app.get("/compute", response_class=HTMLResponse)
async def compute() -> Response:
    return HTMLResponse(common.compute_html(common.bench_compute()))


@app.get("/big", response_class=HTMLResponse)
async def big() -> Response:
    return HTMLResponse(common.big_html())


@app.post("/api/mutate")
async def mutate(body: MutateBody) -> dict:
    seed, repeat = common.clamp_mutation(body.seed, body.repeat)
    return {
        "ok": True,
        "seed": seed,
        "repeat": repeat,
        "value": common.run_mutation(seed, repeat),
    }


@app.get("/login", response_class=HTMLResponse)
async def login() -> Response:
    return HTMLResponse(common.LOGIN_HTML)


@app.get("/protected", response_class=HTMLResponse)
async def protected(request: Request) -> Response:
    if not common.authorized(request.headers.get("authorization")):
        return JSONResponse(
            {"ok": False, "error": "Unauthorized"}, status_code=401
        )
    return HTMLResponse(common.protected_html(True))


@app.post("/api/session/refresh")
async def session_refresh(request: Request) -> Response:
    if not common.authorized(request.headers.get("authorization")):
        return JSONResponse(
            {"ok": False, "error": "Unauthorized"}, status_code=401
        )
    return {"ok": True, "refreshed": True, "token": common.VALID_TOKEN}
