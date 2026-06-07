"""Canonical Neutron conformance app (Python SDK).

Boots a Neutron Python (Starlette + Pydantic) server with NO database so the
cross-SDK conformance runner can assert FRAMEWORK_CONTRACT.md against it. Mirrors
the Go/Rust conformance apps endpoint-for-endpoint:

    GET  /health                  contract health shape {status, nucleus, version}
    GET  /openapi.json            OpenAPI 3.1 document
    GET  /docs                    Swagger UI
    GET  /api/items               200 list (compression / request-id probe)
    POST /api/items               422 validation error (RFC 7807 + errors[])
    GET  /errors/{bad-request,…}  forced standard §2 errors

Listen port comes from PORT (HOST), so the runner can pin an ephemeral port.

Boot (requires: pip install starlette pydantic uvicorn):
    PORT=8083 python conformance_app.py
"""

import os
import sys

# Make the in-repo Python SDK importable without installation.
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))
sys.path.insert(0, os.path.join(_REPO, "python"))

from pydantic import BaseModel, Field  # noqa: E402

from neutron import App, Router  # noqa: E402
from neutron.error import (  # noqa: E402
    bad_request,
    conflict,
    forbidden,
    internal_error,
    not_found,
    rate_limited,
    unauthorized,
)
from neutron.middleware import (  # noqa: E402
    CompressionMiddleware,
    CORSMiddleware,
    default_stack,
)


class Item(BaseModel):
    id: int
    name: str
    price: float


class NewItem(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    price: float = Field(ge=0)


router = Router()


@router.get("/api/items")
async def list_items() -> list[Item]:
    return [Item(id=i, name=f"conformance-item-{i}", price=float(i)) for i in range(1, 51)]


@router.post("/api/items")
async def create_item(body: NewItem) -> Item:
    return Item(id=1, name=body.name, price=body.price)


@router.get("/errors/bad-request")
async def err_bad_request() -> dict:
    raise bad_request("forced bad request")


@router.get("/errors/unauthorized")
async def err_unauthorized() -> dict:
    raise unauthorized("forced unauthorized")


@router.get("/errors/forbidden")
async def err_forbidden() -> dict:
    raise forbidden("forced forbidden")


@router.get("/errors/not-found")
async def err_not_found() -> dict:
    raise not_found("forced not found")


@router.get("/errors/conflict")
async def err_conflict() -> dict:
    raise conflict("forced conflict")


@router.get("/errors/rate-limited")
async def err_rate_limited() -> dict:
    raise rate_limited("forced rate limited")


@router.get("/errors/internal")
async def err_internal() -> dict:
    raise internal_error("forced internal error")


def build() -> App:
    app = App(
        title="Neutron Conformance API",
        version="9.9.9",
        # default_stack enforces the FRAMEWORK_CONTRACT.md middleware order.
        middleware=default_stack(
            cors=CORSMiddleware(allow_origins=["*"]),
            compression=CompressionMiddleware(),
        ),
    )
    app.include_router(router)
    return app


app = build()


if __name__ == "__main__":
    app.run(host=os.getenv("HOST", "127.0.0.1"), port=int(os.getenv("PORT", "8083")))
