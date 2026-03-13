"""Tests for dependency injection."""

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import BaseModel
from starlette.requests import Request

from neutron import App, Depends, Router
from neutron.test import TestClient


class UserResponse(BaseModel):
    id: int
    name: str


# --- Sync dependency ---


def test_depends_repr():
    def get_db():
        pass

    dep = Depends(get_db)
    assert "Depends" in repr(dep)
    assert "get_db" in repr(dep)


# --- Chained dependencies ---


@pytest.mark.asyncio
async def test_chained_dependencies():
    app = App(title="Chain Test")
    router = Router()

    def get_config() -> str:
        return "production"

    def get_service(config: str = Depends(get_config)) -> str:
        return f"service-{config}"

    @router.get("/chain")
    async def chain(svc: str = Depends(get_service)) -> dict:
        return {"service": svc}

    app.include_router(router)

    async with TestClient(app) as client:
        resp = await client.get("/chain")
        assert resp.status_code == 200
        assert resp.json()["service"] == "service-production"


# --- Async dependency ---


@pytest.mark.asyncio
async def test_async_dependency():
    app = App(title="Async Dep Test")
    router = Router()

    async def get_user_id() -> int:
        return 42

    @router.get("/me")
    async def me(uid: int = Depends(get_user_id)) -> dict:
        return {"user_id": uid}

    app.include_router(router)

    async with TestClient(app) as client:
        resp = await client.get("/me")
        assert resp.status_code == 200
        assert resp.json()["user_id"] == 42


# --- Request injection into dependency ---


@pytest.mark.asyncio
async def test_request_in_dependency():
    app = App(title="Req Dep Test")
    router = Router()

    async def get_host(request: Request) -> str:
        return request.headers.get("host", "unknown")

    @router.get("/host")
    async def show_host(host: str = Depends(get_host)) -> dict:
        return {"host": host}

    app.include_router(router)

    async with TestClient(app) as client:
        resp = await client.get("/host")
        assert resp.status_code == 200
        assert resp.json()["host"] == "test"
