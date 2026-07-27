"""Tests for the synchronous TestClient facade (finding N-001)."""

import pytest
from pydantic import BaseModel

from neutron import App, Router
from neutron.test import SyncTestClient, TestClient
from neutron.error import not_found


class EchoIn(BaseModel):
    text: str


class EchoOut(BaseModel):
    text: str


router = Router()


@router.get("/ok")
async def ok() -> dict:
    return {"status": "ok"}


@router.post("/echo")
async def echo(input: EchoIn) -> EchoOut:
    return EchoOut(text=input.text)


@router.get("/boom")
async def boom() -> dict:
    raise not_found("boom")


app = App(title="Sync TestClient", version="1.0.0")
app.include_router(router)


# --- SyncTestClient ---


def test_sync_get():
    with SyncTestClient(app) as client:
        resp = client.get("/ok")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}


def test_sync_post_with_body():
    with SyncTestClient(app) as client:
        resp = client.post("/echo", json={"text": "hello"})
        assert resp.status_code == 201
        assert resp.json() == {"text": "hello"}


def test_sync_non_2xx_status():
    with SyncTestClient(app) as client:
        resp = client.get("/boom")
        assert resp.status_code == 404
        body = resp.json()
        assert body["status"] == 404


def test_sync_context_manager_closes_client():
    client_ref: object | None = None
    with SyncTestClient(app) as client:
        client_ref = client
        assert client.get("/ok").status_code == 200
    assert client_ref is not None
    assert client_ref.is_closed


def test_sync_health_endpoint():
    with SyncTestClient(app) as client:
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"


# --- Async client must still work unchanged ---


@pytest.mark.asyncio
async def test_async_client_still_works():
    async with TestClient(app) as client:
        resp = await client.get("/ok")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}
