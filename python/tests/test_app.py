"""Integration tests for the full Neutron application."""

from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import BaseModel

from neutron import App, Depends, Query, Router
from neutron.error import not_found
from neutron.middleware import CORSMiddleware, RequestIDMiddleware
from neutron.test import TestClient


# --- Fixtures ---


class CreateUserInput(BaseModel):
    name: str
    email: str


class UserResponse(BaseModel):
    id: int
    name: str
    email: str


class ListQuery(BaseModel):
    page: int = 1
    per_page: int = 20


router = Router()


@router.post("/users")
async def create_user(input: CreateUserInput) -> UserResponse:
    return UserResponse(id=1, name=input.name, email=input.email)


@router.get("/users/{user_id}")
async def get_user(user_id: int) -> UserResponse:
    if user_id == 999:
        raise not_found(f"User {user_id} not found")
    return UserResponse(id=user_id, name="Alice", email="alice@test.com")


@router.get("/users")
async def list_users(query: Query[ListQuery]) -> list[UserResponse]:
    return [
        UserResponse(id=1, name="Alice", email="alice@test.com"),
    ]


@router.delete("/users/{user_id}")
async def delete_user(user_id: int) -> None:
    return None


app = App(title="Test API", version="1.0.0")
app.include_router(router, prefix="/api")


# --- Tests ---


@pytest.mark.asyncio
async def test_health_check():
    async with TestClient(app) as client:
        resp = await client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["version"] == "1.0.0"
        assert data["nucleus"] == "unconfigured"


@pytest.mark.asyncio
async def test_create_user():
    async with TestClient(app) as client:
        resp = await client.post(
            "/api/users",
            json={"name": "Alice", "email": "alice@test.com"},
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["name"] == "Alice"
        assert data["email"] == "alice@test.com"
        assert data["id"] == 1


@pytest.mark.asyncio
async def test_get_user_with_path_param():
    async with TestClient(app) as client:
        resp = await client.get("/api/users/42")
        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == 42
        assert data["name"] == "Alice"


@pytest.mark.asyncio
async def test_get_user_not_found():
    async with TestClient(app) as client:
        resp = await client.get("/api/users/999")
        assert resp.status_code == 404
        data = resp.json()
        assert data["type"] == "https://neutron.dev/errors/not-found"
        assert data["status"] == 404
        assert "999" in data["detail"]


@pytest.mark.asyncio
async def test_unmatched_path_is_problem_json_not_plain_text():
    async with TestClient(app) as client:
        resp = await client.get("/definitely/not/a/route")
    assert resp.status_code == 404
    assert resp.headers["content-type"].startswith("application/problem+json")
    data = resp.json()
    assert data["type"] == "https://neutron.dev/errors/not-found"
    assert data["status"] == 404
    assert data["instance"] == "/definitely/not/a/route"


@pytest.mark.asyncio
async def test_wrong_method_is_problem_json_and_keeps_allow():
    async with TestClient(app) as client:
        resp = await client.delete("/health")
    assert resp.status_code == 405
    assert resp.headers["content-type"].startswith("application/problem+json")
    assert "GET" in resp.headers.get("allow", "")
    data = resp.json()
    assert data["type"] == "https://neutron.dev/errors/method-not-allowed"
    assert data["status"] == 405
    assert data["instance"] == "/health"


@pytest.mark.asyncio
async def test_validation_error_missing_field():
    async with TestClient(app) as client:
        resp = await client.post("/api/users", json={"name": "Alice"})
        assert resp.status_code == 422
        data = resp.json()
        assert data["type"] == "https://neutron.dev/errors/validation"
        assert data["status"] == 422
        assert "errors" in data
        assert any(e["field"] == "email" for e in data["errors"])


@pytest.mark.asyncio
async def test_validation_error_invalid_json():
    async with TestClient(app) as client:
        resp = await client.post(
            "/api/users",
            content=b"not json",
            headers={"content-type": "application/json"},
        )
        assert resp.status_code == 400


@pytest.mark.asyncio
async def test_query_params():
    async with TestClient(app) as client:
        resp = await client.get("/api/users?page=2&per_page=10")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)


@pytest.mark.asyncio
async def test_delete_returns_204():
    async with TestClient(app) as client:
        resp = await client.delete("/api/users/1")
        assert resp.status_code == 204


@pytest.mark.asyncio
async def test_openapi_spec():
    async with TestClient(app) as client:
        resp = await client.get("/openapi.json")
        assert resp.status_code == 200
        spec = resp.json()
        assert spec["openapi"] == "3.1.0"
        assert spec["info"]["title"] == "Test API"
        assert spec["info"]["version"] == "1.0.0"
        assert "/api/users" in spec["paths"]
        assert "/api/users/{user_id}" in spec["paths"]
        assert "post" in spec["paths"]["/api/users"]
        assert "get" in spec["paths"]["/api/users/{user_id}"]


@pytest.mark.asyncio
async def test_docs_page():
    async with TestClient(app) as client:
        resp = await client.get("/docs")
        assert resp.status_code == 200
        assert "swagger-ui" in resp.text.lower()


# --- Middleware test ---


@pytest.mark.asyncio
async def test_request_id_middleware():
    mw_app = App(
        title="MW Test",
        middleware=[RequestIDMiddleware()],
    )
    mw_router = Router()

    @mw_router.get("/ping")
    async def ping() -> dict:
        return {"pong": True}

    mw_app.include_router(mw_router)

    async with TestClient(mw_app) as client:
        resp = await client.get("/ping")
        assert resp.status_code == 200
        assert "x-request-id" in resp.headers


# --- Dependency injection test ---


@pytest.mark.asyncio
async def test_dependency_injection():
    di_app = App(title="DI Test")
    di_router = Router()

    def get_greeting() -> str:
        return "Hello"

    @di_router.get("/greet")
    async def greet(greeting: str = Depends(get_greeting)) -> dict:
        return {"message": greeting}

    di_app.include_router(di_router)

    async with TestClient(di_app) as client:
        resp = await client.get("/greet")
        assert resp.status_code == 200
        assert resp.json()["message"] == "Hello"


# --- App.run configuration wiring ---


def test_run_env_port():
    """NEUTRON_PORT changes the port App.run binds."""
    import os

    os.environ["NEUTRON_PORT"] = "9107"
    try:
        run_app = App(title="Run Test")
        with patch("uvicorn.run") as uvicorn_run:
            run_app.run()
        assert uvicorn_run.call_args.kwargs["port"] == 9107
    finally:
        del os.environ["NEUTRON_PORT"]


def test_run_explicit_port_beats_env():
    """An explicit port argument wins over NEUTRON_PORT."""
    import os

    os.environ["NEUTRON_PORT"] = "9107"
    try:
        run_app = App(title="Run Test")
        with patch("uvicorn.run") as uvicorn_run:
            run_app.run(port=9377)
        assert uvicorn_run.call_args.kwargs["port"] == 9377
    finally:
        del os.environ["NEUTRON_PORT"]


def test_run_env_host_and_workers():
    """NEUTRON_HOST and NEUTRON_WORKERS reach uvicorn."""
    import os

    os.environ["NEUTRON_HOST"] = "127.0.0.1"
    os.environ["NEUTRON_WORKERS"] = "3"
    try:
        run_app = App(title="Run Test")
        with patch("uvicorn.run") as uvicorn_run:
            run_app.run()
        assert uvicorn_run.call_args.kwargs["host"] == "127.0.0.1"
        assert uvicorn_run.call_args.kwargs["workers"] == 3
    finally:
        del os.environ["NEUTRON_HOST"]
        del os.environ["NEUTRON_WORKERS"]


def test_run_without_neutron_env_vars_uses_defaults():
    """App.run works with no NEUTRON_* set (no database_url required)."""
    import os

    saved = {k: os.environ.pop(k) for k in list(os.environ) if k.startswith("NEUTRON_")}
    try:
        run_app = App(title="Run Test")
        with patch("uvicorn.run") as uvicorn_run:
            run_app.run()
        assert uvicorn_run.call_args.kwargs["host"] == "0.0.0.0"
        assert uvicorn_run.call_args.kwargs["port"] == 8000
        assert uvicorn_run.call_args.kwargs["workers"] == 1
    finally:
        os.environ.update(saved)
