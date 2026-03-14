"""Tests for neutron/response.py — response serialization."""

from __future__ import annotations

import json

import pytest
from pydantic import BaseModel
from starlette.responses import Response

from neutron.response import JSONResponse, serialize_response, _is_pydantic


# ============================================================================
# Test Models
# ============================================================================


class UserOut(BaseModel):
    id: int
    name: str


class UserFull(BaseModel):
    id: int
    name: str
    secret: str = "hidden"


# ============================================================================
# JSONResponse
# ============================================================================


class TestJSONResponse:
    def test_inherits_starlette(self):
        resp = JSONResponse(content={"ok": True})
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert body["ok"] is True

    def test_custom_status(self):
        resp = JSONResponse(content={"created": True}, status_code=201)
        assert resp.status_code == 201


# ============================================================================
# serialize_response
# ============================================================================


class TestSerializeResponse:
    def test_returns_existing_response_as_is(self):
        resp = Response(content="raw", status_code=200)
        result = serialize_response(resp)
        assert result is resp

    def test_pydantic_model(self):
        user = UserOut(id=1, name="Alice")
        resp = serialize_response(user)
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert body["id"] == 1
        assert body["name"] == "Alice"

    def test_pydantic_model_custom_status(self):
        user = UserOut(id=1, name="Bob")
        resp = serialize_response(user, status_code=201)
        assert resp.status_code == 201

    def test_dict(self):
        resp = serialize_response({"key": "value"})
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert body["key"] == "value"

    def test_list_of_dicts(self):
        data = [{"a": 1}, {"b": 2}]
        resp = serialize_response(data)
        body = json.loads(resp.body)
        assert len(body) == 2
        assert body[0]["a"] == 1

    def test_list_of_pydantic_models(self):
        users = [UserOut(id=1, name="A"), UserOut(id=2, name="B")]
        resp = serialize_response(users)
        body = json.loads(resp.body)
        assert len(body) == 2
        assert body[0]["id"] == 1
        assert body[1]["name"] == "B"

    def test_none_returns_204(self):
        resp = serialize_response(None)
        assert resp.status_code == 204

    def test_none_with_explicit_status(self):
        resp = serialize_response(None, status_code=202)
        assert resp.status_code == 202

    def test_string(self):
        resp = serialize_response("hello")
        body = json.loads(resp.body)
        assert body == "hello"

    def test_int(self):
        resp = serialize_response(42)
        body = json.loads(resp.body)
        assert body == 42

    def test_float(self):
        resp = serialize_response(3.14)
        body = json.loads(resp.body)
        assert body == 3.14

    def test_bool(self):
        resp = serialize_response(True)
        body = json.loads(resp.body)
        assert body is True

    def test_response_model_filtering(self):
        """response_model should filter fields."""
        full = UserFull(id=1, name="Alice", secret="s3cret")
        resp = serialize_response(full, response_model=UserOut)
        body = json.loads(resp.body)
        assert body["id"] == 1
        assert body["name"] == "Alice"
        assert "secret" not in body

    def test_response_model_list_filtering(self):
        """response_model should filter list items."""
        users = [
            UserFull(id=1, name="A", secret="x"),
            UserFull(id=2, name="B", secret="y"),
        ]
        resp = serialize_response(users, response_model=UserOut)
        body = json.loads(resp.body)
        assert len(body) == 2
        for item in body:
            assert "secret" not in item

    def test_response_model_with_dict(self):
        """response_model should validate dict input."""
        resp = serialize_response(
            {"id": 1, "name": "Alice"},
            response_model=UserOut,
        )
        body = json.loads(resp.body)
        assert body["id"] == 1


# ============================================================================
# _is_pydantic
# ============================================================================


class TestIsPydantic:
    def test_pydantic_model(self):
        assert _is_pydantic(UserOut) is True

    def test_non_pydantic(self):
        assert _is_pydantic(dict) is False

    def test_string(self):
        assert _is_pydantic(str) is False

    def test_none(self):
        assert _is_pydantic(None) is False

    def test_instance(self):
        # An instance, not a class
        assert _is_pydantic(UserOut(id=1, name="x")) is False
