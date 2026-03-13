"""Tests for handler signature extraction."""

import inspect

from pydantic import BaseModel
from starlette.requests import Request

from neutron.depends import Depends, _Depends
from neutron.handler import (
    Header,
    ParamKind,
    Query,
    extract_handler_params,
    extract_path_params,
)


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


def test_extract_path_params():
    assert extract_path_params("/users/{user_id}") == {"user_id"}
    assert extract_path_params("/users/{user_id}/posts/{post_id}") == {
        "user_id",
        "post_id",
    }
    assert extract_path_params("/users") == set()


def test_handler_body_param():
    async def create_user(input: CreateUserInput) -> UserResponse:
        ...

    params, ret = extract_handler_params(create_user, set())
    assert len(params) == 1
    assert params[0].name == "input"
    assert params[0].kind == ParamKind.BODY
    assert params[0].annotation is CreateUserInput
    assert ret is UserResponse


def test_handler_path_param():
    async def get_user(user_id: int) -> UserResponse:
        ...

    params, ret = extract_handler_params(get_user, {"user_id"})
    assert len(params) == 1
    assert params[0].name == "user_id"
    assert params[0].kind == ParamKind.PATH
    assert params[0].annotation is int


def test_handler_query_param():
    async def list_users(query: Query[ListQuery]) -> list[UserResponse]:
        ...

    params, ret = extract_handler_params(list_users, set())
    assert len(params) == 1
    assert params[0].name == "query"
    assert params[0].kind == ParamKind.QUERY
    assert params[0].annotation is ListQuery


def test_handler_header_param():
    class AuthHeaders(BaseModel):
        authorization: str

    async def protected(headers: Header[AuthHeaders]) -> dict:
        ...

    params, ret = extract_handler_params(protected, set())
    assert len(params) == 1
    assert params[0].name == "headers"
    assert params[0].kind == ParamKind.HEADER
    assert params[0].annotation is AuthHeaders


def test_handler_request_param():
    async def raw_handler(request: Request) -> dict:
        ...

    params, ret = extract_handler_params(raw_handler, set())
    assert len(params) == 1
    assert params[0].name == "request"
    assert params[0].kind == ParamKind.REQUEST


def test_handler_depends_param():
    async def get_db() -> str:
        return "db"

    async def handler(db: str = Depends(get_db)) -> dict:
        ...

    params, ret = extract_handler_params(handler, set())
    assert len(params) == 1
    assert params[0].name == "db"
    assert params[0].kind == ParamKind.DEPENDS
    assert isinstance(params[0].default, _Depends)


def test_handler_mixed_params():
    async def update_user(
        user_id: int, input: CreateUserInput
    ) -> UserResponse:
        ...

    params, ret = extract_handler_params(update_user, {"user_id"})
    assert len(params) == 2
    path_param = next(p for p in params if p.name == "user_id")
    body_param = next(p for p in params if p.name == "input")
    assert path_param.kind == ParamKind.PATH
    assert body_param.kind == ParamKind.BODY


def test_handler_no_return_type():
    async def fire_and_forget(input: CreateUserInput):
        ...

    params, ret = extract_handler_params(fire_and_forget, set())
    assert ret is None
