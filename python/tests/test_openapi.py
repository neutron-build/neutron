"""Tests for OpenAPI 3.1 spec generation."""

from pydantic import BaseModel

from neutron.handler import ParamKind, extract_handler_params
from neutron.openapi import generate_openapi


class CreateInput(BaseModel):
    name: str
    email: str


class ItemResponse(BaseModel):
    id: int
    name: str


class ListQuery(BaseModel):
    page: int = 1
    per_page: int = 20


def _make_info(path, method, handler, path_params=None):
    """Helper to build handler_info entries."""
    path_param_set = set(path_params or [])
    params, return_type = extract_handler_params(handler, path_param_set)
    return {
        "path": path,
        "method": method,
        "handler": handler,
        "params": params,
        "return_type": return_type,
        "summary": None,
        "tags": [],
    }


def test_basic_spec_structure():
    async def health() -> dict:
        ...

    info = [_make_info("/health", "get", health)]
    spec = generate_openapi("Test API", "1.0.0", info)

    assert spec["openapi"] == "3.1.0"
    assert spec["info"]["title"] == "Test API"
    assert spec["info"]["version"] == "1.0.0"
    assert "/health" in spec["paths"]
    assert "ProblemDetail" in spec["components"]["schemas"]


def test_body_parameter():
    async def create(input: CreateInput) -> ItemResponse:
        ...

    info = [_make_info("/items", "post", create)]
    spec = generate_openapi("Test", "1.0.0", info)

    post_op = spec["paths"]["/items"]["post"]
    assert "requestBody" in post_op
    assert "CreateInput" in spec["components"]["schemas"]
    assert "ItemResponse" in spec["components"]["schemas"]


def test_path_parameter():
    async def get_item(item_id: int) -> ItemResponse:
        ...

    info = [_make_info("/items/{item_id}", "get", get_item, ["item_id"])]
    spec = generate_openapi("Test", "1.0.0", info)

    get_op = spec["paths"]["/items/{item_id}"]["get"]
    assert len(get_op["parameters"]) == 1
    param = get_op["parameters"][0]
    assert param["name"] == "item_id"
    assert param["in"] == "path"
    assert param["required"] is True
    assert param["schema"]["type"] == "integer"


def test_query_parameters():
    from neutron.handler import Query

    async def list_items(query: Query[ListQuery]) -> list[ItemResponse]:
        ...

    info = [_make_info("/items", "get", list_items)]
    spec = generate_openapi("Test", "1.0.0", info)

    get_op = spec["paths"]["/items"]["get"]
    params = get_op["parameters"]
    names = {p["name"] for p in params}
    assert "page" in names
    assert "per_page" in names

    # page has a default so should not be required
    page_param = next(p for p in params if p["name"] == "page")
    assert page_param["required"] is False


def test_list_response_type():
    async def list_items() -> list[ItemResponse]:
        ...

    info = [_make_info("/items", "get", list_items)]
    spec = generate_openapi("Test", "1.0.0", info)

    response_schema = spec["paths"]["/items"]["get"]["responses"]["200"]["content"][
        "application/json"
    ]["schema"]
    assert response_schema["type"] == "array"
    assert "$ref" in response_schema["items"]


def test_no_return_type():
    async def fire() -> None:
        ...

    info = [_make_info("/fire", "post", fire)]
    spec = generate_openapi("Test", "1.0.0", info)

    assert "204" in spec["paths"]["/fire"]["post"]["responses"]


def test_error_responses_included():
    async def get_item(item_id: int) -> ItemResponse:
        ...

    info = [_make_info("/items/{item_id}", "get", get_item, ["item_id"])]
    spec = generate_openapi("Test", "1.0.0", info)

    responses = spec["paths"]["/items/{item_id}"]["get"]["responses"]
    assert "422" in responses


def test_summary_and_tags():
    async def get_item(item_id: int) -> ItemResponse:
        ...

    info = [_make_info("/items/{item_id}", "get", get_item, ["item_id"])]
    info[0]["summary"] = "Get an item by ID"
    info[0]["tags"] = ["items"]
    spec = generate_openapi("Test", "1.0.0", info)

    get_op = spec["paths"]["/items/{item_id}"]["get"]
    assert get_op["summary"] == "Get an item by ID"
    assert get_op["tags"] == ["items"]
