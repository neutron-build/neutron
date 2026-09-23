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


def test_problem_detail_schema_matches_real_422_payload():
    """The documented 422 body must describe what the app actually returns."""
    from neutron import App, Router
    from neutron.test import SyncTestClient

    class StrictInput(BaseModel):
        name: str
        age: int

    router = Router()

    @router.post("/things")
    async def create_thing(input: StrictInput) -> dict:
        return {"ok": True}

    app = App(title="Test", version="1.0.0")
    app.include_router(router)

    spec = app.openapi
    schema = spec["components"]["schemas"]["ProblemDetail"]

    with SyncTestClient(app) as client:
        resp = client.post("/things", json={"name": 123, "age": "NaN"})
    assert resp.status_code == 422
    payload = resp.json()

    props = schema["properties"]
    for key in payload:
        assert key in props, f"payload key {key!r} missing from ProblemDetail"
    for key in schema["required"]:
        assert key in payload, f"required key {key!r} missing from payload"

    errors_schema = props["errors"]["items"]
    for err in payload["errors"]:
        for key in err:
            assert key in errors_schema["properties"]
        for key in errors_schema["required"]:
            assert key in err


# --- Nested models: every $ref must resolve inside the document ---------------


def _billing_models():
    class Address(BaseModel):
        street: str
        city: str

    return Address


def _shipping_models():
    class Address(BaseModel):
        dock: int

    return Address


def _iter_refs(node):
    if isinstance(node, dict):
        if "$ref" in node:
            yield node["$ref"]
        for value in node.values():
            yield from _iter_refs(value)
    elif isinstance(node, list):
        for value in node:
            yield from _iter_refs(value)


def _resolve(spec, ref):
    assert ref.startswith("#/"), f"non-local ref {ref}"
    node = spec
    for part in ref[2:].split("/"):
        assert isinstance(node, dict) and part in node, f"dangling ref {ref}"
        node = node[part]
    return node


def _nested_spec():
    from enum import Enum
    from typing import Literal

    BillingAddress = _billing_models()
    ShippingAddress = _shipping_models()

    class Status(str, Enum):
        active = "active"
        closed = "closed"

    class Customer(BaseModel):
        billing: BillingAddress
        previous: list[BillingAddress] = []
        status: Status
        tier: Literal["free", "pro"] | None = None

    class Shipment(BaseModel):
        to: ShippingAddress
        customer: Customer

    class Node(BaseModel):
        name: str
        children: list["Node"] = []

    class ProblemDetail(BaseModel):  # collides with the shared RFC 7807 schema
        code: int

    async def create_customer(body: Customer) -> Shipment: ...
    async def tree() -> Node: ...
    async def app_problem() -> list[ProblemDetail]: ...

    info = [
        _make_info("/customers", "post", create_customer),
        _make_info("/tree", "get", tree),
        _make_info("/problems", "get", app_problem),
    ]
    return generate_openapi("Test", "1.0.0", info)


def test_nested_model_refs_all_resolve():
    spec = _nested_spec()
    refs = list(_iter_refs(spec))
    assert refs
    for ref in refs:
        assert ref.startswith("#/components/schemas/"), ref
        _resolve(spec, ref)
    assert not any("$defs" in s for s in spec["components"]["schemas"].values())


def test_nested_model_schemas_hoisted_with_correct_shape():
    spec = _nested_spec()
    schemas = spec["components"]["schemas"]

    customer = schemas["Customer"]
    assert set(customer["required"]) == {"billing", "status"}
    assert _resolve(spec, customer["properties"]["status"]["$ref"])["enum"] == [
        "active",
        "closed",
    ]
    tier = customer["properties"]["tier"]["anyOf"]
    assert {"enum": ["free", "pro"], "type": "string"} in tier
    assert {"type": "null"} in tier

    node = schemas["Node"]
    assert node["properties"]["children"]["items"] == {"$ref": "#/components/schemas/Node"}


def test_colliding_model_names_stay_distinct():
    spec = _nested_spec()
    schemas = spec["components"]["schemas"]

    billing = _resolve(spec, schemas["Customer"]["properties"]["billing"]["$ref"])
    shipping = _resolve(spec, schemas["Shipment"]["properties"]["to"]["$ref"])
    assert set(billing["properties"]) == {"street", "city"}
    assert set(shipping["properties"]) == {"dock"}
    assert (
        schemas["Customer"]["properties"]["billing"]["$ref"]
        != schemas["Shipment"]["properties"]["to"]["$ref"]
    )


def test_app_model_named_problem_detail_does_not_replace_shared_schema():
    spec = _nested_spec()
    schemas = spec["components"]["schemas"]

    assert set(schemas["ProblemDetail"]["required"]) == {"type", "title", "status", "detail"}
    items = spec["paths"]["/problems"]["get"]["responses"]["200"]["content"][
        "application/json"
    ]["schema"]["items"]
    assert items == {"$ref": "#/components/schemas/ProblemDetail2"}
    assert schemas["ProblemDetail2"]["properties"] == {
        "code": {"title": "Code", "type": "integer"}
    }


def test_nested_spec_is_deterministic():
    assert _nested_spec() == _nested_spec()


def test_served_openapi_has_no_dangling_refs():
    from neutron import App, Router
    from neutron.test import SyncTestClient

    class Line(BaseModel):
        sku: str

    class Order(BaseModel):
        lines: list[Line]

    router = Router()

    @router.post("/orders")
    async def create(body: Order) -> Order:
        return body

    app = App()
    app.include_router(router)
    with SyncTestClient(app) as client:
        spec = client.get("/openapi.json").json()

    assert "Line" in spec["components"]["schemas"]
    for ref in _iter_refs(spec):
        _resolve(spec, ref)
