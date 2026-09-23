"""OpenAPI 3.1 spec generation from handler type annotations."""

from __future__ import annotations

import inspect
from typing import Any, get_args, get_origin

from pydantic import BaseModel
from pydantic.json_schema import models_json_schema

from neutron.handler import HandlerParam, ParamKind

# ---------------------------------------------------------------------------
# Security scheme helpers
# ---------------------------------------------------------------------------

SecurityScheme = dict[str, Any]
"""Type alias for an OpenAPI Security Scheme Object."""


def bearer_auth_scheme() -> SecurityScheme:
    """Pre-built HTTP Bearer (JWT) security scheme."""
    return {"type": "http", "scheme": "bearer", "bearerFormat": "JWT"}


def api_key_scheme(name: str, location: str = "header") -> SecurityScheme:
    """Pre-built API-key security scheme.

    Args:
        name: Header / query / cookie parameter name (e.g. ``X-API-Key``).
        location: One of ``"header"``, ``"query"``, or ``"cookie"``.
    """
    return {"type": "apiKey", "name": name, "in": location}


def oauth2_scheme(flows: dict[str, Any]) -> SecurityScheme:
    """Pre-built OAuth 2.0 security scheme.

    Args:
        flows: An OpenAPI `OAuthFlows Object
               <https://spec.openapis.org/oas/v3.1.0#oauth-flows-object>`_.
    """
    return {"type": "oauth2", "flows": flows}


# ---------------------------------------------------------------------------
# Spec generation
# ---------------------------------------------------------------------------


def generate_openapi(
    title: str,
    version: str,
    handler_info: list[dict[str, Any]],
    *,
    security_schemes: dict[str, SecurityScheme] | None = None,
    security: list[dict[str, list[str]]] | None = None,
) -> dict[str, Any]:
    """Generate an OpenAPI 3.1 specification from registered handlers.

    Args:
        title: API title shown in the spec ``info`` block.
        version: API version string.
        handler_info: Collected route metadata from routers.
        security_schemes: Mapping of scheme name to
            :data:`SecurityScheme` dicts added under
            ``components.securitySchemes``.
        security: Global security requirements applied to every
            operation (can be overridden per-operation).
    """
    spec: dict[str, Any] = {
        "openapi": "3.1.0",
        "info": {"title": title, "version": version},
        "paths": {},
        "components": {"schemas": {}},
    }

    # Merge security schemes into components
    if security_schemes:
        spec["components"]["securitySchemes"] = dict(security_schemes)

    # Global security requirements
    if security:
        spec["security"] = list(security)

    schemas = spec["components"]["schemas"]
    model_refs = _register_models(_collect_models(handler_info), schemas)

    # Shared error schema
    schemas[_PROBLEM_DETAIL] = {
        "type": "object",
        "properties": {
            "type": {"type": "string"},
            "title": {"type": "string"},
            "status": {"type": "integer"},
            "detail": {"type": "string"},
            "instance": {"type": "string"},
            "errors": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "field": {"type": "string"},
                        "message": {"type": "string"},
                        "value": {},
                    },
                    "required": ["field", "message"],
                },
            },
        },
        "required": ["type", "title", "status", "detail"],
    }

    for info in handler_info:
        path = info["path"]
        method = info["method"]
        params: list[HandlerParam] = info["params"]
        return_type = info["return_type"]

        operation: dict[str, Any] = {}

        if info.get("summary"):
            operation["summary"] = info["summary"]
        if info.get("tags"):
            operation["tags"] = info["tags"]

        # Parameters and request body
        parameters: list[dict[str, Any]] = []
        request_body: dict[str, Any] | None = None

        for param in params:
            if param.kind == ParamKind.PATH:
                parameters.append(
                    {
                        "name": param.name,
                        "in": "path",
                        "required": True,
                        "schema": _type_to_schema(param.annotation),
                    }
                )
            elif param.kind == ParamKind.QUERY:
                if _is_pydantic_model(param.annotation):
                    for field_name, field_info in param.annotation.model_fields.items():
                        param_schema: dict[str, Any] = {
                            "name": field_name,
                            "in": "query",
                            "required": field_info.is_required(),
                            "schema": _type_to_schema(field_info.annotation),
                        }
                        parameters.append(param_schema)
                else:
                    parameters.append(
                        {
                            "name": param.name,
                            "in": "query",
                            "schema": _type_to_schema(param.annotation),
                        }
                    )
            elif param.kind == ParamKind.HEADER:
                if _is_pydantic_model(param.annotation):
                    for field_name, field_info in param.annotation.model_fields.items():
                        parameters.append(
                            {
                                "name": field_name,
                                "in": "header",
                                "required": field_info.is_required(),
                                "schema": _type_to_schema(field_info.annotation),
                            }
                        )
            elif param.kind == ParamKind.QUERY_SCALAR:
                is_required = param.default is inspect.Parameter.empty
                param_entry: dict[str, Any] = {
                    "name": param.name,
                    "in": "query",
                    "required": is_required,
                    "schema": _type_to_schema(param.annotation),
                }
                parameters.append(param_entry)
            elif param.kind == ParamKind.FORM:
                request_body = {
                    "required": True,
                    "content": {
                        "application/x-www-form-urlencoded": {
                            "schema": dict(model_refs[param.annotation]),
                        }
                    },
                }
            elif param.kind == ParamKind.FILE:
                request_body = {
                    "required": True,
                    "content": {
                        "multipart/form-data": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    param.name: {
                                        "type": "string",
                                        "format": "binary",
                                    }
                                },
                            }
                        }
                    },
                }
            elif param.kind == ParamKind.BODY:
                request_body = {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": dict(model_refs[param.annotation]),
                        }
                    },
                }

        if parameters:
            operation["parameters"] = parameters
        if request_body:
            operation["requestBody"] = request_body

        # Responses
        responses: dict[str, Any] = {}
        effective_status = str(info.get("status_code", 200))
        if return_type and return_type is not type(None):
            response_schema = _get_response_schema(return_type, model_refs)
            responses[effective_status] = {
                "description": "Successful response",
                "content": {"application/json": {"schema": response_schema}},
            }
        else:
            responses[effective_status if effective_status != "200" else "204"] = {
                "description": "No content",
            }

        responses["422"] = {
            "description": "Validation Failed",
            "content": {
                "application/problem+json": {
                    "schema": {"$ref": "#/components/schemas/ProblemDetail"},
                }
            },
        }

        operation["responses"] = responses

        # Per-operation security override
        op_security = info.get("security")
        if op_security is not None:
            operation["security"] = op_security

        if path not in spec["paths"]:
            spec["paths"][path] = {}
        spec["paths"][path][method] = operation

    return spec


# --- Helpers ---


def _is_pydantic_model(t: type[Any]) -> bool:
    try:
        return isinstance(t, type) and issubclass(t, BaseModel)
    except TypeError:
        return False


def _type_to_schema(t: type[Any] | None) -> dict[str, Any]:
    if t is None or t is type(None):
        return {"type": "null"}
    if t is int:
        return {"type": "integer"}
    if t is float:
        return {"type": "number"}
    if t is bool:
        return {"type": "boolean"}
    if t is str:
        return {"type": "string"}
    return {"type": "string"}


_PROBLEM_DETAIL = "ProblemDetail"
_REF_PREFIX = "#/components/schemas/"


def _collect_models(handler_info: list[dict[str, Any]]) -> list[type[BaseModel]]:
    """Every Pydantic model that becomes a component, in first-use order."""
    seen: dict[type[BaseModel], None] = {}
    for info in handler_info:
        for param in info["params"]:
            if param.kind in (ParamKind.BODY, ParamKind.FORM) and _is_pydantic_model(
                param.annotation
            ):
                seen.setdefault(param.annotation, None)
        return_type = info["return_type"]
        if get_origin(return_type) is list:
            args = get_args(return_type)
            return_type = args[0] if args else None
        if _is_pydantic_model(return_type):
            seen.setdefault(return_type, None)
    return list(seen)


def _register_models(
    models: list[type[BaseModel]], schemas: dict[str, Any]
) -> dict[type[BaseModel], dict[str, str]]:
    """Add ``models`` and everything they reference to ``components/schemas``.

    Generated in one pass so that every nested model, enum and recursive
    reference lands in ``components/schemas`` and every ``$ref`` points there.
    Per-model ``model_json_schema()`` emits ``#/$defs/<Name>`` refs, which do
    not resolve inside an OpenAPI document, and keys components by bare class
    name, so two different ``Address`` classes overwrote each other.
    Pydantic's shared generator names each distinct class once and qualifies
    colliding names by module (``app__billing__Address``), deterministically.

    ``ProblemDetail`` is reserved for the shared RFC 7807 schema; an
    application model with that name is renamed ``ProblemDetail2`` (then 3,
    ...) and its refs rewritten.

    Returns each top-level model's ``{"$ref": ...}``.
    """
    if not models:
        return {}
    key_map, top = models_json_schema(
        [(m, "validation") for m in models],
        ref_template=_REF_PREFIX + "{model}",
    )
    defs: dict[str, Any] = top.get("$defs", {})
    refs = {model: key_map[(model, "validation")] for model in models}

    if _PROBLEM_DETAIL in defs:
        n = 2
        while f"{_PROBLEM_DETAIL}{n}" in defs:
            n += 1
        new_name = f"{_PROBLEM_DETAIL}{n}"
        defs[new_name] = defs.pop(_PROBLEM_DETAIL)
        _rewrite_ref(
            [defs, refs], _REF_PREFIX + _PROBLEM_DETAIL, _REF_PREFIX + new_name
        )

    schemas.update(defs)
    return refs


def _rewrite_ref(node: Any, old: str, new: str) -> None:
    if isinstance(node, dict):
        if node.get("$ref") == old:
            node["$ref"] = new
        for value in node.values():
            _rewrite_ref(value, old, new)
    elif isinstance(node, list):
        for value in node:
            _rewrite_ref(value, old, new)


def _get_response_schema(
    return_type: type[Any], model_refs: dict[type[BaseModel], dict[str, str]]
) -> dict[str, Any]:
    origin = get_origin(return_type)
    if origin is list:
        args = get_args(return_type)
        if args and _is_pydantic_model(args[0]):
            return {"type": "array", "items": dict(model_refs[args[0]])}
        if args:
            return {"type": "array", "items": _type_to_schema(args[0])}
        return {"type": "array"}

    if _is_pydantic_model(return_type):
        return dict(model_refs[return_type])

    return _type_to_schema(return_type)
