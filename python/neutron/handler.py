"""Handler signature extraction and parameter resolution."""

from __future__ import annotations

import inspect
import re
from types import UnionType
from typing import Annotated, Any, Callable, Union, get_args, get_origin, get_type_hints

from pydantic import BaseModel
from pydantic import ValidationError as PydanticValidationError
from starlette.requests import Request

from neutron.depends import _Depends
from neutron.error import ValidationErrorDetail, bad_request, validation_error

# Primitive types that can be extracted from query strings
_SCALAR_TYPES = (str, int, float, bool)


# --- Marker types for Query[T], Header[T], Form[T] ---


class _QueryMarker:
    pass


class _HeaderMarker:
    pass


class _FormMarker:
    pass


class Query:
    """Extract and validate query parameters into a Pydantic model.

    Usage::

        class Filters(BaseModel):
            page: int = 1
            per_page: int = 20

        @router.get("/users")
        async def list_users(query: Query[Filters]) -> list[User]: ...
    """

    def __class_getitem__(cls, model: type[Any]) -> type[Any]:
        return Annotated[model, _QueryMarker()]


class Header:
    """Extract and validate headers into a Pydantic model.

    Usage::

        class AuthHeaders(BaseModel):
            authorization: str

        @router.get("/me")
        async def get_me(headers: Header[AuthHeaders]) -> User: ...
    """

    def __class_getitem__(cls, model: type[Any]) -> type[Any]:
        return Annotated[model, _HeaderMarker()]


class Form:
    """Extract and validate form data into a Pydantic model.

    Usage::

        class LoginForm(BaseModel):
            username: str
            password: str

        @router.post("/login")
        async def login(form: Form[LoginForm]) -> Token: ...
    """

    def __class_getitem__(cls, model: type[Any]) -> type[Any]:
        return Annotated[model, _FormMarker()]


class UploadFile:
    """Marker for file upload parameters.

    Usage::

        @router.post("/upload")
        async def upload(file: UploadFile) -> dict: ...
    """

    def __init__(self) -> None:
        self.filename: str = ""
        self.content_type: str = ""
        self.file: Any = None


# --- Parameter classification ---


class ParamKind:
    PATH = "path"
    BODY = "body"
    QUERY = "query"
    QUERY_SCALAR = "query_scalar"
    HEADER = "header"
    FORM = "form"
    FILE = "file"
    REQUEST = "request"
    DEPENDS = "depends"


class HandlerParam:
    __slots__ = ("name", "kind", "annotation", "default")

    def __init__(
        self,
        name: str,
        kind: str,
        annotation: type[Any],
        default: Any = inspect.Parameter.empty,
    ) -> None:
        self.name = name
        self.kind = kind
        self.annotation = annotation
        self.default = default


def extract_path_params(path: str) -> set[str]:
    """Extract ``{param}`` names from a route path."""
    return set(re.findall(r"\{(\w+)\}", path))


def _is_pydantic_model(annotation: type[Any]) -> bool:
    try:
        return isinstance(annotation, type) and issubclass(annotation, BaseModel)
    except TypeError:
        return False


def _is_scalar_type(annotation: type[Any]) -> bool:
    """Check if annotation is a primitive scalar or Optional[scalar].

    Handles both ``Optional[int]`` (typing.Union) and ``int | None``
    (types.UnionType, Python 3.10+).
    """
    if annotation in _SCALAR_TYPES:
        return True
    origin = get_origin(annotation)
    # typing.Optional[X] → Union[X, None]
    if origin is Union:
        args = [a for a in get_args(annotation) if a is not type(None)]
        return len(args) == 1 and args[0] in _SCALAR_TYPES
    # Python 3.10+ X | None
    if isinstance(annotation, UnionType):
        args = [a for a in get_args(annotation) if a is not type(None)]
        return len(args) == 1 and args[0] in _SCALAR_TYPES
    return False


def _unwrap_scalar_type(annotation: type[Any]) -> type[Any]:
    """Return the concrete scalar type, unwrapping Optional if needed."""
    if annotation in _SCALAR_TYPES:
        return annotation
    origin = get_origin(annotation)
    if origin is Union:
        args = [a for a in get_args(annotation) if a is not type(None)]
        if args:
            return args[0]
    if isinstance(annotation, UnionType):
        args = [a for a in get_args(annotation) if a is not type(None)]
        if args:
            return args[0]
    return annotation


def extract_handler_params(
    fn: Callable,
    path_params: set[str],
) -> tuple[list[HandlerParam], type[Any] | None]:
    """Inspect a handler and classify each parameter.

    Returns ``(params, return_type)``.
    """
    sig = inspect.signature(fn)
    try:
        hints = get_type_hints(fn, include_extras=True)
    except Exception:
        hints = {}

    params: list[HandlerParam] = []
    return_type = hints.pop("return", None)

    for name, param in sig.parameters.items():
        annotation = hints.get(name, Any)
        default = param.default

        # Depends(...)
        if isinstance(default, _Depends):
            params.append(HandlerParam(name, ParamKind.DEPENDS, annotation, default))
            continue

        # Raw Request
        if annotation is Request:
            params.append(HandlerParam(name, ParamKind.REQUEST, annotation))
            continue

        # UploadFile annotation → FILE
        if annotation is UploadFile:
            params.append(HandlerParam(name, ParamKind.FILE, annotation, default))
            continue

        # Annotated[T, marker] — from Query[T], Header[T], Form[T]
        origin = get_origin(annotation)
        if origin is Annotated:
            args = get_args(annotation)
            inner_type = args[0]
            metadata = args[1:]

            found = False
            for m in metadata:
                if isinstance(m, _QueryMarker):
                    params.append(HandlerParam(name, ParamKind.QUERY, inner_type))
                    found = True
                    break
                if isinstance(m, _HeaderMarker):
                    params.append(HandlerParam(name, ParamKind.HEADER, inner_type))
                    found = True
                    break
                if isinstance(m, _FormMarker):
                    params.append(HandlerParam(name, ParamKind.FORM, inner_type))
                    found = True
                    break

            if not found:
                # Annotated but no known marker — classify normally
                if name in path_params:
                    params.append(HandlerParam(name, ParamKind.PATH, inner_type, default))
                elif _is_pydantic_model(inner_type):
                    params.append(HandlerParam(name, ParamKind.BODY, inner_type))
                elif _is_scalar_type(inner_type):
                    params.append(HandlerParam(name, ParamKind.QUERY_SCALAR, inner_type, default))
                else:
                    params.append(HandlerParam(name, ParamKind.PATH, inner_type, default))
            continue

        # Path parameter
        if name in path_params:
            params.append(HandlerParam(name, ParamKind.PATH, annotation, default))
            continue

        # Scalar types not in path → query parameters
        if _is_scalar_type(annotation):
            params.append(HandlerParam(name, ParamKind.QUERY_SCALAR, annotation, default))
            continue

        # Pydantic model → body
        if _is_pydantic_model(annotation):
            params.append(HandlerParam(name, ParamKind.BODY, annotation))
            continue

        # Fall through: treat as path parameter (for primitives)
        params.append(HandlerParam(name, ParamKind.PATH, annotation, default))

    return params, return_type


# --- Runtime resolution ---


async def resolve_handler_params(
    params: list[HandlerParam],
    request: Request,
    path_params: dict[str, str],
) -> dict[str, Any]:
    """Resolve all handler parameters from an incoming request."""
    resolved: dict[str, Any] = {}
    dep_cache: dict[int, Any] = {}  # keyed by id(dependency_fn)

    for param in params:
        if param.kind == ParamKind.REQUEST:
            resolved[param.name] = request

        elif param.kind == ParamKind.PATH:
            raw = path_params.get(param.name)
            if raw is None:
                if param.default is not inspect.Parameter.empty:
                    resolved[param.name] = param.default
                else:
                    raise bad_request(f"Missing path parameter: {param.name}")
            else:
                try:
                    resolved[param.name] = param.annotation(raw)
                except (ValueError, TypeError):
                    raise bad_request(
                        f"Invalid path parameter '{param.name}': {raw}"
                    )

        elif param.kind == ParamKind.BODY:
            try:
                body = await request.json()
            except Exception:
                raise bad_request("Invalid JSON body")
            try:
                resolved[param.name] = param.annotation.model_validate(body)
            except PydanticValidationError as e:
                errors = [
                    ValidationErrorDetail(
                        field=".".join(str(loc) for loc in err["loc"]),
                        message=err["msg"],
                        value=err.get("input"),
                    )
                    for err in e.errors()
                ]
                raise validation_error("Request body failed validation", errors)

        elif param.kind == ParamKind.QUERY:
            query_data = dict(request.query_params)
            try:
                resolved[param.name] = param.annotation.model_validate(query_data)
            except PydanticValidationError as e:
                errors = [
                    ValidationErrorDetail(
                        field=".".join(str(loc) for loc in err["loc"]),
                        message=err["msg"],
                    )
                    for err in e.errors()
                ]
                raise validation_error("Query parameter validation failed", errors)

        elif param.kind == ParamKind.HEADER:
            header_data = dict(request.headers)
            try:
                resolved[param.name] = param.annotation.model_validate(header_data)
            except PydanticValidationError as e:
                errors = [
                    ValidationErrorDetail(
                        field=".".join(str(loc) for loc in err["loc"]),
                        message=err["msg"],
                    )
                    for err in e.errors()
                ]
                raise validation_error("Header validation failed", errors)

        elif param.kind == ParamKind.QUERY_SCALAR:
            raw = request.query_params.get(param.name)
            if raw is None:
                if param.default is not inspect.Parameter.empty:
                    resolved[param.name] = param.default
                else:
                    raise bad_request(
                        f"Missing required query parameter: {param.name}"
                    )
            else:
                try:
                    inner = _unwrap_scalar_type(param.annotation)
                    resolved[param.name] = inner(raw)
                except (ValueError, TypeError):
                    raise bad_request(
                        f"Invalid query parameter '{param.name}': {raw}"
                    )

        elif param.kind == ParamKind.FORM:
            form_data = dict(await request.form())
            try:
                resolved[param.name] = param.annotation.model_validate(form_data)
            except PydanticValidationError as e:
                errors = [
                    ValidationErrorDetail(
                        field=".".join(str(loc) for loc in err["loc"]),
                        message=err["msg"],
                        value=err.get("input"),
                    )
                    for err in e.errors()
                ]
                raise validation_error("Form validation failed", errors)

        elif param.kind == ParamKind.FILE:
            form_data = await request.form()
            upload = form_data.get(param.name)
            if upload is None:
                if param.default is not inspect.Parameter.empty:
                    resolved[param.name] = param.default
                else:
                    raise bad_request(
                        f"Missing required file upload: {param.name}"
                    )
            else:
                uf = UploadFile()
                uf.filename = getattr(upload, "filename", "")
                uf.content_type = getattr(upload, "content_type", "")
                uf.file = upload.file if hasattr(upload, "file") else upload
                resolved[param.name] = uf

        elif param.kind == ParamKind.DEPENDS:
            dep_fn = param.default.dependency
            cache_key = id(dep_fn)
            if cache_key in dep_cache:
                resolved[param.name] = dep_cache[cache_key]
            else:
                # Recursively resolve the dependency's own params
                dep_path_params: set[str] = set()
                dep_params, _ = extract_handler_params(dep_fn, dep_path_params)
                dep_resolved = await resolve_handler_params(
                    dep_params, request, path_params
                )
                if inspect.iscoroutinefunction(dep_fn):
                    result = await dep_fn(**dep_resolved)
                else:
                    result = dep_fn(**dep_resolved)
                dep_cache[cache_key] = result
                resolved[param.name] = result

    return resolved
