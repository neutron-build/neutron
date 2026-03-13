"""Response serialization helpers."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel
from starlette.responses import JSONResponse as _StarletteJSON
from starlette.responses import Response


def serialize_response(
    result: Any,
    *,
    status_code: int = 200,
    response_model: type[Any] | None = None,
) -> Response:
    """Convert a handler return value to a Starlette Response.

    Parameters
    ----------
    result:
        The raw return value from the handler.
    status_code:
        HTTP status code for the response. Defaults to 200.
    response_model:
        Optional Pydantic model to filter/validate the result through.
    """
    # If the handler already returned a full Response, respect it as-is.
    if isinstance(result, Response):
        return result

    # Apply response_model filtering if provided
    if response_model is not None and _is_pydantic(response_model):
        if isinstance(result, list):
            result = [
                response_model.model_validate(
                    item.model_dump(mode="json") if isinstance(item, BaseModel) else item,
                    from_attributes=True,
                )
                for item in result
            ]
        elif result is not None:
            data = result.model_dump(mode="json") if isinstance(result, BaseModel) else result
            result = response_model.model_validate(data, from_attributes=True)

    if isinstance(result, BaseModel):
        return JSONResponse(
            content=result.model_dump(mode="json"), status_code=status_code
        )

    if isinstance(result, list):
        content = [
            item.model_dump(mode="json") if isinstance(item, BaseModel) else item
            for item in result
        ]
        return JSONResponse(content=content, status_code=status_code)

    if result is None:
        # For None results, use the provided status_code (which may already
        # be 204 from the DELETE default) or fall back to 204.
        return Response(status_code=status_code if status_code != 200 else 204)

    if isinstance(result, (dict, int, float, str, bool)):
        return JSONResponse(content=result, status_code=status_code)

    return JSONResponse(content=result, status_code=status_code)


def _is_pydantic(cls: type[Any]) -> bool:
    """Check if cls is a Pydantic BaseModel subclass."""
    try:
        return isinstance(cls, type) and issubclass(cls, BaseModel)
    except TypeError:
        return False


class JSONResponse(_StarletteJSON):
    """JSON response with sensible defaults."""

    pass
